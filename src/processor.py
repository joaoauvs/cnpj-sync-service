"""
Processor: reads raw CSV files and writes validated, normalised output.

Design choices
--------------
* Chunked reading via ``pandas.read_csv`` — never loads a full multi-GB
  file into memory.
* Schema is applied from ``config.SCHEMAS`` (no headers in source files).
* Date columns are normalised from 'YYYYMMDD' strings → 'YYYY-MM-DD'.
* Brazilian decimal columns (comma separator) are converted to float.
* Invalid rows are counted and optionally written to a separate reject file.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd

from src.config import (
    CSV_CHUNK_ROWS,
    CSV_ENCODING,
    CSV_SEPARATOR,
    DATE_COLUMNS,
    DECIMAL_COLUMNS,
    PROCESS_WORKERS,
    PROCESSED_DIR,
    SCHEMAS,
    STORAGE_BACKEND,
)
from src.logger_enhanced import logger
from src.models import ExtractionResult, FileStatus, ProcessingResult
from src.storage import get_writer

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise_date_column(series: pd.Series) -> pd.Series:
    """
    Convert 'YYYYMMDD' strings to 'YYYY-MM-DD'.

    Non-parseable values become NaT / empty string.
    """
    def _convert(val: str) -> str:
        if pd.isna(val) or not str(val).strip() or str(val).strip() == "0":
            return ""
        s = str(val).strip().zfill(8)
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return ""

    return series.astype(str).map(_convert)


def _normalise_decimal_column(series: pd.Series) -> pd.Series:
    """Replace Brazilian comma-decimal with dot, then cast to float."""
    return (
        series.astype(str)
        .str.replace(",", ".", regex=False)
        .str.strip()
        .replace("", pd.NA)
        .pipe(pd.to_numeric, errors="coerce")
    )


def _detect_group(csv_path: Path) -> Optional[str]:
    """
    Infer the data group (e.g. 'Empresas') from the CSV file name.

    RF CNPJ files are typically named like:
      K3241.K03200Y0.D20316EMPRECSV  → Empresas
      K3241.K03200Y0.D20316ESTABELCSV → Estabelecimentos
      ...
    We also try the parent ZIP stem as fallback.
    """
    name_upper = csv_path.name.upper()
    group_hints = {
        "EMPRECSV": "Empresas",
        "EMPRE": "Empresas",
        "ESTABEL": "Estabelecimentos",
        "SOCIOCSV": "Socios",
        "SOCIO": "Socios",
        "SIMPLESCSV": "Simples",
        "SIMPLES": "Simples",
        "CNAECSV": "Cnaes",
        "CNAE": "Cnaes",
        "MOTIVCSV": "Motivos",
        "MOTIV": "Motivos",
        "MUNICCSV": "Municipios",
        "MUNIC": "Municipios",
        "NATJUCSV": "Naturezas",
        "NATJU": "Naturezas",
        "PAISCSV": "Paises",
        "PAIS": "Paises",
        "QUALSCSV": "Qualificacoes",
        "QUALS": "Qualificacoes",
    }
    for hint, group in group_hints.items():
        if hint in name_upper:
            return group

    # Fallback: check parent directory or ZIP stem passed by pipeline
    return None


# ---------------------------------------------------------------------------
# Serviço OO + compatibilidade pública
# ---------------------------------------------------------------------------

class CSVProcessor:
    """Serviço orientado a objeto para normalização e persistência dos CSVs."""

    def __init__(
        self,
        output_dir: Path = PROCESSED_DIR,
        chunk_size: int = CSV_CHUNK_ROWS,
        workers: int = PROCESS_WORKERS,
        storage_backend: str = STORAGE_BACKEND,
    ) -> None:
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.workers = workers
        self.storage_backend = storage_backend

    def process_csv(
        self,
        extraction_result: ExtractionResult,
        group: str,
        write_rejects: bool = True,
    ) -> ProcessingResult:
        if extraction_result.status == FileStatus.FAILED:
            return ProcessingResult(
                extraction_result=extraction_result,
                status=FileStatus.SKIPPED,
                error="Skipped because extraction failed",
            )

        if not extraction_result.extracted_paths:
            return ProcessingResult(
                extraction_result=extraction_result,
                status=FileStatus.FAILED,
                error="No extracted CSV paths found",
            )

        schema = SCHEMAS.get(group)
        if schema is None:
            return ProcessingResult(
                extraction_result=extraction_result,
                status=FileStatus.FAILED,
                error=f"Unknown group '{group}' — no schema defined",
            )

        date_cols = DATE_COLUMNS.get(group, [])
        decimal_cols = DECIMAL_COLUMNS.get(group, [])

        zip_stem = extraction_result.download_result.remote_file.stem
        from src.storage import output_extension

        ext = output_extension(self.storage_backend)
        output_path = self.output_dir / f"{zip_stem}{ext}"
        reject_path = self.output_dir / f"{zip_stem}_rejects.csv"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()
        if reject_path.exists() and write_rejects:
            reject_path.unlink()

        rows_written = 0
        rows_invalid = 0
        reject_first = True
        t0 = time.perf_counter()

        try:
            with get_writer(self.storage_backend, output_path, group) as writer:
                for csv_path in extraction_result.extracted_paths:
                    logger.debug("Processing {} → {}", csv_path.name, output_path.name)

                    reader = pd.read_csv(
                        csv_path,
                        sep=CSV_SEPARATOR,
                        header=None,
                        names=schema,
                        encoding=CSV_ENCODING,
                        dtype=str,
                        na_values=[""],
                        keep_default_na=False,
                        chunksize=self.chunk_size,
                        on_bad_lines="warn",
                        engine="python",
                    )

                    for chunk in reader:
                        if len(chunk.columns) != len(schema):
                            logger.warning(
                                "Column count mismatch in chunk: expected {}, got {}",
                                len(schema),
                                len(chunk.columns),
                            )

                        for col in date_cols:
                            if col in chunk.columns:
                                chunk[col] = _normalise_date_column(chunk[col])

                        for col in decimal_cols:
                            if col in chunk.columns:
                                chunk[col] = _normalise_decimal_column(chunk[col])

                        chunk = chunk.apply(
                            lambda s: s.str.strip() if s.dtype == object else s
                        )

                        if "cnpj_basico" in chunk.columns:
                            valid_mask = chunk["cnpj_basico"].notna() & (
                                chunk["cnpj_basico"].str.strip() != ""
                            )
                            invalid = chunk[~valid_mask]
                            chunk = chunk[valid_mask]

                            if not invalid.empty and write_rejects:
                                invalid.to_csv(
                                    reject_path,
                                    mode="a",
                                    index=False,
                                    header=reject_first,
                                    encoding="utf-8",
                                )
                                reject_first = False
                            rows_invalid += len(invalid)

                        writer.write(chunk)

            rows_written = writer.rows_written
            elapsed = time.perf_counter() - t0
            logger.debug(
                "Processed {}: {:,} rows, {:,} invalid in {:.1f}s",
                zip_stem,
                rows_written,
                rows_invalid,
                elapsed,
                operation="process_complete",
                file_name=zip_stem,
                rows_written=rows_written,
                rows_invalid=rows_invalid,
                duration_seconds=elapsed,
                rows_per_second=rows_written / elapsed if elapsed > 0 else 0,
            )
            return ProcessingResult(
                extraction_result=extraction_result,
                output_path=output_path,
                rows_written=rows_written,
                rows_invalid=rows_invalid,
                status=FileStatus.DONE,
                duration_seconds=elapsed,
            )

        except Exception as exc:
            elapsed = time.perf_counter() - t0
            logger.error("Processing failed for {}: {}", zip_stem, exc)
            return ProcessingResult(
                extraction_result=extraction_result,
                output_path=output_path if output_path.exists() else None,
                rows_written=rows_written,
                rows_invalid=rows_invalid,
                status=FileStatus.FAILED,
                error=str(exc),
                duration_seconds=elapsed,
            )

    def process_all(
        self,
        extraction_results: list[ExtractionResult],
        workers: Optional[int] = None,
    ) -> list[ProcessingResult]:
        if not extraction_results:
            return []

        worker_count = workers or self.workers

        def _task(er: ExtractionResult) -> ProcessingResult:
            return self.process_csv(er, er.download_result.remote_file.group)

        results: dict[int, ProcessingResult] = {}
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="pr") as pool:
            future_to_idx = {
                pool.submit(_task, er): i
                for i, er in enumerate(extraction_results)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:
                    results[idx] = ProcessingResult(
                        extraction_result=extraction_results[idx],
                        status=FileStatus.FAILED,
                        error=str(exc),
                    )

        ordered = [results[i] for i in range(len(extraction_results))]
        n_ok = sum(1 for r in ordered if r.status == FileStatus.DONE)
        n_skip = sum(1 for r in ordered if r.status == FileStatus.SKIPPED)
        n_fail = sum(1 for r in ordered if r.status == FileStatus.FAILED)
        parts = []
        if n_ok:
            parts.append(f"{n_ok} processados")
        if n_skip:
            parts.append(f"{n_skip} ignorados")
        if n_fail:
            parts.append(f"{n_fail} falhas")
        logger.info("Processamento concluído: {}", ", ".join(parts))
        return ordered


def process_csv(
    extraction_result: ExtractionResult,
    group: str,
    output_dir: Path = PROCESSED_DIR,
    chunk_size: int = CSV_CHUNK_ROWS,
    write_rejects: bool = True,
) -> ProcessingResult:
    """Compatibilidade pública com a API funcional anterior."""
    return CSVProcessor(output_dir=output_dir, chunk_size=chunk_size).process_csv(
        extraction_result,
        group,
        write_rejects=write_rejects,
    )


def process_all(
    extraction_results: list[ExtractionResult],
    output_dir: Path = PROCESSED_DIR,
    workers: int = PROCESS_WORKERS,
) -> list[ProcessingResult]:
    """Compatibilidade pública com a API funcional anterior."""
    return CSVProcessor(output_dir=output_dir, workers=workers).process_all(
        extraction_results,
        workers=workers,
    )
