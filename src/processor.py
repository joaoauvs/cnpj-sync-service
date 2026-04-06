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
from pathlib import Path
from typing import Optional

import pandas as pd

from src.config import (
    CSV_CHUNK_ROWS,
    CSV_ENCODING,
    CSV_SEPARATOR,
    DATE_COLUMNS,
    DECIMAL_COLUMNS,
    PROCESSED_DIR,
    SCHEMAS,
)
from src.logger import logger
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
# Public API
# ---------------------------------------------------------------------------

def process_csv(
    extraction_result: ExtractionResult,
    group: str,
    output_dir: Path = PROCESSED_DIR,
    chunk_size: int = CSV_CHUNK_ROWS,
    write_rejects: bool = True,
) -> ProcessingResult:
    """
    Read a raw RF CNPJ CSV and write a normalised output CSV.

    Parameters
    ----------
    extraction_result: Result from the extractor module.
    group:             Logical group name (key into ``SCHEMAS``).
    output_dir:        Directory for the output CSV.
    chunk_size:        Rows per pandas chunk.
    write_rejects:     Write rows that fail validation to a reject file.

    Returns
    -------
    ProcessingResult
    """
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
    output_path = output_dir / f"{zip_stem}.csv"
    reject_path = output_dir / f"{zip_stem}_rejects.csv"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Remove stale output from a previous incomplete run
    if output_path.exists():
        output_path.unlink()
    if reject_path.exists() and write_rejects:
        reject_path.unlink()

    rows_written = 0
    rows_invalid = 0
    reject_first = True
    t0 = time.perf_counter()

    try:
        with get_writer("csv", output_path, group) as writer:
            for csv_path in extraction_result.extracted_paths:
                logger.info(
                    "Processing {} → {}",
                    csv_path.name, output_path.name,
                )

                reader = pd.read_csv(
                    csv_path,
                    sep=CSV_SEPARATOR,
                    header=None,
                    names=schema,
                    encoding=CSV_ENCODING,
                    dtype=str,
                    na_values=[""],
                    keep_default_na=False,
                    chunksize=chunk_size,
                    on_bad_lines="warn",
                    engine="python",
                )

                for chunk in reader:
                    if len(chunk.columns) != len(schema):
                        logger.warning(
                            "Column count mismatch in chunk: expected {}, got {}",
                            len(schema), len(chunk.columns),
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
        logger.success(
            "Processed {}: {:,} rows, {:,} invalid in {:.1f}s",
            zip_stem, rows_written, rows_invalid, elapsed,
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
    extraction_results: list[ExtractionResult],
    output_dir: Path = PROCESSED_DIR,
) -> list[ProcessingResult]:
    """
    Process all extraction results.

    Group is derived from the original RemoteFile metadata so callers
    don't need to re-specify it.
    """
    results = []
    for er in extraction_results:
        group = er.download_result.remote_file.group
        results.append(process_csv(er, group, output_dir))
    return results
