"""
Extractor: unzips downloaded archives.

The RF CNPJ ZIPs each contain exactly one CSV file (no header row).
Extraction is done in-process using the stdlib ``zipfile`` module,
streaming the content so we never need to hold the full file in memory.
"""

from __future__ import annotations

import time
import zipfile
from pathlib import Path
from typing import Optional

from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config import EXTRACT_WORKERS, EXTRACTED_DIR
from src.logger_enhanced import logger
from src.models import DownloadResult, ExtractionResult, FileStatus


class ZipExtractor:
    """Serviço OO para extração paralela de ZIPs."""

    def __init__(self, dest_dir: Path = EXTRACTED_DIR, workers: int = EXTRACT_WORKERS) -> None:
        self.dest_dir = dest_dir
        self.workers = workers

    def extract_zip(
        self,
        download_result: DownloadResult,
        overwrite: bool = False,
    ) -> ExtractionResult:
        if download_result.status == FileStatus.FAILED:
            return ExtractionResult(
                download_result=download_result,
                status=FileStatus.SKIPPED,
                error="Skipped because download failed",
            )

        zip_path = download_result.local_path
        if not zip_path.exists():
            return ExtractionResult(
                download_result=download_result,
                status=FileStatus.FAILED,
                error=f"ZIP not found: {zip_path}",
            )

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                bad = zf.testzip()
                if bad:
                    return ExtractionResult(
                        download_result=download_result,
                        status=FileStatus.FAILED,
                        error=f"Corrupt entry in ZIP: {bad}",
                    )
                members = zf.namelist()
        except zipfile.BadZipFile as exc:
            return ExtractionResult(
                download_result=download_result,
                status=FileStatus.FAILED,
                error=f"BadZipFile: {exc}",
            )

        self.dest_dir.mkdir(parents=True, exist_ok=True)
        extracted_paths: list[Path] = []
        t0 = time.perf_counter()

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for member in members:
                    out_path = self.dest_dir / member
                    if out_path.exists() and not overwrite:
                        logger.debug("SKIP extract {} (already exists)", member)
                        extracted_paths.append(out_path)
                        continue

                    logger.debug("Extracting {} → {}", member, out_path)
                    zf.extract(member, self.dest_dir)
                    extracted_paths.append(out_path)

            elapsed = time.perf_counter() - t0
            logger.debug(
                "Extracted {} ({} files) in {:.1f}s",
                zip_path.name,
                len(extracted_paths),
                elapsed,
                operation="extract",
                file_name=zip_path.name,
                extracted_count=len(extracted_paths),
                duration_seconds=elapsed,
            )
            return ExtractionResult(
                download_result=download_result,
                extracted_paths=extracted_paths,
                status=FileStatus.EXTRACTED,
                duration_seconds=elapsed,
            )

        except Exception as exc:
            elapsed = time.perf_counter() - t0
            logger.error("Extraction failed for {}: {}", zip_path.name, exc)
            return ExtractionResult(
                download_result=download_result,
                extracted_paths=extracted_paths,
                status=FileStatus.FAILED,
                error=str(exc),
                duration_seconds=elapsed,
            )

    def extract_all(
        self,
        download_results: list[DownloadResult],
        overwrite: bool = False,
        workers: Optional[int] = None,
    ) -> list[ExtractionResult]:
        if not download_results:
            return []

        worker_count = workers or self.workers
        results: dict[int, ExtractionResult] = {}

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="ex") as pool:
            future_to_idx = {
                pool.submit(self.extract_zip, dr, overwrite): i
                for i, dr in enumerate(download_results)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:
                    results[idx] = ExtractionResult(
                        download_result=download_results[idx],
                        status=FileStatus.FAILED,
                        error=str(exc),
                    )

        ordered = [results[i] for i in range(len(download_results))]
        n_ok = sum(1 for r in ordered if r.status == FileStatus.EXTRACTED)
        n_skip = sum(1 for r in ordered if r.status == FileStatus.SKIPPED)
        n_fail = sum(1 for r in ordered if r.status == FileStatus.FAILED)
        parts = []
        if n_ok:
            parts.append(f"{n_ok} extraídos")
        if n_skip:
            parts.append(f"{n_skip} ignorados")
        if n_fail:
            parts.append(f"{n_fail} falhas")
        logger.info("Extração concluída: {}", ", ".join(parts))
        return ordered


def extract_zip(
    download_result: DownloadResult,
    dest_dir: Path = EXTRACTED_DIR,
    overwrite: bool = False,
) -> ExtractionResult:
    """Compatibilidade pública com a API funcional anterior."""
    return ZipExtractor(dest_dir=dest_dir).extract_zip(download_result, overwrite=overwrite)


def extract_all(
    download_results: list[DownloadResult],
    dest_dir: Path = EXTRACTED_DIR,
    overwrite: bool = False,
    workers: int = EXTRACT_WORKERS,
) -> list[ExtractionResult]:
    """Compatibilidade pública com a API funcional anterior."""
    return ZipExtractor(dest_dir=dest_dir, workers=workers).extract_all(
        download_results,
        overwrite=overwrite,
        workers=workers,
    )
