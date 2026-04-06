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

from src.config import EXTRACTED_DIR
from src.logger import logger
from src.models import DownloadResult, ExtractionResult, FileStatus


def extract_zip(
    download_result: DownloadResult,
    dest_dir: Path = EXTRACTED_DIR,
    overwrite: bool = False,
) -> ExtractionResult:
    """
    Extract all members of a downloaded ZIP file.

    Parameters
    ----------
    download_result: Result from the downloader module.
    dest_dir:        Directory where extracted files will be placed.
    overwrite:       Re-extract even if the output file already exists.

    Returns
    -------
    ExtractionResult
    """
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

    # Validate ZIP integrity before extraction
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

    dest_dir.mkdir(parents=True, exist_ok=True)
    extracted_paths: list[Path] = []
    t0 = time.perf_counter()

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in members:
                out_path = dest_dir / member
                if out_path.exists() and not overwrite:
                    logger.debug("SKIP extract {} (already exists)", member)
                    extracted_paths.append(out_path)
                    continue

                logger.debug("Extracting {} → {}", member, out_path)
                zf.extract(member, dest_dir)
                extracted_paths.append(out_path)

        elapsed = time.perf_counter() - t0
        logger.info(
            "Extracted {} ({} files) in {:.1f}s",
            zip_path.name,
            len(extracted_paths),
            elapsed,
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
    download_results: list[DownloadResult],
    dest_dir: Path = EXTRACTED_DIR,
    overwrite: bool = False,
) -> list[ExtractionResult]:
    """Extract all downloaded ZIPs sequentially (I/O-bound, not CPU-bound)."""
    results = []
    for dr in download_results:
        results.append(extract_zip(dr, dest_dir, overwrite))
    return results
