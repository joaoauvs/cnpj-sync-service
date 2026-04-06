"""
Pipeline: orchestrates the full end-to-end workflow.

Execution modes
---------------
full        — crawl → download all → extract → process
download    — crawl + download only (no extraction/processing)
extract     — extract already-downloaded files (no network)
process     — process already-extracted files (no network/disk I/O)
groups      — run only specified file groups (e.g. Empresas, Socios)

The pipeline is intentionally linear per file (download → extract →
process) rather than waiting for all downloads to finish first, so
processing can overlap with ongoing downloads on large datasets.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

from src.config import (
    DOWNLOAD_WORKERS,
    DOWNLOADS_DIR,
    EXTRACTED_DIR,
    HEADERS,
    PROCESS_WORKERS,
    PROCESSED_DIR,
    REFERENCE_FILES,
)
from src.crawler import discover_latest_snapshot_with_fallback
from src.downloader import download_file
from src.extractor import extract_zip
from src.logger import logger
from src.models import DownloadResult, ExtractionResult, FileStatus, PipelineRun, ProcessingResult, RemoteFile
from src.processor import process_csv

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _file_pipeline(
    remote_file: RemoteFile,
    session: requests.Session,
    snapshot_date: str,
    force_download: bool = False,
    force_extract: bool = False,
) -> ProcessingResult:
    """Full pipeline for a single file: download → extract → process."""
    dl = download_file(remote_file, session, DOWNLOADS_DIR, force=force_download)
    ex = extract_zip(dl, EXTRACTED_DIR, overwrite=force_extract)
    pr = process_csv(ex, remote_file.group, PROCESSED_DIR)
    return pr


# ---------------------------------------------------------------------------
# Public pipeline entry points
# ---------------------------------------------------------------------------

def run_pipeline(
    groups: Optional[list[str]] = None,
    snapshot_date: Optional[str] = None,
    force_download: bool = False,
    force_extract: bool = False,
    download_workers: int = DOWNLOAD_WORKERS,
    process_workers: int = PROCESS_WORKERS,
    reference_only: bool = False,
) -> PipelineRun:
    """
    Execute the full scraping pipeline.

    Parameters
    ----------
    groups:
        Restrict processing to these file groups (e.g. ['Empresas', 'Socios']).
        None means all groups.
    snapshot_date:
        Target a specific snapshot (YYYY-MM-DD).  None = use the latest.
    force_download:
        Re-download files even if they appear complete locally.
    force_extract:
        Re-extract even if the target CSV already exists.
    download_workers:
        Parallel download threads.
    process_workers:
        Parallel processing threads (extraction + CSV writing).
    reference_only:
        Only download and process the small reference/lookup tables.

    Returns
    -------
    PipelineRun
        Summary object with per-file results and aggregate statistics.
    """
    run = PipelineRun(
        snapshot_date=snapshot_date or "latest",
        started_at=datetime.utcnow(),
    )

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # ------------------------------------------------------------------
        # 1. Discover snapshot
        # ------------------------------------------------------------------
        logger.info("=== PIPELINE START ===")
        snapshot = discover_latest_snapshot_with_fallback(session=session)

        if snapshot_date and snapshot.date != snapshot_date:
            logger.warning(
                "Requested snapshot {} but latest is {}",
                snapshot_date,
                snapshot.date,
            )

        run.snapshot_date = snapshot.date

        # ------------------------------------------------------------------
        # 2. Filter files
        # ------------------------------------------------------------------
        files = snapshot.files

        if reference_only:
            files = [f for f in files if f.group in REFERENCE_FILES]
            logger.info("reference_only=True: {} files selected", len(files))
        elif groups:
            groups_set = {g.lower() for g in groups}
            files = [f for f in files if f.group.lower() in groups_set]
            logger.info(
                "Group filter {}: {} files selected", groups, len(files)
            )

        if not files:
            logger.warning("No files match the current filter — nothing to do")
            run.finished_at = datetime.utcnow()
            return run

        logger.info(
            "Processing {} files from snapshot {}",
            len(files),
            snapshot.date,
        )

        # ------------------------------------------------------------------
        # 3. Reference tables first (tiny, fast, sequential)
        # ------------------------------------------------------------------
        ref_files = [f for f in files if f.group in REFERENCE_FILES]
        main_files = [f for f in files if f.group not in REFERENCE_FILES]

        if ref_files:
            logger.info("--- Reference tables ({}) ---", len(ref_files))
            for rf in ref_files:
                pr = _file_pipeline(
                    rf, session, snapshot.date, force_download, force_extract,
                )
                run.results.append(pr)

        # ------------------------------------------------------------------
        # 4. Main data files — pipelined per-file with a thread pool
        # ------------------------------------------------------------------
        if main_files:
            logger.info(
                "--- Main data files ({}) with {} workers ---",
                len(main_files),
                process_workers,
            )
            with ThreadPoolExecutor(
                max_workers=process_workers,
                thread_name_prefix="pipe",
            ) as pool:
                future_to_file = {
                    pool.submit(
                        _file_pipeline,
                        f,
                        session,
                        snapshot.date,
                        force_download,
                        force_extract,
                    ): f
                    for f in main_files
                }
                for future in as_completed(future_to_file):
                    rf = future_to_file[future]
                    try:
                        pr = future.result()
                    except Exception as exc:
                        logger.error(
                            "Unhandled error for {}: {}", rf.name, exc
                        )
                        # Build a failed result so the run summary is complete
                        dummy_dl = DownloadResult(
                            remote_file=rf,
                            local_path=DOWNLOADS_DIR / rf.name,
                            status=FileStatus.FAILED,
                            error=str(exc),
                        )
                        dummy_ex = ExtractionResult(
                            download_result=dummy_dl,
                            status=FileStatus.FAILED,
                            error=str(exc),
                        )
                        pr = ProcessingResult(
                            extraction_result=dummy_ex,
                            status=FileStatus.FAILED,
                            error=str(exc),
                        )
                    run.results.append(pr)

    except Exception as exc:
        logger.critical("Pipeline aborted: {}", exc)
        raise

    finally:
        session.close()
        run.finished_at = datetime.utcnow()
        _save_run_report(run)
        _log_summary(run)
        logger.info("=== PIPELINE END ===")

    return run


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _save_run_report(run: PipelineRun) -> None:
    """Persist a JSON summary of the run to the logs directory."""
    from src.config import LOGS_DIR

    ts = run.started_at.strftime("%Y%m%d_%H%M%S")
    report_path = LOGS_DIR / f"run_{run.snapshot_date}_{ts}.json"
    try:
        report_path.write_text(
            json.dumps(run.summary(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Run report saved: {}", report_path)
    except Exception as exc:
        logger.warning("Could not save run report: {}", exc)


def _log_summary(run: PipelineRun) -> None:
    summary = run.summary()
    logger.info(
        "SUMMARY | snapshot={} | files={} | ok={} | failed={} | rows={:,}",
        summary["snapshot_date"],
        summary["total_files"],
        summary["successful"],
        summary["failed"],
        summary["total_rows_written"],
    )
    if summary["failed_files"]:
        logger.warning("Failed files: {}", summary["failed_files"])
