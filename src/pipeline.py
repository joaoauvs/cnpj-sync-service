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

import pyarrow.parquet as pq
import requests

from src.config import (
    DATABASE_WORKERS,
    DOWNLOAD_WORKERS,
    DOWNLOADS_DIR,
    EXTRACTED_DIR,
    EXTRACT_WORKERS,
    HEADERS,
    PROCESS_WORKERS,
    PROCESSED_DIR,
    REFERENCE_FILES,
    RF_AUTH,
    STORAGE_BACKEND,
    TOTAL_DOWNLOAD_WORKERS,
    TOTAL_PROCESS_WORKERS,
)
from src.crawler import SnapshotCrawler
from src.downloader import FileDownloader, download_file, download_all
from src.extractor import ZipExtractor, extract_zip, extract_all
from src.logger_enhanced import logger, structured_logger
from src.models import DownloadResult, ExtractionResult, FileStatus, PipelineRun, ProcessingResult, RemoteFile, Snapshot
from src.processor import CSVProcessor, process_csv, process_all
from src.storage import output_extension

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _processed_output_path(remote_file: RemoteFile, output_dir: Path = PROCESSED_DIR) -> Path:
    return output_dir / f"{remote_file.stem}{output_extension(STORAGE_BACKEND)}"


def _snapshot_stage_dir(base_dir: Path, snapshot_label: str) -> Path:
    return base_dir / snapshot_label


def _count_processed_rows(path: Path) -> int:
    if not path.exists():
        return 0
    if path.suffix.lower() == ".parquet":
        return pq.ParquetFile(path).metadata.num_rows
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            return max(sum(1 for _ in fh) - 1, 0)
    return 0


def _reused_processed_result(
    remote_file: RemoteFile,
    output_dir: Path = PROCESSED_DIR,
    download_dir: Path = DOWNLOADS_DIR,
) -> ProcessingResult | None:
    output_path = _processed_output_path(remote_file, output_dir)
    if not output_path.exists():
        return None

    local_zip = download_dir / remote_file.name
    download_result = DownloadResult(
        remote_file=remote_file,
        local_path=local_zip,
        status=FileStatus.SKIPPED,
        bytes_downloaded=local_zip.stat().st_size if local_zip.exists() else 0,
    )
    extraction_result = ExtractionResult(
        download_result=download_result,
        status=FileStatus.SKIPPED,
    )
    rows_written = _count_processed_rows(output_path)
    return ProcessingResult(
        extraction_result=extraction_result,
        output_path=output_path,
        rows_written=rows_written,
        status=FileStatus.DONE,
    )


class CNPJPipeline:
    """Orquestrador OO do fluxo download -> extract -> process."""

    def __init__(
        self,
        crawler: Optional[SnapshotCrawler] = None,
        downloader: Optional[FileDownloader] = None,
        extractor: Optional[ZipExtractor] = None,
        processor: Optional[CSVProcessor] = None,
    ) -> None:
        self.crawler = crawler or SnapshotCrawler()
        self.downloader = downloader or FileDownloader(dest_dir=DOWNLOADS_DIR, workers=DOWNLOAD_WORKERS)
        self.extractor = extractor or ZipExtractor(dest_dir=EXTRACTED_DIR, workers=EXTRACT_WORKERS)
        self.processor = processor or CSVProcessor(output_dir=PROCESSED_DIR, workers=PROCESS_WORKERS)

    def _download_stage(
        self,
        files: list[RemoteFile],
        force_download: bool = False,
    ) -> list[DownloadResult]:
        return self.downloader.download_all(files, force=force_download, workers=self.downloader.workers)

    def _extract_stage(
        self,
        download_results: list[DownloadResult],
        force_extract: bool = False,
    ) -> list[ExtractionResult]:
        return self.extractor.extract_all(download_results, overwrite=force_extract, workers=self.extractor.workers)

    def _process_stage(
        self,
        extraction_results: list[ExtractionResult],
    ) -> list[ProcessingResult]:
        return self.processor.process_all(extraction_results, workers=self.processor.workers)

    def run(
        self,
        groups: Optional[list[str]] = None,
        snapshot_date: Optional[str] = None,
        force_download: bool = False,
        force_extract: bool = False,
        download_workers: int = TOTAL_DOWNLOAD_WORKERS,
        process_workers: int = TOTAL_PROCESS_WORKERS,
        reference_only: bool = False,
        snapshot: Optional["Snapshot"] = None,
        reuse_processed: bool = False,
    ) -> PipelineRun:
        run = PipelineRun(
            snapshot_date=snapshot_date or "latest",
            started_at=datetime.utcnow(),
        )

        self.downloader.workers = download_workers
        self.processor.workers = process_workers

        session = self.crawler.create_session()
        session.auth = RF_AUTH

        try:
            if snapshot is None:
                if snapshot_date:
                    snapshot = self.crawler.discover_snapshot_with_fallback(snapshot_date, session=session)
                else:
                    snapshot = self.crawler.discover_latest_snapshot_with_fallback(session=session)
            run.snapshot_date = snapshot.date
            files = snapshot.files

            download_dir = _snapshot_stage_dir(DOWNLOADS_DIR, snapshot.date)
            extracted_dir = _snapshot_stage_dir(EXTRACTED_DIR, snapshot.date)
            processed_dir = _snapshot_stage_dir(PROCESSED_DIR, snapshot.date)
            for path in (download_dir, extracted_dir, processed_dir):
                path.mkdir(parents=True, exist_ok=True)

            self.downloader.dest_dir = download_dir
            self.extractor.dest_dir = extracted_dir
            self.processor.output_dir = processed_dir

            if reference_only:
                files = [f for f in files if f.group in REFERENCE_FILES]
                logger.info("reference_only=True: {} files selected", len(files))
            elif groups:
                groups_set = {g.lower() for g in groups}
                files = [f for f in files if f.group.lower() in groups_set]
                logger.info("Group filter {}: {} files selected", groups, len(files))

            if not files:
                logger.warning("No files match the current filter — nothing to do")
                run.finished_at = datetime.utcnow()
                return run

            if reuse_processed and not (force_download or force_extract):
                reused_count = 0
                pending_files: list[RemoteFile] = []
                for remote_file in files:
                    reused = _reused_processed_result(
                        remote_file,
                        output_dir=processed_dir,
                        download_dir=download_dir,
                    )
                    if reused is None:
                        pending_files.append(remote_file)
                        continue
                    run.results.append(reused)
                    reused_count += 1
                    logger.debug(
                        "Reusing processed artifact for {} -> {}",
                        remote_file.name,
                        reused.output_path.name if reused.output_path else "",
                    )
                files = pending_files
                if reused_count:
                    logger.info(
                        "Processed reuse: {} reutilizados, {} restantes",
                        reused_count,
                        len(files),
                    )

            if not files:
                logger.info("All selected files already have processed artifacts — skipping pipeline stages")
                run.finished_at = datetime.utcnow()
                return run

            logger.info("Processing {} files from snapshot {}", len(files), snapshot.date)

            ref_files = [f for f in files if f.group in REFERENCE_FILES]
            main_files = [f for f in files if f.group not in REFERENCE_FILES]

            if ref_files:
                ref_ok = ref_fail = 0
                for rf in ref_files:
                    dl = self.downloader.download_file(rf, session=session, force=force_download)
                    if dl.status == FileStatus.FAILED:
                        logger.error("Falha ao baixar referência {}", rf.name)
                        ref_fail += 1
                        continue
                    ex = self.extractor.extract_zip(dl, overwrite=force_extract)
                    if ex.status == FileStatus.FAILED:
                        logger.error("Falha ao extrair referência {}", rf.name)
                        ref_fail += 1
                        continue
                    pr = self.processor.process_csv(ex, rf.group)
                    run.results.append(pr)
                    ref_ok += 1
                logger.info("Tabelas de referência: {} ok{}", ref_ok, f", {ref_fail} falhas" if ref_fail else "")

            if main_files:
                logger.info("Arquivos principais: {} arquivos", len(main_files))
                download_results = self._download_stage(main_files, force_download)

                successful_downloads = [
                    dr for dr in download_results
                    if dr.status in (FileStatus.DOWNLOADED, FileStatus.SKIPPED)
                ]
                failed_downloads = [dr for dr in download_results if dr.status == FileStatus.FAILED]

                if failed_downloads:
                    logger.warning("{} files failed to download", len(failed_downloads))
                    for dr in failed_downloads:
                        dummy_ex = ExtractionResult(
                            download_result=dr,
                            status=FileStatus.FAILED,
                            error=dr.error,
                        )
                        run.results.append(
                            ProcessingResult(
                                extraction_result=dummy_ex,
                                status=FileStatus.FAILED,
                                error=dr.error,
                            )
                        )

                if successful_downloads:
                    extraction_results = self._extract_stage(successful_downloads, force_extract)
                    successful_extractions = [
                        er for er in extraction_results
                        if er.status in (FileStatus.EXTRACTED, FileStatus.SKIPPED)
                    ]
                    failed_extractions = [er for er in extraction_results if er.status == FileStatus.FAILED]

                    if failed_extractions:
                        logger.warning("{} files failed to extract", len(failed_extractions))
                        for er in failed_extractions:
                            run.results.append(
                                ProcessingResult(
                                    extraction_result=er,
                                    status=FileStatus.FAILED,
                                    error=er.error,
                                )
                            )

                    if successful_extractions:
                        run.results.extend(self._process_stage(successful_extractions))

        except Exception as exc:
            logger.critical("Pipeline aborted: {}", exc)
            raise

        finally:
            session.close()
            run.finished_at = datetime.utcnow()
            _save_run_report(run)
            _log_summary(run)
            structured_logger.info("=== PIPELINE END ===", operation="pipeline_complete")

        return run


def run_pipeline(
    groups: Optional[list[str]] = None,
    snapshot_date: Optional[str] = None,
    force_download: bool = False,
    force_extract: bool = False,
    download_workers: int = TOTAL_DOWNLOAD_WORKERS,
    process_workers: int = TOTAL_PROCESS_WORKERS,
    reference_only: bool = False,
    snapshot: Optional["Snapshot"] = None,
    reuse_processed: bool = False,
) -> PipelineRun:
    """Compatibilidade pública com a API funcional anterior."""
    return CNPJPipeline().run(
        groups=groups,
        snapshot_date=snapshot_date,
        force_download=force_download,
        force_extract=force_extract,
        download_workers=download_workers,
        process_workers=process_workers,
        reference_only=reference_only,
        snapshot=snapshot,
        reuse_processed=reuse_processed,
    )


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
        logger.debug("Run report saved: {}", report_path)
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
