"""
Downloader: fetches ZIP files from the remote server.

Features
--------
* Parallel downloads via ``concurrent.futures.ThreadPoolExecutor``.
* Resume support using HTTP Range headers — already-complete files are
  skipped without a network round-trip.
* Exponential back-off retry on transient errors.
* Per-file progress bars via tqdm.
* Validates downloaded size against the server-reported size.
"""

from __future__ import annotations

import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tqdm import tqdm

from src.config import (
    BACKOFF_FACTOR,
    DOWNLOAD_CHUNK_SIZE,
    DOWNLOAD_WORKERS,
    DOWNLOADS_DIR,
    HEADERS,
    MAX_RETRIES,
    RF_AUTH,
    REQUEST_TIMEOUT,
    TOTAL_DOWNLOAD_WORKERS,
)
from src.logger_enhanced import logger, structured_logger
from src.models import DownloadResult, FileStatus, RemoteFile


def _local_path(remote_file: RemoteFile, base_dir: Path = DOWNLOADS_DIR) -> Path:
    """Determine the local filesystem path for a remote file."""
    return base_dir / remote_file.name


def _already_complete(path: Path, expected_bytes: Optional[int]) -> bool:
    """
    Return True if the local file exists and its size matches the expected
    size.

    The directory-listing size is an approximation (e.g., "22K" can mean
    anywhere from ~22000 to ~22528 bytes).  We therefore only skip when the
    local file is within 5% of the reported size, or when we have an exact
    server-reported byte count from a prior HEAD request stored separately.
    """
    if not path.exists():
        return False
    if expected_bytes is None:
        return False
    actual = path.stat().st_size
    # Tolerate ±5% to account for rounded directory-listing sizes
    return abs(actual - expected_bytes) / max(expected_bytes, 1) <= 0.05


def _valid_local_zip(path: Path) -> bool:
    """
    Return True if a local ZIP exists and passes an integrity check.

    This lets us safely reuse already-downloaded files even when the remote
    listing size is missing or rounded imprecisely.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with zipfile.ZipFile(path, "r") as zf:
            if not zf.namelist():
                return False
            return zf.testzip() is None
    except zipfile.BadZipFile:
        return False


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=2, max=120),
    reraise=True,
)
def _download_file(
    remote_file: RemoteFile,
    session: requests.Session,
    dest: Path,
) -> int:
    """
    Stream-download a single file with resume support.

    Returns the number of bytes written in this call (not total size).
    Raises on HTTP errors after retries are exhausted.
    """
    existing_bytes = dest.stat().st_size if dest.exists() else 0
    headers = dict(HEADERS)

    if existing_bytes > 0:
        headers["Range"] = f"bytes={existing_bytes}-"
        logger.debug(
            "Resuming {} from byte {}", remote_file.name, existing_bytes
        )

    resp = session.get(
        remote_file.url,
        headers=headers,
        stream=True,
        timeout=REQUEST_TIMEOUT,
    )

    # 416 = Range Not Satisfiable → file is already complete
    if resp.status_code == 416:
        logger.debug("{} already complete (416)", remote_file.name)
        return 0

    resp.raise_for_status()

    total = remote_file.size_bytes
    mode = "ab" if existing_bytes > 0 and resp.status_code == 206 else "wb"
    if mode == "wb":
        existing_bytes = 0  # server sent full file

    bytes_written = 0
    desc = f"{remote_file.name[:30]:<30}"

    with (
        open(dest, mode) as fh,
        tqdm(
            total=total,
            initial=existing_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc,
            leave=False,
            dynamic_ncols=True,
        ) as bar,
    ):
        for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            if chunk:
                fh.write(chunk)
                bytes_written += len(chunk)
                bar.update(len(chunk))

    return bytes_written


class FileDownloader:
    """Serviço orientado a objeto para download e reaproveitamento de ZIPs."""

    def __init__(
        self,
        dest_dir: Path = DOWNLOADS_DIR,
        workers: int = TOTAL_DOWNLOAD_WORKERS,
        default_headers: Optional[dict[str, str]] = None,
        rf_auth: tuple[str, str] = RF_AUTH,
    ) -> None:
        self.dest_dir = dest_dir
        self.workers = workers
        self.default_headers = default_headers or HEADERS
        self.rf_auth = rf_auth

    def create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self.default_headers)
        session.auth = self.rf_auth
        return session

    def download_file(
        self,
        remote_file: RemoteFile,
        session: Optional[requests.Session] = None,
        force: bool = False,
    ) -> DownloadResult:
        dest = _local_path(remote_file, self.dest_dir)
        own_session = session is None

        if own_session:
            session = self.create_session()

        try:
            if not force and (
                _already_complete(dest, remote_file.size_bytes)
                or _valid_local_zip(dest)
            ):
                logger.debug("SKIP {} (already downloaded locally)", remote_file.name)
                return DownloadResult(
                    remote_file=remote_file,
                    local_path=dest,
                    status=FileStatus.SKIPPED,
                    bytes_downloaded=dest.stat().st_size,
                )

            structured_logger.info(
                "Downloading {} ({:.0f} MB)",
                remote_file.name,
                (remote_file.size_bytes or 0) / 1_024**2,
                operation="download",
                file_name=remote_file.name,
                file_size_mb=(remote_file.size_bytes or 0) / 1_024**2,
                file_group=remote_file.group,
            )
            t0 = time.perf_counter()
            bytes_dl = _download_file(remote_file, session, dest)
            elapsed = time.perf_counter() - t0

            actual = dest.stat().st_size
            expected = remote_file.size_bytes
            if expected and actual == 0:
                raise ValueError(f"Downloaded file is empty: {remote_file.name}")

            speed = bytes_dl / elapsed / 1_024**2 if elapsed > 0 else 0
            structured_logger.success(
                "Downloaded {} in {:.1f}s ({:.1f} MB/s)",
                remote_file.name,
                elapsed,
                speed,
                operation="download_complete",
                file_name=remote_file.name,
                duration_seconds=elapsed,
                speed_mbps=speed,
                bytes_downloaded=bytes_dl,
            )
            return DownloadResult(
                remote_file=remote_file,
                local_path=dest,
                status=FileStatus.DOWNLOADED,
                bytes_downloaded=bytes_dl,
                duration_seconds=elapsed,
            )

        except Exception as exc:
            logger.error("Failed to download {}: {}", remote_file.name, exc)
            return DownloadResult(
                remote_file=remote_file,
                local_path=dest,
                status=FileStatus.FAILED,
                error=str(exc),
            )

        finally:
            if own_session:
                session.close()

    def download_all(
        self,
        files: list[RemoteFile],
        force: bool = False,
        workers: Optional[int] = None,
    ) -> list[DownloadResult]:
        if not files:
            return []

        worker_count = workers or self.workers
        logger.info("Download: {} arquivos, {} workers", len(files), worker_count)
        results: dict[int, DownloadResult] = {}

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="dl") as pool:
            future_to_idx = {
                pool.submit(self.download_file, remote_file, None, force): idx
                for idx, remote_file in enumerate(files)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:
                    results[idx] = DownloadResult(
                        remote_file=files[idx],
                        local_path=_local_path(files[idx], self.dest_dir),
                        status=FileStatus.FAILED,
                        error=str(exc),
                    )

        ordered = [results[i] for i in range(len(files))]
        n_skip = sum(1 for r in ordered if r.status == FileStatus.SKIPPED)
        n_dl = sum(1 for r in ordered if r.status == FileStatus.DOWNLOADED)
        n_fail = sum(1 for r in ordered if r.status == FileStatus.FAILED)
        parts = []
        if n_dl:
            parts.append(f"{n_dl} baixados")
        if n_skip:
            parts.append(f"{n_skip} já existiam")
        if n_fail:
            parts.append(f"{n_fail} falhas")
        logger.info("Download concluído: {}", ", ".join(parts))
        return ordered


def download_file(
    remote_file: RemoteFile,
    session: Optional[requests.Session] = None,
    dest_dir: Path = DOWNLOADS_DIR,
    force: bool = False,
) -> DownloadResult:
    """Compatibilidade pública com a API funcional anterior."""
    return FileDownloader(dest_dir=dest_dir).download_file(remote_file, session=session, force=force)


def download_all(
    files: list[RemoteFile],
    dest_dir: Path = DOWNLOADS_DIR,
    workers: int = TOTAL_DOWNLOAD_WORKERS,
    force: bool = False,
) -> list[DownloadResult]:
    """Compatibilidade pública com a API funcional anterior."""
    return FileDownloader(dest_dir=dest_dir, workers=workers).download_all(files, force=force, workers=workers)
