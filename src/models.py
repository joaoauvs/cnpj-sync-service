"""
Pydantic models used for discovery metadata and pipeline state.

These are NOT row-level validators — rows are validated via pandas dtype
coercion and the processor module.  These models capture high-level
pipeline artefacts so they can be serialised/deserialised cleanly.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class FileStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


class RemoteFile(BaseModel):
    """Represents a single ZIP file discovered on the remote index."""

    name: str                  # e.g. "Empresas0.zip"
    url: str                   # full URL
    group: str                 # e.g. "Empresas"
    partition: Optional[int]   # numeric suffix from filename (e.g. 0, 1, 10…); None for non-partitioned files
    size_bytes: Optional[int]  # None if server didn't report size
    last_modified: Optional[datetime]

    @property
    def stem(self) -> str:
        """File name without extension, e.g. 'Empresas0'."""
        return Path(self.name).stem


class Snapshot(BaseModel):
    """A dated snapshot directory on the remote server."""

    date: str           # YYYY-MM-DD
    url: str
    files: list[RemoteFile] = Field(default_factory=list)

    @property
    def total_size_bytes(self) -> int:
        return sum(f.size_bytes or 0 for f in self.files)


class DownloadResult(BaseModel):
    """Outcome of a single file download."""

    remote_file: RemoteFile
    local_path: Path
    status: FileStatus
    bytes_downloaded: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0


class ExtractionResult(BaseModel):
    """Outcome of extracting a ZIP into CSV(s)."""

    download_result: DownloadResult
    extracted_paths: list[Path] = Field(default_factory=list)
    status: FileStatus
    error: Optional[str] = None
    duration_seconds: float = 0.0


class ProcessingResult(BaseModel):
    """Outcome of processing a CSV into a structured output."""

    extraction_result: ExtractionResult
    output_path: Optional[Path] = None
    rows_written: int = 0
    rows_invalid: int = 0
    status: FileStatus
    error: Optional[str] = None
    duration_seconds: float = 0.0


class PipelineRun(BaseModel):
    """Top-level summary of a full pipeline execution."""

    snapshot_date: str
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    results: list[ProcessingResult] = Field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(r.rows_written for r in self.results)

    @property
    def failed_files(self) -> list[str]:
        return [
            r.extraction_result.download_result.remote_file.name
            for r in self.results
            if r.status == FileStatus.FAILED
        ]

    def summary(self) -> dict:
        return {
            "snapshot_date": self.snapshot_date,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "total_files": len(self.results),
            "successful": sum(1 for r in self.results if r.status == FileStatus.DONE),
            "failed": len(self.failed_files),
            "total_rows_written": self.total_rows,
            "failed_files": self.failed_files,
        }
