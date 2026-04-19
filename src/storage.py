"""
Storage backend for pipeline output: CSV and Parquet.
"""

from __future__ import annotations

import abc
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.logger_enhanced import logger

# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class StorageWriter(abc.ABC):
    """
    Context-manager that accepts DataFrame chunks and writes them to
    a single output artefact (file or database table).

    Usage
    -----
    with backend.open(path, group) as writer:
        for chunk in ...:
            writer.write(chunk)
    """

    def __init__(self, path: Path, group: str) -> None:
        self.path = path
        self.group = group
        self._rows = 0

    @property
    def rows_written(self) -> int:
        return self._rows

    @abc.abstractmethod
    def _open(self) -> None:
        """Initialise the writer (open file handles, create tables, etc.)."""

    @abc.abstractmethod
    def write(self, chunk: pd.DataFrame) -> None:
        """Write one chunk of rows."""

    @abc.abstractmethod
    def _close(self) -> None:
        """Flush and close the writer."""

    def __enter__(self) -> "StorageWriter":
        self._open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._close()
        return False


# ---------------------------------------------------------------------------
# CSV backend
# ---------------------------------------------------------------------------

class CSVWriter(StorageWriter):
    """Appends chunks to a single UTF-8 CSV file."""

    _first: bool = True

    def _open(self) -> None:
        if self.path.exists():
            self.path.unlink()
        self._first = True
        logger.debug("CSVWriter: {}", self.path)

    def write(self, chunk: pd.DataFrame) -> None:
        chunk.to_csv(
            self.path,
            mode="a",
            index=False,
            header=self._first,
            encoding="utf-8",
        )
        self._rows += len(chunk)
        self._first = False

    def _close(self) -> None:
        logger.debug("CSVWriter closed: {:,} rows → {}", self._rows, self.path.name)


# ---------------------------------------------------------------------------
# Parquet backend
# ---------------------------------------------------------------------------

class ParquetWriter(StorageWriter):
    """Writes chunks incrementally to a single Parquet file (one row group per chunk).

    Uses pyarrow.parquet.ParquetWriter directly so chunks are flushed to disk
    as they arrive — no full-file accumulation in memory.
    """

    _pq_writer: Optional[pq.ParquetWriter] = None

    def _open(self) -> None:
        if self.path.exists():
            self.path.unlink()
        self._pq_writer = None
        logger.debug("ParquetWriter: {}", self.path)

    def write(self, chunk: pd.DataFrame) -> None:
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if self._pq_writer is None:
            self._pq_writer = pq.ParquetWriter(self.path, table.schema)
        self._pq_writer.write_table(table)
        self._rows += len(chunk)

    def _close(self) -> None:
        if self._pq_writer is not None:
            self._pq_writer.close()
            self._pq_writer = None
        logger.debug("ParquetWriter closed: {:,} rows → {}", self._rows, self.path.name)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_writer(backend: str, path: Path, group: str) -> "StorageWriter":
    """Return a StorageWriter. Supports 'csv' and 'parquet'."""
    backend_lower = backend.lower()
    if backend_lower == "csv":
        return CSVWriter(path, group)
    elif backend_lower == "parquet":
        return ParquetWriter(path, group)
    else:
        raise ValueError(f"Unknown storage backend '{backend}'. Supported: 'csv', 'parquet'.")


def output_extension(backend: str) -> str:
    """Return the file extension for the given backend."""
    backend_lower = backend.lower()
    if backend_lower == "csv":
        return ".csv"
    elif backend_lower == "parquet":
        return ".parquet"
    else:
        raise ValueError(f"Unknown storage backend '{backend}'.")
