"""
Storage backend for pipeline output: CSV only.
"""

from __future__ import annotations

import abc
from pathlib import Path

import pandas as pd

from src.logger import logger

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
# Factory
# ---------------------------------------------------------------------------

def get_writer(backend: str, path: Path, group: str) -> "StorageWriter":
    """Return a StorageWriter. Currently only 'csv' is supported."""
    if backend.lower() != "csv":
        raise ValueError(f"Unknown storage backend '{backend}'. Only 'csv' is supported.")
    return CSVWriter(path, group)


def output_extension(backend: str) -> str:
    """Return the file extension for the given backend."""
    return ".csv"
