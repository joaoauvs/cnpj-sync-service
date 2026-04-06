"""
Storage backends for pipeline output.

Supported backends
------------------
csv     — plain CSV (UTF-8, comma-separated).  Simple, universal, slow for
          large datasets.
parquet — Apache Parquet with Snappy compression (default).  Columnar,
          typed, ~3-5x smaller than CSV, readable by pandas/DuckDB/Spark.
duckdb  — writes directly into a DuckDB database file.  Best for local
          analytical queries via SQL without any extra infrastructure.

All backends share the same interface so the processor never needs to know
which one is active.  Switch via config.STORAGE_BACKEND or --format CLI flag.

Future evolution
----------------
Adding SQLAlchemy support (PostgreSQL, etc.) is a matter of implementing
SQLAlchemyWriter below and pointing config.STORAGE_BACKEND = "sqlalchemy".
"""

from __future__ import annotations

import abc
from pathlib import Path
from typing import Optional

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
# Parquet backend
# ---------------------------------------------------------------------------

class ParquetWriter(StorageWriter):
    """
    Streams chunks into a single Parquet file via PyArrow's ParquetWriter.

    Each chunk becomes one row-group; this avoids loading the full file
    into memory while still producing a single, efficient output file.

    Compression: Snappy (good balance of speed vs size).
    """

    _writer: Optional[object] = None  # pyarrow.parquet.ParquetWriter

    def _open(self) -> None:
        if self.path.exists():
            self.path.unlink()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = None  # initialised lazily on first chunk (need schema)
        logger.debug("ParquetWriter: {}", self.path)

    def write(self, chunk: pd.DataFrame) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self.path,
                table.schema,
                compression="snappy",
            )

        self._writer.write_table(table)
        self._rows += len(chunk)

    def _close(self) -> None:
        if self._writer is not None:
            self._writer.close()
        logger.debug("ParquetWriter closed: {:,} rows → {}", self._rows, self.path.name)


# ---------------------------------------------------------------------------
# DuckDB backend
# ---------------------------------------------------------------------------

class DuckDBWriter(StorageWriter):
    """
    Writes chunks directly into a DuckDB database table.

    A single .duckdb file is shared across all groups (configured via
    config.DUCKDB_PATH).  Each group maps to one table.

    DuckDB is a zero-infrastructure analytical database: no server, no
    configuration — just a file.  It supports full SQL and can also query
    Parquet files directly without importing them.
    """

    _conn: Optional[object] = None    # duckdb.DuckDBPyConnection
    _table_created: bool = False

    def _open(self) -> None:
        import duckdb
        from src.config import DUCKDB_PATH

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(DUCKDB_PATH))
        self._table_created = False
        logger.debug("DuckDBWriter: table={} db={}", self.group, DUCKDB_PATH)

    def write(self, chunk: pd.DataFrame) -> None:
        if not self._table_created:
            # CREATE OR REPLACE so re-runs are idempotent
            self._conn.execute(
                f"CREATE OR REPLACE TABLE {self.group} AS SELECT * FROM chunk LIMIT 0"
            )
            self._table_created = True

        self._conn.execute(f"INSERT INTO {self.group} SELECT * FROM chunk")
        self._rows += len(chunk)

    def _close(self) -> None:
        if self._conn is not None:
            self._conn.close()
        logger.debug("DuckDBWriter closed: {:,} rows → table {}", self._rows, self.group)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_BACKENDS = {
    "csv": CSVWriter,
    "parquet": ParquetWriter,
    "duckdb": DuckDBWriter,
}

_EXTENSIONS = {
    "csv": ".csv",
    "parquet": ".parquet",
    "duckdb": ".parquet",   # DuckDB backend still writes a sidecar Parquet for portability? No — use .duckdb directly
}


def get_writer(backend: str, path: Path, group: str) -> StorageWriter:
    """
    Return a ``StorageWriter`` for the given backend name.

    Parameters
    ----------
    backend : "csv" | "parquet" | "duckdb"
    path    : Output file path (ignored for duckdb — uses config.DUCKDB_PATH).
    group   : Logical group name (table name for duckdb).
    """
    cls = _BACKENDS.get(backend.lower())
    if cls is None:
        raise ValueError(
            f"Unknown storage backend '{backend}'. "
            f"Valid options: {list(_BACKENDS)}"
        )
    return cls(path, group)


def output_extension(backend: str) -> str:
    """Return the file extension for the given backend."""
    return {
        "csv": ".csv",
        "parquet": ".parquet",
        "duckdb": ".duckdb",
    }.get(backend.lower(), ".csv")
