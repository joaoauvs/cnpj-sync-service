#!/usr/bin/env python3
"""
Sincronização CNPJ com SQL Server — Script Principal

Configuração via .env:
    DB_SERVER        Endereço do SQL Server  (padrão: 72.60.4.227)
    DB_DATABASE      Nome do banco           (padrão: receita-federal)
    DB_USERNAME      Usuário SQL Server
    DB_PASSWORD      Senha SQL Server
    LOG_LEVEL        DEBUG | INFO | WARNING | ERROR  (padrão: INFO)
    FORCE_SYNC       true | 1 — re-sincroniza mesmo que snapshot já processado
    SNAPSHOT_DATE    YYYY-MM ou YYYY-MM-DD — data específica (padrão: mais recente)
"""

from __future__ import annotations

import os
import shutil
import sys
import time
from datetime import date, datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from src.config import DOWNLOAD_WORKERS, DOWNLOADS_DIR, EXTRACTED_DIR, PROCESS_WORKERS, PROCESSED_DIR
from src.database import CNPJDatabase
from src.logger_enhanced import logger, setup_enhanced_logging, structured_logger
from src.sync import CNPJSync

_DB_SERVER     = os.getenv("DB_SERVER", "72.60.4.227")
_DB_DATABASE   = os.getenv("DB_DATABASE", "receita-federal")
_DB_USERNAME   = os.getenv("DB_USERNAME") or os.getenv("SQLSERVER_USERNAME")
_DB_PASSWORD   = os.getenv("DB_PASSWORD") or os.getenv("SQLSERVER_PASSWORD")
_LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO").upper()
_FORCE         = os.getenv("FORCE_SYNC", "false").lower() in ("1", "true", "yes")
_SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE")  # YYYY-MM ou YYYY-MM-DD, ou None
_REUSE_PROCESSED = os.getenv("REUSE_PROCESSED", "true").lower() in ("1", "true", "yes")


def _clean_data_dir(reuse_processed: bool = False) -> None:
    targets = [EXTRACTED_DIR]
    if not reuse_processed:
        targets.append(PROCESSED_DIR)

    for d in targets:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)

    if reuse_processed:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)


def _parse_snapshot_date(raw: str) -> date:
    """Aceita YYYY-MM (→ primeiro dia do mês) e YYYY-MM-DD."""
    if len(raw) == 7:
        return datetime.strptime(raw + "-01", "%Y-%m-%d").date()
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _fmt_elapsed(seconds: float) -> str:
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}min {s:02d}s"
    if m:
        return f"{m}min {s:02d}s"
    return f"{s}s"


def _log_total_execution_time(result: dict, elapsed_seconds: float) -> int:
    """Centraliza o log do tempo total e define o exit code da execução."""
    elapsed = _fmt_elapsed(elapsed_seconds)

    if result.get("success"):
        logger.info(
            "=== SINCRONIZAÇÃO CONCLUÍDA: {}/{} arquivos, {:,} registros — tempo total: {} ===",
            result.get("successful_files", 0),
            result.get("total_files", 0),
            result.get("total_records", 0),
            elapsed,
        )
        return 0

    if result.get("skipped"):
        logger.info(
            "=== SEM AÇÃO: {} — tempo total: {} ===",
            result.get("message", ""),
            elapsed,
        )
        return 0

    logger.error(
        "=== SINCRONIZAÇÃO FALHOU: {} — tempo total: {} ===",
        result.get("message", ""),
        elapsed,
    )
    return 1


class CNPJSyncApplication:
    """Aplicação principal orientada a objeto para o sync CNPJ."""

    def __init__(
        self,
        server: str = _DB_SERVER,
        database: str = _DB_DATABASE,
        username: Optional[str] = _DB_USERNAME,
        password: Optional[str] = _DB_PASSWORD,
        log_level: str = _LOG_LEVEL,
        force: bool = _FORCE,
        snapshot_date_raw: Optional[str] = _SNAPSHOT_DATE,
        reuse_processed_env: bool = _REUSE_PROCESSED,
    ) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.log_level = log_level
        self.force = force
        self.snapshot_date_raw = snapshot_date_raw
        self.reuse_processed = reuse_processed_env and not force

    def create_database(self) -> CNPJDatabase:
        return CNPJDatabase(
            server=self.server,
            database=self.database,
            username=self.username,
            password=self.password,
        )

    def create_sync(self, db: CNPJDatabase) -> CNPJSync:
        return CNPJSync(db_connection=db)

    def resolve_snapshot_date(self) -> Optional[date]:
        if not self.snapshot_date_raw:
            return None
        return _parse_snapshot_date(self.snapshot_date_raw)

    def run(self) -> int:
        setup_enhanced_logging(level=self.log_level)
        start = time.perf_counter()
        structured_logger.set_correlation_id()
        _clean_data_dir(reuse_processed=self.reuse_processed)
        logger.info("=== CNPJ SYNC SERVICE INICIADO ===")
        logger.info("Reuse processed: {}", self.reuse_processed)

        try:
            db = self.create_database()
            sync = self.create_sync(db)
        except Exception as exc:
            logger.error("Erro ao criar conexão com banco: {}", exc)
            return 1

        if not sync.initialize_database():
            logger.error("Falha ao inicializar banco de dados")
            return 1

        if not db.test_connection():
            logger.error("Falha na conexão com o banco de dados")
            return 1

        try:
            snapshot_date = self.resolve_snapshot_date()
        except ValueError:
            logger.error("SNAPSHOT_DATE inválido: '{}'. Use YYYY-MM ou YYYY-MM-DD", self.snapshot_date_raw)
            return 1

        result = sync.sync_snapshot(
            snapshot_date=snapshot_date,
            groups=None,
            force_download=self.force,
            force_extract=self.force,
            download_workers=DOWNLOAD_WORKERS,
            process_workers=PROCESS_WORKERS,
            reference_only=False,
            force=self.force,
            reuse_processed=self.reuse_processed,
        )

        elapsed = time.perf_counter() - start
        return _log_total_execution_time(result, elapsed)


def main() -> None:
    sys.exit(CNPJSyncApplication().run())


if __name__ == "__main__":
    main()
