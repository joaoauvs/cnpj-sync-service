#!/usr/bin/env python3
"""
Sincronização CNPJ com PostgreSQL — Script Principal

Configuração via .env:
    DATABASE_URL     URL completa PostgreSQL (opcional, tem prioridade)
                     Ex: postgresql://user:pass@host:5432/db
    DB_SERVER        Host do PostgreSQL      (padrão: localhost)
    DB_DATABASE      Nome do banco           (padrão: postgres)
    DB_USERNAME      Usuário PostgreSQL
    DB_PASSWORD      Senha PostgreSQL
    LOG_LEVEL        DEBUG | INFO | WARNING | ERROR  (padrão: INFO)
    FORCE_SYNC       true | 1 — re-sincroniza mesmo que snapshot já processado
    SNAPSHOT_DATE    YYYY-MM ou YYYY-MM-DD — data específica (padrão: mais recente)
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from datetime import date, datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from src.config import DOWNLOAD_WORKERS, DOWNLOADS_DIR, EXTRACTED_DIR, PROCESS_WORKERS, PROCESSED_DIR
from src.logger_enhanced import logger, setup_enhanced_logging, structured_logger

_DB_SERVER     = os.getenv("DB_SERVER", "localhost")
_DB_DATABASE   = os.getenv("DB_DATABASE", "postgres")
_DB_USERNAME   = os.getenv("DB_USERNAME") or os.getenv("POSTGRES_USER")
_DB_PASSWORD   = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
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


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("valor deve ser maior que zero")
    return value


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
        download_workers: int = DOWNLOAD_WORKERS,
        process_workers: int = PROCESS_WORKERS,
    ) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.log_level = log_level
        self.force = force
        self.snapshot_date_raw = snapshot_date_raw
        self.reuse_processed = reuse_processed_env and not force
        self.download_workers = download_workers
        self.process_workers = process_workers

    def create_database(self):
        from src.database import CNPJDatabase

        return CNPJDatabase(
            server=self.server,
            database=self.database,
            username=self.username,
            password=self.password,
        )

    def create_sync(self, db):
        from src.sync import CNPJSync

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
            download_workers=self.download_workers,
            process_workers=self.process_workers,
            reference_only=False,
            force=self.force,
            reuse_processed=self.reuse_processed,
        )

        elapsed = time.perf_counter() - start
        return _log_total_execution_time(result, elapsed)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sincronização CNPJ com PostgreSQL")
    parser.add_argument("--force", action="store_true", default=_FORCE, help="Re-sincroniza mesmo se o snapshot já foi processado")
    parser.add_argument("--date", dest="snapshot_date_raw", default=_SNAPSHOT_DATE, help="Snapshot alvo em YYYY-MM ou YYYY-MM-DD")
    parser.add_argument("--log-level", default=_LOG_LEVEL, choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Nível de log")
    parser.add_argument("--workers", type=_positive_int, help="Define o mesmo valor para download e processamento")
    parser.add_argument("--download-workers", type=_positive_int, default=None, help="Workers de download")
    parser.add_argument("--process-workers", type=_positive_int, default=None, help="Workers de processamento")
    parser.add_argument("--server", default=_DB_SERVER, help="Host do PostgreSQL")
    parser.add_argument("--database", default=_DB_DATABASE, help="Nome do banco PostgreSQL")
    parser.add_argument("--username", default=_DB_USERNAME, help="Usuário do banco")
    parser.add_argument("--password", default=_DB_PASSWORD, help="Senha do banco")
    parser.add_argument(
        "--reuse-processed",
        dest="reuse_processed",
        action="store_true",
        default=_REUSE_PROCESSED,
        help="Reaproveita artefatos processados já existentes",
    )
    parser.add_argument(
        "--no-reuse-processed",
        dest="reuse_processed",
        action="store_false",
        help="Descarta artefatos processados e reprocesa do zero",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    shared_workers = args.workers
    download_workers = args.download_workers or shared_workers or DOWNLOAD_WORKERS
    process_workers = args.process_workers or shared_workers or PROCESS_WORKERS

    app = CNPJSyncApplication(
        server=args.server,
        database=args.database,
        username=args.username,
        password=args.password,
        log_level=args.log_level,
        force=args.force,
        snapshot_date_raw=args.snapshot_date_raw,
        reuse_processed_env=args.reuse_processed,
        download_workers=download_workers,
        process_workers=process_workers,
    )
    sys.exit(app.run())


if __name__ == "__main__":
    main()
