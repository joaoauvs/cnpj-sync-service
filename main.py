#!/usr/bin/env python3
"""
Sincronização CNPJ com SQL Server — Script Principal

Fluxo:
  1. Descobre a data do snapshot mais recente disponível na RF
  2. Verifica se esse snapshot já foi processado (controle_sincronizacao)
  3. Se não, baixa, extrai, processa e carrega no SQL Server
  4. Registra resultado na tabela de controle

Uso:
    python main.py [--force] [--date YYYY-MM-DD] [--log-level DEBUG]

Variáveis de ambiente (.env):
    DB_USERNAME / SQLSERVER_USERNAME
    DB_PASSWORD / SQLSERVER_PASSWORD
    DB_SERVER        (padrão: 72.60.4.227)
    DB_DATABASE      (padrão: receita-federal)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))

from src.config import HEADERS
from src.crawler import discover_latest_snapshot_with_fallback
from src.database import CNPJDatabase
from src.logger import logger, setup_logging
from src.sync import CNPJSync

# ---------------------------------------------------------------------------
# Descoberta de snapshot
# ---------------------------------------------------------------------------

def get_latest_snapshot_date() -> Optional[date]:
    """Consulta o site da RF e retorna a data do snapshot mais recente."""
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        snapshot = discover_latest_snapshot_with_fallback(session=session)
        session.close()

        if snapshot and snapshot.date:
            dt = datetime.strptime(snapshot.date, "%Y-%m-%d").date()
            logger.info("Snapshot mais recente disponível: {}", dt)
            return dt

        logger.error("Não foi possível determinar a data do snapshot")
        return None
    except Exception as e:
        logger.error("Erro ao consultar snapshot: {}", e)
        return None


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Sincronização CNPJ → SQL Server")
    parser.add_argument("--force", action="store_true",
                        help="Forçar sincronização mesmo se snapshot já processado")
    parser.add_argument("--date", type=str, metavar="YYYY-MM-DD",
                        help="Data específica do snapshot. Padrão: mais recente disponível")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--server",
                        default=os.getenv("DB_SERVER", "72.60.4.227"))
    parser.add_argument("--database",
                        default=os.getenv("DB_DATABASE", "receita-federal"))
    parser.add_argument("--username", default=None,
                        help="Usuário SQL Server (padrão: variável DB_USERNAME)")
    parser.add_argument("--password", default=None,
                        help="Senha SQL Server (padrão: variável DB_PASSWORD)")
    parser.add_argument("--workers", type=int, default=4,
                        help="Número de workers para download e processamento (padrão: 4)")

    args = parser.parse_args()
    setup_logging(level=args.log_level)

    _start = time.perf_counter()
    logger.info("=== CNPJ SYNC SERVICE INICIADO ===")

    # Conexão com o banco
    try:
        db = CNPJDatabase(
            server=args.server,
            database=args.database,
            username=args.username,
            password=args.password,
        )
        sync = CNPJSync(db_connection=db)
    except Exception as e:
        logger.error("Erro ao criar conexão com banco: {}", e)
        sys.exit(1)

    # Inicializar banco (cria se não existe)
    logger.info("Verificando banco de dados...")
    if not sync.initialize_database():
        logger.error("Falha ao inicializar banco de dados")
        sys.exit(1)

    if not db.test_connection():
        logger.error("Falha na conexão com o banco de dados")
        sys.exit(1)

    logger.info("Banco de dados pronto")

    # Determinar data do snapshot alvo
    if args.date:
        try:
            snapshot_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            logger.info("Usando snapshot especificado: {}", snapshot_date)
        except ValueError:
            logger.error("Data inválida: {}. Use formato YYYY-MM-DD", args.date)
            sys.exit(1)
    else:
        snapshot_date = get_latest_snapshot_date()
        if not snapshot_date:
            logger.error("Não foi possível obter data do snapshot mais recente")
            sys.exit(1)

    # Verificar se precisa sincronizar (antes de baixar qualquer arquivo)
    if not args.force:
        needs, msg = sync.check_snapshot_needs_sync(snapshot_date)
        if not needs:
            logger.info("{} — encerrando sem ação", msg)
            _elapsed = time.perf_counter() - _start
            logger.info("=== CNPJ SYNC SERVICE ENCERRADO (SEM AÇÃO) — tempo total: {} ===",
                        _fmt_elapsed(_elapsed))
            sys.exit(0)

    # Sincronizar
    result = sync.sync_snapshot(
        snapshot_date=snapshot_date,
        groups=None,
        force_download=args.force,
        force_extract=args.force,
        download_workers=args.workers,
        process_workers=args.workers,
        reference_only=False,
        force=args.force,
    )

    _elapsed = time.perf_counter() - _start
    if result.get("success"):
        logger.info(
            "=== SINCRONIZAÇÃO CONCLUÍDA: {}/{} arquivos, {:,} registros — tempo total: {} ===",
            result.get("successful_files", 0),
            result.get("total_files", 0),
            result.get("total_records", 0),
            _fmt_elapsed(_elapsed),
        )
        sys.exit(0)
    else:
        logger.error("=== SINCRONIZAÇÃO FALHOU: {} — tempo total: {} ===",
                     result.get("message", ""), _fmt_elapsed(_elapsed))
        sys.exit(1)


def _fmt_elapsed(seconds: float) -> str:
    """Formata segundos em string legível: '1h 23min 45s' ou '4min 30s' ou '45s'."""
    total = int(seconds)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h:
        return f"{h}h {m:02d}min {s:02d}s"
    if m:
        return f"{m}min {s:02d}s"
    return f"{s}s"


if __name__ == "__main__":
    main()
