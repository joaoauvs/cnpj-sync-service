#!/usr/bin/env python3
"""
Apaga o banco de dados e recria o schema do zero.

Uso:
    python reset_database.py
    python reset_database.py --server 72.60.4.227 --database receita-federal
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))

from src.database import CNPJDatabase
from src.logger import logger, setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset do banco CNPJ — DROP + recriar schema")
    parser.add_argument("--server",   default=os.getenv("DB_SERVER", "72.60.4.227"))
    parser.add_argument("--database", default=os.getenv("DB_DATABASE", "receita-federal"))
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    setup_logging(level=args.log_level)

    db = CNPJDatabase(
        server=args.server,
        database=args.database,
        username=args.username,
        password=args.password,
    )

    logger.info("Conectando ao servidor {}...", args.server)

    # --- DROP DATABASE ---
    logger.warning("Apagando banco '{}' em {}...", args.database, args.server)
    try:
        with db.connect_master() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sys.databases WHERE name = ?", (args.database,))
            if not cur.fetchone():
                logger.info("Banco '{}' não existe, nada a apagar.", args.database)
            else:
                # Fechar conexões ativas antes do DROP
                cur.execute(f"""
                    ALTER DATABASE [{args.database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                """)
                cur.execute(f"DROP DATABASE [{args.database}]")
                logger.info("Banco '{}' apagado com sucesso.", args.database)
    except Exception as e:
        logger.error("Erro ao apagar banco: {}", e)
        sys.exit(1)

    # --- RECRIAR ---
    logger.info("Recriando banco '{}'...", args.database)
    if not db.create_database_if_not_exists():
        logger.error("Falha ao criar banco.")
        sys.exit(1)

    # --- EXECUTAR SCHEMA ---
    script = Path(__file__).parent / "sql" / "schema.sql"
    if not script.exists():
        logger.error("Script não encontrado: {}", script)
        sys.exit(1)

    logger.info("Executando schema.sql...")
    if not db.execute_schema_script(script):
        logger.error("Falha ao executar schema.")
        sys.exit(1)

    logger.info("=== Banco '{}' recriado com sucesso! ===", args.database)


if __name__ == "__main__":
    main()
