"""
Script de teste: carrega apenas os arquivos Estabelecimentos*.parquet
já processados no PostgreSQL, sem fazer download/extração/processamento.

Uso:
    python test_estab.py
    python test_estab.py --file Estabelecimentos0.parquet   # arquivo específico
    python test_estab.py --dry-run                          # só lê, não grava
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from src.config import PROCESSED_DIR, CSV_CHUNK_ROWS
from src.database import CNPJDatabase
from src.logger_enhanced import logger, setup_enhanced_logging
from src.sync import CNPJSync, ProcessedFileReader

_DB_SERVER   = os.getenv("DB_SERVER", "localhost")
_DB_DATABASE = os.getenv("DB_DATABASE", "postgres")
_DB_USERNAME = os.getenv("DB_USERNAME") or os.getenv("POSTGRES_USER")
_DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
_SNAPSHOT    = os.getenv("SNAPSHOT_DATE", "2026-04-01")


def main() -> None:
    parser = argparse.ArgumentParser(description="Teste de carga de Estabelecimentos")
    parser.add_argument("--file", help="Parquet específico (ex: Estabelecimentos0.parquet)")
    parser.add_argument("--dry-run", action="store_true", help="Lê e prepara dados mas não grava no banco")
    parser.add_argument("--snapshot", default=_SNAPSHOT, help="Data do snapshot YYYY-MM-DD")
    args = parser.parse_args()

    setup_enhanced_logging("INFO")

    snapshot_date = date.fromisoformat(args.snapshot)

    processed_root = Path(PROCESSED_DIR)
    preferred_dir = processed_root / args.snapshot
    fallback_dir = processed_root / args.snapshot[:7] if len(args.snapshot) == 10 else preferred_dir
    processed_dir = preferred_dir if preferred_dir.exists() else fallback_dir
    if args.file:
        files = [processed_dir / args.file]
    else:
        files = sorted(processed_dir.glob("Estabelecimentos*.parquet"))

    if not files:
        logger.error("Nenhum arquivo Estabelecimentos*.parquet encontrado em {}", processed_dir)
        sys.exit(1)

    logger.info("Arquivos a processar: {}", [f.name for f in files])

    if args.dry_run:
        logger.info("=== DRY RUN — lendo e preparando dados sem gravar ===")
        reader = ProcessedFileReader()
        for f in files:
            logger.info("Lendo {}...", f.name)
            try:
                chunks = list(reader.read(f, chunksize=CSV_CHUNK_ROWS))
                total = sum(len(c) for c in chunks)
                logger.info("  {} chunks, {:,} linhas OK", len(chunks), total)
                # Mostra max lengths por coluna no primeiro chunk
                if chunks:
                    chunk = chunks[0]
                    str_cols = chunk.select_dtypes(include="object").columns
                    over = {c: int(chunk[c].dropna().str.len().max())
                            for c in str_cols if chunk[c].dropna().str.len().max() > 0}
                    top = sorted(over.items(), key=lambda x: -x[1])[:10]
                    logger.info("  Top 10 max lengths: {}", top)
            except Exception as e:
                logger.error("  ERRO ao ler {}: {}", f.name, e)
        return

    db = CNPJDatabase(
        server=_DB_SERVER,
        database=_DB_DATABASE,
        username=_DB_USERNAME,
        password=_DB_PASSWORD,
    )

    sync = CNPJSync(db_connection=db)

    total_ins = total_upd = 0
    failed = []
    t0 = time.monotonic()

    for f in files:
        logger.info("=== Carregando {} ===", f.name)
        ft = time.monotonic()
        try:
            ins, upd = sync._load_estabelecimentos(f, snapshot_date)
            total_ins += ins
            total_upd += upd
            elapsed = time.monotonic() - ft
            logger.info("  OK: {} inseridos, {} atualizados — {:.1f}s", ins, upd, elapsed)
        except Exception as e:
            logger.error("  FALHA em {}: {}", f.name, e)
            failed.append(f.name)

    elapsed_total = time.monotonic() - t0
    logger.info("=== RESULTADO: {:,} ins, {:,} upd | falhas={} | {:.0f}s ===",
                total_ins, total_upd, failed, elapsed_total)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
