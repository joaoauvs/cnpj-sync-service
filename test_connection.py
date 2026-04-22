"""
Teste de conexão com o PostgreSQL.

Uso:
    python test_connection.py
"""

from __future__ import annotations

import os
import sys

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
DB_SERVER   = os.getenv("DB_SERVER", "localhost")
DB_DATABASE = os.getenv("DB_DATABASE", "postgres")
DB_USERNAME = os.getenv("DB_USERNAME") or os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")


def get_dsn() -> str:
    if DATABASE_URL:
        return DATABASE_URL
    dsn = f"host={DB_SERVER} port=5432 dbname={DB_DATABASE}"
    if DB_USERNAME:
        dsn += f" user={DB_USERNAME}"
    if DB_PASSWORD:
        dsn += f" password={DB_PASSWORD}"
    return dsn


def test_connection() -> bool:
    dsn = get_dsn()
    masked = dsn.replace(DB_PASSWORD, "***") if DB_PASSWORD else dsn
    print(f"Conectando em: {masked}")

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()

        cur.execute("SELECT current_database(), version(), current_user")
        db, version, user = cur.fetchone()
        print(f"\n  Banco:   {db}")
        print(f"  Usuário: {user}")
        print(f"  Versão:  {version}\n")

        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'cnpj'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        if tables:
            print(f"  Tabelas no schema 'cnpj' ({len(tables)}):")
            for t in tables:
                cur.execute(f"SELECT COUNT(*) FROM cnpj.{t}")
                count = cur.fetchone()[0]
                print(f"    cnpj.{t:<35} {count:>15,} registros")
        else:
            print("  Schema 'cnpj' ainda não possui tabelas (execute schema.sql primeiro).")

        conn.close()
        print("\n  Conexão OK.")
        return True

    except psycopg2.OperationalError as e:
        print(f"\n  ERRO de conexão: {e}")
        return False
    except Exception as e:
        print(f"\n  ERRO inesperado: {e}")
        return False


if __name__ == "__main__":
    ok = test_connection()
    sys.exit(0 if ok else 1)
