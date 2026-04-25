"""
Fixtures compartilhados entre todos os testes.

A fixture `db_conn` tenta conectar ao PostgreSQL via variáveis de ambiente.
Se a conexão falhar, todos os testes que dependem dela são automaticamente
pulados com a mensagem de erro.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _get_dsn() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    return (
        f"host={os.getenv('DB_SERVER', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_DATABASE', 'postgres')} "
        f"user={os.getenv('DB_USERNAME') or os.getenv('POSTGRES_USER', '')} "
        f"password={os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD', '')}"
    )


@pytest.fixture(scope="session")
def db_conn():
    """Conexão PostgreSQL reutilizada por toda a sessão de testes."""
    try:
        conn = psycopg2.connect(
            _get_dsn(),
            cursor_factory=psycopg2.extras.RealDictCursor,
            connect_timeout=10,
        )
        yield conn
        conn.close()
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Banco indisponível — verifique .env: {exc}")


@pytest.fixture
def cur(db_conn):
    """Cursor isolado por teste (não faz commit)."""
    with db_conn.cursor() as c:
        yield c
