"""
Testes de conectividade com o banco de dados.
"""

from __future__ import annotations

import psycopg2

TABELAS_ESPERADAS = {
    "empresas",
    "estabelecimentos",
    "socios",
    "simples",
    "cnaes",
    "motivos",
    "municipios",
    "naturezas",
    "paises",
    "qualificacoes",
    "controle_sincronizacao",
    "controle_arquivos",
}


def test_conecta_no_banco(db_conn):
    """Verifica que a conexão está aberta e operacional."""
    assert db_conn.closed == 0


def test_versao_postgresql(cur):
    """Exige PostgreSQL 14 ou superior."""
    cur.execute("SELECT current_setting('server_version_num')::int AS v")
    version_num = cur.fetchone()["v"]
    assert version_num >= 140000, f"PostgreSQL < 14 detectado: {version_num}"


def test_schema_cnpj_existe(cur):
    """Schema 'cnpj' deve existir no banco."""
    cur.execute(
        "SELECT COUNT(*) AS n FROM information_schema.schemata WHERE schema_name = 'cnpj'"
    )
    assert cur.fetchone()["n"] == 1, "Schema 'cnpj' não encontrado"


def test_tabelas_esperadas_existem(cur):
    """Todas as tabelas do schema.sql devem estar presentes."""
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'cnpj'
          AND table_type  = 'BASE TABLE'
        """
    )
    existentes = {row["table_name"] for row in cur.fetchall()}
    ausentes = TABELAS_ESPERADAS - existentes
    assert not ausentes, f"Tabelas ausentes no schema 'cnpj': {sorted(ausentes)}"


def test_banco_e_usuario_retornam(cur):
    """SELECT básico deve retornar banco e usuário sem erro."""
    cur.execute("SELECT current_database() AS db, current_user AS u")
    row = cur.fetchone()
    assert row["db"]
    assert row["u"]
