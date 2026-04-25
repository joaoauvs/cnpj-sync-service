"""
Testes de estrutura do schema: colunas das tabelas e existência da view.
"""

from __future__ import annotations

import pytest


def _colunas(cur, tabela: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'cnpj'
          AND table_name   = %s
        """,
        (tabela,),
    )
    return {row["column_name"] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Tabelas principais
# ---------------------------------------------------------------------------

def test_empresas_colunas_obrigatorias(cur):
    cols = _colunas(cur, "empresas")
    esperadas = {
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social",
        "porte_empresa", "ente_federativo_responsavel",
        "snapshot_date", "data_carga",
    }
    assert esperadas <= cols, f"Colunas ausentes em 'empresas': {esperadas - cols}"


def test_estabelecimentos_colunas_cnpj(cur):
    cols = _colunas(cur, "estabelecimentos")
    assert {"cnpj_completo", "cnpj_basico", "cnpj_ordem", "cnpj_dv"} <= cols


def test_estabelecimentos_colunas_endereco(cur):
    cols = _colunas(cur, "estabelecimentos")
    esperadas = {"tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio"}
    assert esperadas <= cols, f"Colunas de endereço ausentes: {esperadas - cols}"


def test_estabelecimentos_colunas_contato(cur):
    cols = _colunas(cur, "estabelecimentos")
    assert {"ddd_1", "telefone_1", "correio_eletronico"} <= cols


def test_socios_colunas_obrigatorias(cur):
    cols = _colunas(cur, "socios")
    esperadas = {
        "id_socio", "cnpj_basico", "identificador_socio",
        "nome_socio_razao_social", "qualificacao_socio",
        "data_entrada_sociedade",
    }
    assert esperadas <= cols, f"Colunas ausentes em 'socios': {esperadas - cols}"


def test_simples_colunas_obrigatorias(cur):
    cols = _colunas(cur, "simples")
    esperadas = {
        "cnpj_basico", "opcao_pelo_simples", "opcao_mei",
        "data_opcao_simples", "data_exclusao_simples",
        "data_opcao_mei", "data_exclusao_mei",
    }
    assert esperadas <= cols, f"Colunas ausentes em 'simples': {esperadas - cols}"


# ---------------------------------------------------------------------------
# Tabelas de referência
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tabela", ["cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"])
def test_tabela_referencia_tem_codigo_descricao(cur, tabela):
    cols = _colunas(cur, tabela)
    assert {"codigo", "descricao", "snapshot_date"} <= cols, \
        f"Estrutura incorreta em tabela de referência '{tabela}': {cols}"


# ---------------------------------------------------------------------------
# Tabelas de controle
# ---------------------------------------------------------------------------

def test_controle_sincronizacao_colunas(cur):
    cols = _colunas(cur, "controle_sincronizacao")
    assert {"id_execucao", "snapshot_date", "status", "total_registros"} <= cols


def test_controle_arquivos_colunas(cur):
    cols = _colunas(cur, "controle_arquivos")
    assert {"id_arquivo", "id_execucao", "grupo_arquivo", "nome_arquivo", "status"} <= cols


# ---------------------------------------------------------------------------
# View
# ---------------------------------------------------------------------------

def test_view_existe(cur):
    cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM information_schema.views
        WHERE table_schema = 'cnpj'
          AND table_name   = 'vw_empresas_completo'
        """
    )
    assert cur.fetchone()["n"] == 1, "View 'cnpj.vw_empresas_completo' não encontrada"


def test_view_colunas_principais(cur):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'cnpj'
          AND table_name   = 'vw_empresas_completo'
        """
    )
    cols = {row["column_name"] for row in cur.fetchall()}
    esperadas = {
        "cnpj_completo", "cnpj_formatado", "razao_social",
        "situacao_cadastral_descricao", "tipo_unidade",
        "porte_empresa_descricao", "natureza_juridica_descricao",
        "cnae_fiscal_descricao", "municipio_descricao",
        "opcao_pelo_simples", "opcao_mei",
    }
    assert esperadas <= cols, f"Colunas ausentes na view: {esperadas - cols}"
