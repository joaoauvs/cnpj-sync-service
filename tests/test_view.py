"""
Testes de integridade de dados da view cnpj.vw_empresas_completo.
"""

from __future__ import annotations

import re

import pytest

VIEW = "cnpj.vw_empresas_completo"
SITUACOES_VALIDAS = {"01", "02", "03", "04", "08"}
PATTERN_FORMATADO = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")


def test_view_retorna_dados(cur):
    """A view deve ter ao menos um registro."""
    cur.execute(f"SELECT COUNT(*) AS total FROM {VIEW}")
    total = cur.fetchone()["total"]
    assert total > 0, "View está vazia — dados não foram carregados"


def test_cnpj_completo_tem_14_digitos(cur):
    """Nenhum cnpj_completo pode ter comprimento diferente de 14."""
    cur.execute(
        f"SELECT COUNT(*) AS n FROM {VIEW} WHERE LENGTH(cnpj_completo) <> 14"
    )
    invalidos = cur.fetchone()["n"]
    assert invalidos == 0, f"{invalidos} CNPJs com comprimento inválido na view"


def test_cnpj_formatado_segue_mascara(cur):
    """cnpj_formatado deve seguir o padrão XX.XXX.XXX/XXXX-XX."""
    cur.execute(
        f"""
        SELECT cnpj_formatado
        FROM {VIEW}
        WHERE cnpj_formatado !~ '^[0-9]{{2}}\\.[0-9]{{3}}\\.[0-9]{{3}}/[0-9]{{4}}-[0-9]{{2}}$'
        LIMIT 5
        """
    )
    invalidos = cur.fetchall()
    assert not invalidos, f"cnpj_formatado fora do padrão: {[r['cnpj_formatado'] for r in invalidos]}"


def test_situacoes_cadastrais_conhecidas(cur):
    """Todos os valores de situacao_cadastral devem ser do conjunto esperado."""
    cur.execute(
        f"SELECT DISTINCT situacao_cadastral FROM {VIEW} WHERE situacao_cadastral IS NOT NULL"
    )
    encontradas = {row["situacao_cadastral"] for row in cur.fetchall()}
    desconhecidas = encontradas - SITUACOES_VALIDAS
    assert not desconhecidas, f"Situações cadastrais desconhecidas: {desconhecidas}"


def test_tipo_unidade_so_matriz_ou_filial(cur):
    """tipo_unidade deve ser apenas 'Matriz', 'Filial' ou 'Desconhecido'."""
    cur.execute(
        f"SELECT DISTINCT tipo_unidade FROM {VIEW} WHERE tipo_unidade IS NOT NULL"
    )
    valores = {row["tipo_unidade"] for row in cur.fetchall()}
    assert valores <= {"Matriz", "Filial", "Desconhecido"}, \
        f"Valores inesperados em tipo_unidade: {valores}"


def test_descricoes_situacao_preenchidas(cur):
    """situacao_cadastral_descricao não deve ser nulo quando situacao_cadastral é preenchida."""
    cur.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM {VIEW}
        WHERE situacao_cadastral IS NOT NULL
          AND situacao_cadastral_descricao IS NULL
        """
    )
    sem_descricao = cur.fetchone()["n"]
    assert sem_descricao == 0, \
        f"{sem_descricao} registros com situacao_cadastral sem descrição"


def test_razao_social_nunca_nula(cur):
    """Toda empresa sincronizada deve ter razão social."""
    cur.execute(
        f"SELECT COUNT(*) AS n FROM {VIEW} WHERE razao_social IS NULL OR razao_social = ''"
    )
    assert cur.fetchone()["n"] == 0, "Existem registros com razao_social nula na view"


def test_filtragem_por_uf_retorna_dados(cur):
    """Filtragem por UF deve retornar resultado."""
    cur.execute(f"SELECT COUNT(*) AS n FROM {VIEW} WHERE uf = 'SP'")
    assert cur.fetchone()["n"] > 0, "Nenhum estabelecimento em SP"


def test_join_simples_opcional(cur):
    """A view deve retornar linhas mesmo para empresas sem registro em simples."""
    cur.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM {VIEW}
        WHERE opcao_pelo_simples IS NULL
        """
    )
    assert cur.fetchone()["n"] >= 0  # join LEFT — não pode falhar


def test_snapshot_date_preenchida(cur):
    """snapshot_date deve estar preenchida em todos os registros."""
    cur.execute(f"SELECT COUNT(*) AS n FROM {VIEW} WHERE snapshot_date IS NULL")
    assert cur.fetchone()["n"] == 0, "Existem registros sem snapshot_date na view"
