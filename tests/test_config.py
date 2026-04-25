"""
Testes unitários para src/config.py.
Não requerem conexão com banco de dados.
"""

from __future__ import annotations

from src.config import (
    DATE_COLUMNS,
    DECIMAL_COLUMNS,
    DOWNLOADS_DIR,
    EXTRACTED_DIR,
    LOGS_DIR,
    PARTITIONED_GROUPS,
    PROCESSED_DIR,
    REFERENCE_FILES,
    SCHEMAS,
)

GRUPOS_PRINCIPAIS = {"Empresas", "Estabelecimentos", "Socios", "Simples"}
GRUPOS_REFERENCIA = {"Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes"}


def test_schemas_contem_todos_os_grupos():
    esperados = GRUPOS_PRINCIPAIS | GRUPOS_REFERENCIA
    ausentes = esperados - set(SCHEMAS)
    assert not ausentes, f"Grupos ausentes em SCHEMAS: {ausentes}"


def test_estabelecimentos_tem_partes_do_cnpj():
    cols = SCHEMAS["Estabelecimentos"]
    assert "cnpj_basico" in cols
    assert "cnpj_ordem" in cols
    assert "cnpj_dv" in cols


def test_estabelecimentos_tem_30_colunas():
    assert len(SCHEMAS["Estabelecimentos"]) == 30


def test_empresas_tem_7_colunas():
    assert len(SCHEMAS["Empresas"]) == 7


def test_date_columns_existem_nos_schemas():
    for grupo, cols in DATE_COLUMNS.items():
        assert grupo in SCHEMAS, f"Grupo '{grupo}' em DATE_COLUMNS não está em SCHEMAS"
        for col in cols:
            assert col in SCHEMAS[grupo], \
                f"Coluna de data '{col}' não está no schema de '{grupo}'"


def test_decimal_columns_existem_nos_schemas():
    for grupo, cols in DECIMAL_COLUMNS.items():
        assert grupo in SCHEMAS, f"Grupo '{grupo}' em DECIMAL_COLUMNS não está em SCHEMAS"
        for col in cols:
            assert col in SCHEMAS[grupo], \
                f"Coluna decimal '{col}' não está no schema de '{grupo}'"


def test_grupos_particionados():
    assert set(PARTITIONED_GROUPS) == {"Empresas", "Estabelecimentos", "Socios"}


def test_grupos_referencia():
    assert set(REFERENCE_FILES) == GRUPOS_REFERENCIA


def test_diretorios_existem():
    for d in (DOWNLOADS_DIR, EXTRACTED_DIR, PROCESSED_DIR, LOGS_DIR):
        assert d.exists(), f"Diretório não criado pelo config: {d}"


def test_referencias_tem_apenas_codigo_descricao():
    for grupo in GRUPOS_REFERENCIA:
        assert SCHEMAS[grupo] == ["codigo", "descricao"], \
            f"Schema inesperado para tabela de referência '{grupo}': {SCHEMAS[grupo]}"
