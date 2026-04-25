"""
Testes unitários para DataFrameNormalizer e funções de normalização do processor.
Não requerem conexão com banco de dados.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.sync import DataFrameNormalizer
from src.processor import _normalise_date_column, _normalise_decimal_column


# ---------------------------------------------------------------------------
# DataFrameNormalizer._norm_date
# ---------------------------------------------------------------------------

@pytest.fixture
def normalizer():
    return DataFrameNormalizer()


class TestNormDate:
    def test_formato_yyyymmdd(self, normalizer):
        assert normalizer._norm_date("20230115") == "2023-01-15"

    def test_ja_formato_iso(self, normalizer):
        assert normalizer._norm_date("2023-01-15") == "2023-01-15"

    def test_zeros_retorna_none(self, normalizer):
        assert normalizer._norm_date("00000000") is None

    def test_string_vazia_retorna_none(self, normalizer):
        assert normalizer._norm_date("") is None

    def test_none_retorna_none(self, normalizer):
        assert normalizer._norm_date(None) is None

    def test_nan_retorna_none(self, normalizer):
        assert normalizer._norm_date(float("nan")) is None

    def test_data_invalida_retorna_none(self, normalizer):
        assert normalizer._norm_date("20231345") is None  # mês 13 inválido

    def test_padding_com_menos_de_8_digitos(self, normalizer):
        # zfill(8) → "00230115" → data válida
        assert normalizer._norm_date("230115") == "0023-01-15"


class TestApplyDateCols:
    def test_converte_coluna_de_datas(self, normalizer):
        df = pd.DataFrame({"data_situacao_cadastral": ["20230101", "00000000", None]})
        result = normalizer.apply_date_cols(df, "Estabelecimentos")
        valores = result["data_situacao_cadastral"].tolist()
        assert valores[0] == "2023-01-01"
        assert pd.isna(valores[1])  # 00000000 → None → nan no pandas
        assert pd.isna(valores[2])

    def test_ignora_grupo_sem_datas(self, normalizer):
        df = pd.DataFrame({"cnpj_basico": ["12345678"]})
        result = normalizer.apply_date_cols(df, "Empresas")
        assert list(result.columns) == ["cnpj_basico"]


# ---------------------------------------------------------------------------
# DataFrameNormalizer.apply_decimal_cols
# ---------------------------------------------------------------------------

class TestApplyDecimalCols:
    def test_virgula_brasileira(self, normalizer):
        # RF usa vírgula como decimal sem separador de milhar: "1234,56" → 1234.56
        df = pd.DataFrame({"capital_social": ["1234,56"]})
        result = normalizer.apply_decimal_cols(df, "Empresas")
        assert abs(result["capital_social"].iloc[0] - 1234.56) < 0.001

    def test_valor_inteiro(self, normalizer):
        df = pd.DataFrame({"capital_social": ["1000"]})
        result = normalizer.apply_decimal_cols(df, "Empresas")
        assert result["capital_social"].iloc[0] == 1000.0

    def test_vazio_vira_nan(self, normalizer):
        df = pd.DataFrame({"capital_social": [""]})
        result = normalizer.apply_decimal_cols(df, "Empresas")
        assert pd.isna(result["capital_social"].iloc[0])

    def test_ignora_grupo_sem_decimais(self, normalizer):
        df = pd.DataFrame({"cnpj_basico": ["12345678"]})
        result = normalizer.apply_decimal_cols(df, "Socios")
        assert list(result.columns) == ["cnpj_basico"]


# ---------------------------------------------------------------------------
# _normalise_date_column (processor.py)
# ---------------------------------------------------------------------------

class TestNormaliseDateColumn:
    def test_converte_serie(self):
        s = pd.Series(["20230115", "00000000", "", "20241231"])
        result = _normalise_date_column(s)
        # "00000000" é formatado como "0000-00-00" (a filtragem de zeros ocorre no DataFrameNormalizer)
        assert result.tolist() == ["2023-01-15", "0000-00-00", "", "2024-12-31"]

    def test_valor_zero_simples(self):
        s = pd.Series(["0"])
        result = _normalise_date_column(s)
        assert result.iloc[0] == ""


# ---------------------------------------------------------------------------
# _normalise_decimal_column (processor.py)
# ---------------------------------------------------------------------------

class TestNormaliseDecimalColumn:
    def test_virgula_para_ponto(self):
        # RF usa vírgula como decimal sem separador de milhar
        s = pd.Series(["1234,56"])
        result = _normalise_decimal_column(s)
        assert abs(result.iloc[0] - 1234.56) < 0.001

    def test_vazio_vira_na(self):
        s = pd.Series([""])
        result = _normalise_decimal_column(s)
        assert pd.isna(result.iloc[0])
