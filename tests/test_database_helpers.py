"""
Testes unitários para helpers de database.py — sem conexão com banco.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from src.database import CNPJDatabase, _NULL_STRS


# ---------------------------------------------------------------------------
# _NULL_STRS membership
# ---------------------------------------------------------------------------

class TestNullStrs:
    @pytest.mark.parametrize("val", ["nan", "NaN", "NAN", "None", "NONE", "none",
                                      "NULL", "null", "Null", "<NA>", "na", "NA",
                                      "N/A", "n/a"])
    def test_strings_nulas_conhecidas(self, val):
        assert val in _NULL_STRS

    def test_valor_real_nao_esta_no_set(self):
        assert "Empresa SA" not in _NULL_STRS
        assert "12345678" not in _NULL_STRS


# ---------------------------------------------------------------------------
# _df_to_copy_buffer
# ---------------------------------------------------------------------------

class TestDfToCopyBuffer:
    def _parse_buf(self, buf) -> list[list[str]]:
        buf.seek(0)
        return [line.split("\t") for line in buf.read().splitlines()]

    def test_linha_simples(self):
        df = pd.DataFrame({"a": ["hello"], "b": ["world"]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert rows == [["hello", "world"]]

    def test_none_vira_backslash_n(self):
        df = pd.DataFrame({"a": [None]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert rows[0][0] == "\\N"

    def test_float_nan_vira_backslash_n(self):
        df = pd.DataFrame({"a": [float("nan")]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert rows[0][0] == "\\N"

    @pytest.mark.parametrize("null_str", ["nan", "NaN", "NULL", "None", "<NA>", ""])
    def test_strings_nulas_viram_backslash_n(self, null_str):
        df = pd.DataFrame({"a": [null_str]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert rows[0][0] == "\\N"

    def test_null_byte_removido(self):
        df = pd.DataFrame({"a": ["abc\x00def"]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert "\x00" not in rows[0][0]
        assert rows[0][0] == "abcdef"

    def test_tab_substituido_por_espaco(self):
        df = pd.DataFrame({"a": ["col1\tcol2"]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert "\t" not in rows[0][0]
        assert rows[0][0] == "col1 col2"

    def test_newline_substituido_por_espaco(self):
        df = pd.DataFrame({"a": ["linha1\nlinha2"]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert rows[0][0] == "linha1 linha2"

    def test_barra_invertida_escapada(self):
        df = pd.DataFrame({"a": ["c:\\path"]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert rows[0][0] == "c:\\\\path"

    def test_inteiro_convertido_para_string(self):
        df = pd.DataFrame({"a": [42]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert rows[0][0] == "42"

    def test_multiplas_linhas(self):
        df = pd.DataFrame({"a": ["x", "y", "z"], "b": ["1", "2", "3"]})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        rows = self._parse_buf(buf)
        assert len(rows) == 3
        assert rows[1] == ["y", "2"]

    def test_dataframe_vazio_buffer_vazio(self):
        df = pd.DataFrame({"a": pd.Series([], dtype=str)})
        buf = CNPJDatabase._df_to_copy_buffer(df)
        buf.seek(0)
        assert buf.read() == ""
