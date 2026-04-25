"""
Testes unitários para src/storage.py — CSV e Parquet writers.
"""

from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.storage import CSVWriter, ParquetWriter, get_writer, output_extension


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "cnpj_basico": ["12345678", "87654321"],
        "razao_social": ["Empresa A", "Empresa B"],
        "capital_social": [1000.0, 2000.0],
    })


# ---------------------------------------------------------------------------
# output_extension
# ---------------------------------------------------------------------------

def test_extensao_csv():
    assert output_extension("csv") == ".csv"

def test_extensao_parquet():
    assert output_extension("parquet") == ".parquet"

def test_extensao_invalida_levanta():
    with pytest.raises(ValueError):
        output_extension("json")


# ---------------------------------------------------------------------------
# get_writer factory
# ---------------------------------------------------------------------------

def test_get_writer_csv(tmp_path):
    w = get_writer("csv", tmp_path / "out.csv", "Empresas")
    assert isinstance(w, CSVWriter)

def test_get_writer_parquet(tmp_path):
    w = get_writer("parquet", tmp_path / "out.parquet", "Empresas")
    assert isinstance(w, ParquetWriter)

def test_get_writer_invalido(tmp_path):
    with pytest.raises(ValueError):
        get_writer("redis", tmp_path / "out.x", "Empresas")


# ---------------------------------------------------------------------------
# CSVWriter
# ---------------------------------------------------------------------------

class TestCSVWriter:
    def test_escreve_arquivo_csv(self, tmp_path):
        path = tmp_path / "out.csv"
        df = _sample_df()
        with CSVWriter(path, "Empresas") as w:
            w.write(df)
        assert path.exists()
        result = pd.read_csv(path)
        assert list(result.columns) == list(df.columns)
        assert len(result) == 2

    def test_rows_written_correto(self, tmp_path):
        path = tmp_path / "out.csv"
        df = _sample_df()
        with CSVWriter(path, "Empresas") as w:
            w.write(df)
            w.write(df)
        assert w.rows_written == 4

    def test_multiplos_chunks_header_unico(self, tmp_path):
        path = tmp_path / "out.csv"
        df = _sample_df()
        with CSVWriter(path, "Empresas") as w:
            w.write(df)
            w.write(df)
        result = pd.read_csv(path)
        assert len(result) == 4
        assert list(result.columns) == ["cnpj_basico", "razao_social", "capital_social"]

    def test_sobrescreve_arquivo_existente(self, tmp_path):
        path = tmp_path / "out.csv"
        path.write_text("conteudo antigo")
        df = _sample_df()
        with CSVWriter(path, "Empresas") as w:
            w.write(df)
        result = pd.read_csv(path)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# ParquetWriter
# ---------------------------------------------------------------------------

class TestParquetWriter:
    def test_escreve_arquivo_parquet(self, tmp_path):
        path = tmp_path / "out.parquet"
        df = _sample_df()
        with ParquetWriter(path, "Empresas") as w:
            w.write(df)
        assert path.exists()
        result = pq.read_table(path).to_pandas()
        assert len(result) == 2

    def test_rows_written_correto(self, tmp_path):
        path = tmp_path / "out.parquet"
        df = _sample_df()
        with ParquetWriter(path, "Empresas") as w:
            w.write(df)
            w.write(df)
        assert w.rows_written == 4

    def test_multiplos_chunks_concatenados(self, tmp_path):
        path = tmp_path / "out.parquet"
        df = _sample_df()
        with ParquetWriter(path, "Empresas") as w:
            w.write(df)
            w.write(df)
        result = pq.read_table(path).to_pandas()
        assert len(result) == 4

    def test_sobrescreve_arquivo_existente(self, tmp_path):
        path = tmp_path / "out.parquet"
        df1 = _sample_df()
        with ParquetWriter(path, "Empresas") as w:
            w.write(df1)
        df2 = pd.DataFrame({"cnpj_basico": ["11111111"]})
        with ParquetWriter(path, "Empresas") as w:
            w.write(df2)
        result = pq.read_table(path).to_pandas()
        assert len(result) == 1
