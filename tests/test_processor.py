"""
Testes unitários para src/processor.py — sem rede, usa tmp_path.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
import pytest

from src.models import (
    DownloadResult,
    ExtractionResult,
    FileStatus,
    RemoteFile,
)
from src.processor import CSVProcessor, _detect_group


# ---------------------------------------------------------------------------
# _detect_group
# ---------------------------------------------------------------------------

class TestDetectGroup:
    @pytest.mark.parametrize("filename,expected", [
        ("K3241.K03200Y0.D20316EMPRECSV", "Empresas"),
        ("K3241.K03200Y0.D20316ESTABELCSV", "Estabelecimentos"),
        ("K3241.K03200Y0.D20316SOCIOCSV", "Socios"),
        ("K3241.K03200Y0.D20316SIMPLESCSV", "Simples"),
        ("K3241.K03200Y0.D20316CNAECSV", "Cnaes"),
        ("K3241.K03200Y0.D20316MOTIVCSV", "Motivos"),
        ("K3241.K03200Y0.D20316MUNICCSV", "Municipios"),
        ("K3241.K03200Y0.D20316NATJUCSV", "Naturezas"),
        ("K3241.K03200Y0.D20316PAISCSV", "Paises"),
        ("K3241.K03200Y0.D20316QUALSCSV", "Qualificacoes"),
    ])
    def test_detecta_grupo_por_nome(self, filename, expected, tmp_path):
        csv = tmp_path / filename
        csv.write_text("")
        assert _detect_group(csv) == expected

    def test_desconhecido_retorna_none(self, tmp_path):
        csv = tmp_path / "arquivo_desconhecido.csv"
        csv.write_text("")
        assert _detect_group(csv) is None


# ---------------------------------------------------------------------------
# Helpers para montar ExtractionResult
# ---------------------------------------------------------------------------

def _make_extraction_result(
    csv_path: Path,
    group: str,
    status: FileStatus = FileStatus.EXTRACTED,
) -> ExtractionResult:
    stem = csv_path.stem
    remote = RemoteFile(
        name=f"{stem}.zip",
        url=f"http://example.com/{stem}.zip",
        group=group,
        partition=None,
        size_bytes=None,
        last_modified=None,
    )
    dr = DownloadResult(
        remote_file=remote,
        local_path=csv_path.parent / f"{stem}.zip",
        status=FileStatus.DOWNLOADED,
    )
    return ExtractionResult(
        download_result=dr,
        extracted_paths=[csv_path] if status == FileStatus.EXTRACTED else [],
        status=status,
    )


def _write_csv(path: Path, rows: list[list[str]], sep: str = ";") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="latin-1") as f:
        for row in rows:
            f.write(sep.join(row) + "\n")


# ---------------------------------------------------------------------------
# CSVProcessor.process_csv
# ---------------------------------------------------------------------------

class TestCSVProcessorProcessCsv:
    def test_processa_csv_empresas_csv_backend(self, tmp_path):
        csv = tmp_path / "Empresas0CSV"
        # Empresas: 7 colunas
        _write_csv(csv, [
            ["12345678", "01", "03", "1000,00", "1", "0000-00-00", ""],
            ["87654321", "02", "03", "2000,50", "1", "0000-00-00", ""],
        ])
        er = _make_extraction_result(csv, "Empresas")
        processor = CSVProcessor(output_dir=tmp_path / "out", storage_backend="csv")

        result = processor.process_csv(er, "Empresas", write_rejects=False)

        assert result.status == FileStatus.DONE
        assert result.rows_written == 2
        assert result.output_path is not None
        assert result.output_path.exists()

    def test_processa_csv_referencias_csv_backend(self, tmp_path):
        csv = tmp_path / "CnaesCSV"
        _write_csv(csv, [
            ["0111-3/01", "Cultivo de arroz"],
            ["0111-3/02", "Cultivo de milho"],
        ])
        er = _make_extraction_result(csv, "Cnaes")
        processor = CSVProcessor(output_dir=tmp_path / "out", storage_backend="csv")

        result = processor.process_csv(er, "Cnaes", write_rejects=False)

        assert result.status == FileStatus.DONE
        assert result.rows_written == 2

    def test_processa_csv_parquet_backend(self, tmp_path):
        csv = tmp_path / "Cnaes0CSV"
        _write_csv(csv, [["0111-3/01", "Cultivo de arroz"]])
        er = _make_extraction_result(csv, "Cnaes")
        processor = CSVProcessor(output_dir=tmp_path / "out", storage_backend="parquet")

        result = processor.process_csv(er, "Cnaes", write_rejects=False)

        assert result.status == FileStatus.DONE
        assert result.output_path is not None
        assert result.output_path.suffix == ".parquet"

    def test_extraction_failed_retorna_skipped(self, tmp_path):
        csv = tmp_path / "Empresas0CSV"
        er = _make_extraction_result(csv, "Empresas", status=FileStatus.FAILED)
        processor = CSVProcessor(output_dir=tmp_path / "out", storage_backend="csv")

        result = processor.process_csv(er, "Empresas")

        assert result.status == FileStatus.SKIPPED

    def test_grupo_desconhecido_retorna_failed(self, tmp_path):
        csv = tmp_path / "Unknowns0CSV"
        csv.write_text("a;b\n")
        er = _make_extraction_result(csv, "GrupoInexistente")
        processor = CSVProcessor(output_dir=tmp_path / "out", storage_backend="csv")

        result = processor.process_csv(er, "GrupoInexistente")

        assert result.status == FileStatus.FAILED
        assert "schema" in (result.error or "").lower()

    def test_sem_extracted_paths_retorna_failed(self, tmp_path):
        remote = RemoteFile(
            name="Empresas0.zip",
            url="http://x/Empresas0.zip",
            group="Empresas",
            partition=0,
            size_bytes=None,
            last_modified=None,
        )
        dr = DownloadResult(
            remote_file=remote,
            local_path=tmp_path / "Empresas0.zip",
            status=FileStatus.DOWNLOADED,
        )
        er = ExtractionResult(
            download_result=dr,
            extracted_paths=[],
            status=FileStatus.EXTRACTED,
        )
        processor = CSVProcessor(output_dir=tmp_path / "out", storage_backend="csv")

        result = processor.process_csv(er, "Empresas")

        assert result.status == FileStatus.FAILED

    def test_rejeitos_gravados_quando_cnpj_basico_vazio(self, tmp_path):
        csv = tmp_path / "Empresas0CSV"
        # Linha com cnpj_basico vazio deve ir para rejects
        _write_csv(csv, [
            ["12345678", "01", "03", "1000,00", "1", "", ""],
            ["", "02", "03", "500,00", "1", "", ""],  # rejeitada
        ])
        er = _make_extraction_result(csv, "Empresas")
        out = tmp_path / "out"
        processor = CSVProcessor(output_dir=out, storage_backend="csv")

        result = processor.process_csv(er, "Empresas", write_rejects=True)

        assert result.rows_written == 1
        assert result.rows_invalid == 1
        # reject path usa o stem do CSV (Empresas0CSV)
        reject_path = out / f"{csv.stem}_rejects.csv"
        assert reject_path.exists()

    def test_normalizacao_data_aplicada(self, tmp_path):
        csv = tmp_path / "Estabelecimentos0CSV"
        # Estabelecimentos: 30 colunas
        # data_situacao_cadastral é índice 6 no schema
        row = ["12345678", "0001", "01"] + [""] * 27
        row[6] = "20230115"  # data_situacao_cadastral
        _write_csv(csv, [row])
        er = _make_extraction_result(csv, "Estabelecimentos")
        processor = CSVProcessor(output_dir=tmp_path / "out", storage_backend="csv")

        result = processor.process_csv(er, "Estabelecimentos", write_rejects=False)

        assert result.status == FileStatus.DONE
        out_df = pd.read_csv(result.output_path)
        assert out_df["data_situacao_cadastral"].iloc[0] == "2023-01-15"
