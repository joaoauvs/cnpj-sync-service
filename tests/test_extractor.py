"""
Testes unitários para src/extractor.py — usa tmp_path, sem rede.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from src.extractor import ZipExtractor
from src.models import DownloadResult, FileStatus, RemoteFile


def _make_remote_file(name: str = "Empresas0.zip") -> RemoteFile:
    return RemoteFile(
        name=name,
        url=f"http://example.com/{name}",
        group="Empresas",
        partition=0,
        size_bytes=None,
        last_modified=None,
    )


def _make_download_result(local_path: Path, status: FileStatus = FileStatus.DOWNLOADED) -> DownloadResult:
    return DownloadResult(
        remote_file=_make_remote_file(local_path.name),
        local_path=local_path,
        status=status,
    )


def _make_zip(path: Path, content: str = "cnpj;nome\n12345678;Empresa") -> None:
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("data.csv", content)


# ---------------------------------------------------------------------------
# ZipExtractor.extract_zip
# ---------------------------------------------------------------------------

class TestExtractZip:
    def test_extracao_bem_sucedida(self, tmp_path):
        zip_path = tmp_path / "Empresas0.zip"
        _make_zip(zip_path)
        dr = _make_download_result(zip_path)
        extractor = ZipExtractor(dest_dir=tmp_path / "extracted")

        result = extractor.extract_zip(dr)

        assert result.status == FileStatus.EXTRACTED
        assert len(result.extracted_paths) == 1
        assert result.extracted_paths[0].exists()

    def test_download_falho_pula_extracao(self, tmp_path):
        zip_path = tmp_path / "Empresas0.zip"
        dr = _make_download_result(zip_path, status=FileStatus.FAILED)
        extractor = ZipExtractor(dest_dir=tmp_path / "extracted")

        result = extractor.extract_zip(dr)

        assert result.status == FileStatus.SKIPPED
        assert "download failed" in (result.error or "").lower()

    def test_zip_inexistente_retorna_failed(self, tmp_path):
        zip_path = tmp_path / "inexistente.zip"
        dr = _make_download_result(zip_path)
        extractor = ZipExtractor(dest_dir=tmp_path / "extracted")

        result = extractor.extract_zip(dr)

        assert result.status == FileStatus.FAILED
        assert "not found" in (result.error or "").lower()

    def test_zip_corrompido_retorna_failed(self, tmp_path):
        zip_path = tmp_path / "corrupt.zip"
        zip_path.write_bytes(b"dados corrompidos")
        dr = _make_download_result(zip_path)
        extractor = ZipExtractor(dest_dir=tmp_path / "extracted")

        result = extractor.extract_zip(dr)

        assert result.status == FileStatus.FAILED

    def test_skip_quando_ja_extraido(self, tmp_path):
        zip_path = tmp_path / "Empresas0.zip"
        dest = tmp_path / "extracted"
        dest.mkdir()
        _make_zip(zip_path)

        # Pre-extract
        extractor = ZipExtractor(dest_dir=dest)
        extractor.extract_zip(_make_download_result(zip_path))

        # Extract again without overwrite
        result = extractor.extract_zip(_make_download_result(zip_path), overwrite=False)

        assert result.status == FileStatus.EXTRACTED
        assert len(result.extracted_paths) == 1

    def test_overwrite_reextrai(self, tmp_path):
        zip_path = tmp_path / "Empresas0.zip"
        dest = tmp_path / "extracted"
        _make_zip(zip_path)
        extractor = ZipExtractor(dest_dir=dest)
        extractor.extract_zip(_make_download_result(zip_path))

        result = extractor.extract_zip(_make_download_result(zip_path), overwrite=True)

        assert result.status == FileStatus.EXTRACTED

    def test_duracao_preenchida(self, tmp_path):
        zip_path = tmp_path / "Empresas0.zip"
        _make_zip(zip_path)
        dr = _make_download_result(zip_path)
        extractor = ZipExtractor(dest_dir=tmp_path / "extracted")

        result = extractor.extract_zip(dr)

        assert result.duration_seconds >= 0


# ---------------------------------------------------------------------------
# ZipExtractor.extract_all
# ---------------------------------------------------------------------------

class TestExtractAll:
    def test_lista_vazia_retorna_vazia(self, tmp_path):
        extractor = ZipExtractor(dest_dir=tmp_path)
        assert extractor.extract_all([]) == []

    def test_extrai_multiplos_arquivos(self, tmp_path):
        dest = tmp_path / "extracted"
        results_dr = []
        for i in range(3):
            z = tmp_path / f"Empresas{i}.zip"
            _make_zip(z, f"linha{i}")
            results_dr.append(_make_download_result(z))
            results_dr[-1].remote_file.name  # just access for sanity

        extractor = ZipExtractor(dest_dir=dest, workers=2)
        results = extractor.extract_all(results_dr)

        assert len(results) == 3
        assert all(r.status == FileStatus.EXTRACTED for r in results)
