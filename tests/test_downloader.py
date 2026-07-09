"""
Testes unitários para src/downloader.py — sem rede.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from src.downloader import FileDownloader, _already_complete, _valid_local_zip
from src.models import FileStatus, RemoteFile


# ---------------------------------------------------------------------------
# _already_complete
# ---------------------------------------------------------------------------

class TestAlreadyComplete:
    def test_arquivo_inexistente_retorna_false(self, tmp_path):
        assert _already_complete(tmp_path / "nao_existe.zip", 1000) is False

    def test_tamanho_exato(self, tmp_path):
        f = tmp_path / "file.zip"
        f.write_bytes(b"x" * 1000)
        assert _already_complete(f, 1000) is True

    def test_dentro_tolerancia_5pct(self, tmp_path):
        f = tmp_path / "file.zip"
        f.write_bytes(b"x" * 1000)
        assert _already_complete(f, 1040) is True  # 4% menor

    def test_fora_tolerancia_retorna_false(self, tmp_path):
        f = tmp_path / "file.zip"
        f.write_bytes(b"x" * 500)
        assert _already_complete(f, 1000) is False  # 50% menor

    def test_expected_none_retorna_false(self, tmp_path):
        f = tmp_path / "file.zip"
        f.write_bytes(b"x" * 100)
        assert _already_complete(f, None) is False


# ---------------------------------------------------------------------------
# _valid_local_zip
# ---------------------------------------------------------------------------

class TestValidLocalZip:
    def test_zip_valido(self, tmp_path):
        z = tmp_path / "valid.zip"
        with zipfile.ZipFile(z, "w") as zf:
            zf.writestr("data.csv", "cnpj;nome\n12345678;Empresa")
        assert _valid_local_zip(z) is True

    def test_arquivo_inexistente(self, tmp_path):
        assert _valid_local_zip(tmp_path / "nao_existe.zip") is False

    def test_arquivo_vazio(self, tmp_path):
        f = tmp_path / "empty.zip"
        f.write_bytes(b"")
        assert _valid_local_zip(f) is False

    def test_arquivo_corrompido(self, tmp_path):
        f = tmp_path / "corrupt.zip"
        f.write_bytes(b"isso nao e um zip valido")
        assert _valid_local_zip(f) is False

    def test_zip_sem_entradas_retorna_false(self, tmp_path):
        z = tmp_path / "empty_zip.zip"
        with zipfile.ZipFile(z, "w"):
            pass  # ZIP vazio, sem membros
        assert _valid_local_zip(z) is False


# ---------------------------------------------------------------------------
# FileDownloader.download_file — validação pós-download
# ---------------------------------------------------------------------------

def _remote_file(name: str = "Empresas0.zip", size: int | None = None) -> RemoteFile:
    return RemoteFile(
        name=name,
        url=f"https://example.com/{name}",
        group="Empresas",
        partition=0,
        size_bytes=size,
        last_modified=None,
    )


def _zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", "12345678;Empresa\n")
    return buf.getvalue()


class TestDownloadFileValidation:
    def test_download_corrompido_falha_e_descarta_arquivo(self, tmp_path, monkeypatch):
        """Um download que resulta em ZIP inválido deve falhar e remover o arquivo."""
        rf = _remote_file(size=1000)
        dest = tmp_path / rf.name

        def fake_download(remote_file, session, dest_path):
            dest_path.write_bytes(b"conteudo truncado, nao e zip")
            return 28

        monkeypatch.setattr("src.downloader._download_file", fake_download)
        result = FileDownloader(dest_dir=tmp_path).download_file(rf)

        assert result.status == FileStatus.FAILED
        assert "ZIP inválido" in (result.error or "")
        assert not dest.exists()  # descartado para rebaixar na próxima execução

    def test_download_valido_reporta_tamanho_total(self, tmp_path, monkeypatch):
        """bytes_downloaded deve refletir o tamanho total do arquivo em disco."""
        rf = _remote_file()
        payload = _zip_bytes()

        def fake_download(remote_file, session, dest_path):
            dest_path.write_bytes(payload)
            return 10  # simula retomada: só parte foi baixada nesta chamada

        monkeypatch.setattr("src.downloader._download_file", fake_download)
        result = FileDownloader(dest_dir=tmp_path).download_file(rf)

        assert result.status == FileStatus.DOWNLOADED
        assert result.bytes_downloaded == len(payload)

    def test_zip_local_valido_e_pulado_sem_rede(self, tmp_path, monkeypatch):
        rf = _remote_file()
        (tmp_path / rf.name).write_bytes(_zip_bytes())

        def explode(*args, **kwargs):
            raise AssertionError("não deveria baixar nada")

        monkeypatch.setattr("src.downloader._download_file", explode)
        result = FileDownloader(dest_dir=tmp_path).download_file(rf)

        assert result.status == FileStatus.SKIPPED
