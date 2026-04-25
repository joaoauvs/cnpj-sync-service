"""
Testes unitários para src/downloader.py — sem rede.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from src.downloader import _already_complete, _valid_local_zip


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
