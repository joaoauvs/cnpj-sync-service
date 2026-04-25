"""
Testes unitários para src/crawler.py — funções puras, sem rede.
"""

from __future__ import annotations

import pytest

from src.crawler import (
    _group_and_partition,
    _parse_date,
    _parse_size,
    _parse_snapshot_listing_html,
    _select_snapshot_dir,
)


# ---------------------------------------------------------------------------
# _group_and_partition
# ---------------------------------------------------------------------------

class TestGroupAndPartition:
    def test_empresas_com_numero(self):
        assert _group_and_partition("Empresas3.zip") == ("Empresas", 3)

    def test_empresas_zero(self):
        assert _group_and_partition("Empresas0.zip") == ("Empresas", 0)

    def test_estabelecimentos_com_numero(self):
        assert _group_and_partition("Estabelecimentos5.zip") == ("Estabelecimentos", 5)

    def test_socios_com_numero(self):
        assert _group_and_partition("Socios2.zip") == ("Socios", 2)

    def test_simples_sem_numero(self):
        assert _group_and_partition("Simples.zip") == ("Simples", None)

    def test_cnaes_sem_numero(self):
        assert _group_and_partition("Cnaes.zip") == ("Cnaes", None)

    def test_municipios_sem_numero(self):
        assert _group_and_partition("Municipios.zip") == ("Municipios", None)

    def test_particionado_dois_digitos(self):
        assert _group_and_partition("Empresas10.zip") == ("Empresas", 10)

    def test_desconhecido_sem_numero(self):
        group, part = _group_and_partition("Qualificacoes.zip")
        assert group == "Qualificacoes"
        assert part is None


# ---------------------------------------------------------------------------
# _parse_size
# ---------------------------------------------------------------------------

class TestParseSize:
    def test_megabytes(self):
        assert _parse_size("486M") == 486 * 1_024**2

    def test_gigabytes(self):
        assert _parse_size("2G") == 2 * 1_024**3

    def test_kilobytes(self):
        assert _parse_size("512K") == 512 * 1_024

    def test_bytes_sem_sufixo(self):
        assert _parse_size("1024") == 1024

    def test_float_megabytes(self):
        result = _parse_size("1.5M")
        assert abs(result - int(1.5 * 1_024**2)) <= 1

    def test_vazio_retorna_none(self):
        assert _parse_size("") is None

    def test_traco_retorna_none(self):
        assert _parse_size("-") is None

    def test_invalido_retorna_none(self):
        assert _parse_size("abc") is None

    def test_case_insensitive(self):
        assert _parse_size("100m") == 100 * 1_024**2


# ---------------------------------------------------------------------------
# _parse_date
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_formato_rfc(self):
        result = _parse_date("Thu, 01 Jan 2026 00:00:00 GMT")
        assert result is not None
        assert result.year == 2026
        assert result.month == 1
        assert result.day == 1

    def test_formato_yyyy_mm_dd_hhmm(self):
        result = _parse_date("2026-04-15 10:30")
        assert result is not None
        assert result.year == 2026
        assert result.month == 4
        assert result.day == 15

    def test_formato_iso_sem_segundos(self):
        result = _parse_date("2026-04-15T10:30")
        assert result is not None
        assert result.hour == 10

    def test_invalido_retorna_none(self):
        assert _parse_date("not-a-date") is None

    def test_vazio_retorna_none(self):
        assert _parse_date("") is None


# ---------------------------------------------------------------------------
# _select_snapshot_dir
# ---------------------------------------------------------------------------

class TestSelectSnapshotDir:
    DIRS = ["2025-10", "2025-11", "2025-12", "2026-01", "2026-04"]

    def test_sem_requested_retorna_mais_recente(self):
        assert _select_snapshot_dir(self.DIRS) == "2026-04"

    def test_requested_exato_yyyy_mm(self):
        assert _select_snapshot_dir(self.DIRS, "2025-11") == "2025-11"

    def test_requested_yyyy_mm_dd_converte_para_yyyy_mm(self):
        assert _select_snapshot_dir(self.DIRS, "2025-12-15") == "2025-12"

    def test_requested_inexistente_levanta_runtime(self):
        with pytest.raises(RuntimeError, match="não encontrado"):
            _select_snapshot_dir(self.DIRS, "2024-01")

    def test_lista_vazia_levanta_runtime(self):
        with pytest.raises(RuntimeError):
            _select_snapshot_dir([])

    def test_dirs_desordenados_retorna_maior(self):
        dirs = ["2026-01", "2025-06", "2026-03"]
        assert _select_snapshot_dir(dirs) == "2026-03"

    def test_daily_dirs_fallback(self):
        dirs = ["2026-04-01", "2026-04-15", "2026-03-01"]
        assert _select_snapshot_dir(dirs, "2026-04") == "2026-04-15"


# ---------------------------------------------------------------------------
# _parse_snapshot_listing_html — estratégia tabela Apache
# ---------------------------------------------------------------------------

class TestParseSnapshotListingHtml:
    APACHE_HTML = """
    <html><body>
    <table>
      <tr><th>Name</th><th>Name</th><th>Modified</th><th>Size</th></tr>
      <tr>
        <td></td>
        <td><a href="Empresas0.zip">Empresas0.zip</a></td>
        <td>2026-04-01 10:00</td>
        <td>486M</td>
      </tr>
      <tr>
        <td></td>
        <td><a href="Simples.zip">Simples.zip</a></td>
        <td>2026-04-01 10:00</td>
        <td>120M</td>
      </tr>
    </table>
    </body></html>
    """

    def test_extrai_arquivos_zip(self):
        files = _parse_snapshot_listing_html(self.APACHE_HTML, "http://example.com/2026-04/")
        nomes = [f.name for f in files]
        assert "Empresas0.zip" in nomes
        assert "Simples.zip" in nomes

    def test_url_construida_corretamente(self):
        files = _parse_snapshot_listing_html(self.APACHE_HTML, "http://example.com/2026-04/")
        emp = next(f for f in files if f.name == "Empresas0.zip")
        assert emp.url == "http://example.com/2026-04/Empresas0.zip"

    def test_grupo_e_particao_extraidos(self):
        files = _parse_snapshot_listing_html(self.APACHE_HTML, "http://example.com/2026-04/")
        emp = next(f for f in files if f.name == "Empresas0.zip")
        assert emp.group == "Empresas"
        assert emp.partition == 0

    def test_tamanho_parseado(self):
        files = _parse_snapshot_listing_html(self.APACHE_HTML, "http://example.com/2026-04/")
        emp = next(f for f in files if f.name == "Empresas0.zip")
        assert emp.size_bytes == 486 * 1_024**2

    def test_html_sem_zips_retorna_lista_vazia(self):
        html = "<html><body><p>sem arquivos</p></body></html>"
        assert _parse_snapshot_listing_html(html, "http://x.com/") == []
