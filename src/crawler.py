"""
Crawler: descobre o snapshot mais recente e lista os arquivos disponíveis.

Fonte primária: Receita Federal — WebDAV Nextcloud (Basic Auth com share token)
  - Raiz: https://arquivos.receitafederal.gov.br/public.php/webdav/
  - Pastas: YYYY-MM
  - Download: {WEBDAV_BASE}/{pasta}/{arquivo}.zip

Fonte fallback: casadosdados.com.br — listagem HTML
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import (
    BACKOFF_FACTOR,
    FALLBACK_BASE_URL,
    HEADERS,
    MAX_RETRIES,
    PARTITIONED_GROUPS,
    RF_AUTH,
    RF_WEBDAV_BASE,
    REQUEST_TIMEOUT,
)
from src.logger_enhanced import logger
from src.models import RemoteFile, Snapshot

_DAV_NS = {"d": "DAV:"}
_YYYY_MM = re.compile(r"^\d{4}-\d{2}$")
_YYYY_MM_DD = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SIZE_MAP = {"K": 1_024, "M": 1_024**2, "G": 1_024**3}


# ---------------------------------------------------------------------------
# Helpers compartilhados
# ---------------------------------------------------------------------------

def _group_and_partition(filename: str) -> tuple[str, Optional[int]]:
    """
    Extrai grupo lógico e índice de partição do nome do arquivo.
    'Empresas3.zip' → ('Empresas', 3)
    'Simples.zip'   → ('Simples', None)
    """
    stem = filename.replace(".zip", "")
    for group in PARTITIONED_GROUPS:
        if stem.startswith(group):
            suffix = stem[len(group):]
            if suffix.isdigit():
                return group, int(suffix)
    return stem, None


def _parse_size(raw: str) -> Optional[int]:
    """Converte '486M' → bytes."""
    raw = raw.strip()
    if not raw or raw == "-":
        return None
    m = re.match(r"^([\d.]+)\s*([KMG]?)$", raw, re.I)
    if not m:
        return None
    return int(float(m.group(1)) * _SIZE_MAP.get(m.group(2).upper(), 1))


def _parse_date(raw: str) -> Optional[datetime]:
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Fonte primária: WebDAV Nextcloud
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=60),
    reraise=True,
)
def _propfind(url: str, session: requests.Session) -> ET.Element:
    """Executa PROPFIND e retorna o root XML."""
    resp = session.request(
        "PROPFIND",
        url,
        headers={"Depth": "1", "Content-Type": "application/xml; charset=utf-8"},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return ET.fromstring(resp.text)


def _latest_folder(session: requests.Session) -> str:
    """Detecta automaticamente a pasta YYYY-MM mais recente."""
    root = _propfind(RF_WEBDAV_BASE + "/", session)
    pastas: list[str] = []

    for resp in root.findall("d:response", _DAV_NS):
        href = resp.find("d:href", _DAV_NS).text or ""
        nome = unquote(href.rstrip("/").split("/")[-1])
        if _YYYY_MM.match(nome):
            pastas.append(nome)

    if not pastas:
        raise RuntimeError(f"Nenhuma pasta YYYY-MM encontrada em {RF_WEBDAV_BASE}/")

    latest = sorted(pastas)[-1]
    logger.info("Pastas disponíveis: {} — mais recente: {}", len(pastas), latest)
    return latest


def _list_files_webdav(pasta: str, session: requests.Session) -> list[RemoteFile]:
    """Lista arquivos .zip em uma pasta via WebDAV."""
    url = f"{RF_WEBDAV_BASE}/{pasta}/"
    root = _propfind(url, session)
    files: list[RemoteFile] = []

    for resp in root.findall("d:response", _DAV_NS):
        href = resp.find("d:href", _DAV_NS).text or ""
        nome = unquote(href.rstrip("/").split("/")[-1])

        if not nome.endswith(".zip"):
            continue

        props = resp.find(".//d:prop", _DAV_NS)
        size_bytes: Optional[int] = None
        last_modified: Optional[datetime] = None

        if props is not None:
            size_el = props.find("d:getcontentlength", _DAV_NS)
            if size_el is not None and size_el.text:
                try:
                    size_bytes = int(size_el.text)
                except ValueError:
                    pass
            date_el = props.find("d:getlastmodified", _DAV_NS)
            if date_el is not None and date_el.text:
                last_modified = _parse_date(date_el.text)

        group, partition = _group_and_partition(nome)
        download_url = f"{RF_WEBDAV_BASE}/{pasta}/{nome}"

        files.append(RemoteFile(
            name=nome,
            url=download_url,
            group=group,
            partition=partition,
            size_bytes=size_bytes,
            last_modified=last_modified,
        ))

    return sorted(files, key=lambda f: f.name)


def _discover_webdav(session: requests.Session) -> Snapshot:
    """Descobre o snapshot mais recente via WebDAV."""
    session.auth = RF_AUTH

    pasta = _latest_folder(session)
    files = _list_files_webdav(pasta, session)

    if not files:
        raise RuntimeError(f"Nenhum arquivo .zip encontrado na pasta {pasta}")

    total_gb = sum(f.size_bytes or 0 for f in files) / 1_024**3
    logger.info("Snapshot {}: {} arquivos, ~{:.1f} GB comprimidos", pasta, len(files), total_gb)

    return Snapshot(
        date=pasta,
        url=f"{RF_WEBDAV_BASE}/{pasta}/",
        files=files,
    )


# ---------------------------------------------------------------------------
# Fonte fallback: HTML (casadosdados)
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=60),
    reraise=True,
)
def _fetch_html(url: str, session: requests.Session) -> str:
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _parse_snapshot_listing_html(html: str, snapshot_url: str) -> list[RemoteFile]:
    """Extrai arquivos .zip de uma listagem Apache/nginx."""
    soup = BeautifulSoup(html, "lxml")
    files: list[RemoteFile] = []

    # Estratégia 1: bloco <pre>
    pre = soup.find("pre")
    if pre:
        for a in pre.find_all("a", href=True):
            href: str = a["href"]
            if not href.endswith(".zip"):
                continue
            filename = href.split("/")[-1]
            sibling_text = "".join(
                s if isinstance(s, str) else s.get_text()
                for s in a.next_siblings
            ).split("\n")[0]
            size_m = re.search(r"(\d+(?:\.\d+)?\s*[KMG])", sibling_text, re.I)
            date_m = re.search(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})", sibling_text)
            group, partition = _group_and_partition(filename)
            files.append(RemoteFile(
                name=filename,
                url=snapshot_url.rstrip("/") + "/" + filename,
                group=group, partition=partition,
                size_bytes=_parse_size(size_m.group(1)) if size_m else None,
                last_modified=_parse_date(date_m.group(1)) if date_m else None,
            ))
        if files:
            return files

    # Estratégia 2: tabela Apache
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        a = cells[1].find("a")
        if not a or not a.get("href", "").endswith(".zip"):
            continue
        filename = a["href"].split("/")[-1]
        group, partition = _group_and_partition(filename)
        files.append(RemoteFile(
            name=filename,
            url=snapshot_url.rstrip("/") + "/" + filename,
            group=group, partition=partition,
            size_bytes=_parse_size(cells[3].get_text(strip=True)),
            last_modified=_parse_date(cells[2].get_text(strip=True)),
        ))

    return files


def _discover_html(session: requests.Session, base_url: str) -> Snapshot:
    """Descobre o snapshot mais recente via listagem HTML."""
    logger.info("Buscando índice HTML: {}", base_url)
    html = _fetch_html(base_url, session)
    soup = BeautifulSoup(html, "lxml")

    dated_dirs: list[str] = []
    for a in soup.find_all("a", href=True):
        name = a["href"].rstrip("/").split("/")[-1]
        if _YYYY_MM_DD.match(name) or _YYYY_MM.match(name):
            dated_dirs.append(name)

    if not dated_dirs:
        raise RuntimeError(f"Nenhuma pasta de snapshot encontrada em {base_url}")

    latest = sorted(dated_dirs)[-1]
    snapshot_url = base_url.rstrip("/") + "/" + latest + "/"
    logger.info("Snapshot mais recente: {}", latest)

    snap_html = _fetch_html(snapshot_url, session)
    files = _parse_snapshot_listing_html(snap_html, snapshot_url)

    if not files:
        raise RuntimeError(f"Nenhum arquivo .zip em {latest}")

    logger.info("Snapshot {}: {} arquivos", latest, len(files))
    return Snapshot(date=latest, url=snapshot_url, files=files)


# ---------------------------------------------------------------------------
# Serviço OO + compatibilidade pública
# ---------------------------------------------------------------------------

class SnapshotCrawler:
    """Serviço responsável por descobrir snapshots disponíveis."""

    def __init__(
        self,
        fallback_base_url: str = FALLBACK_BASE_URL,
        default_headers: Optional[dict[str, str]] = None,
        rf_auth: tuple[str, str] = RF_AUTH,
    ) -> None:
        self.fallback_base_url = fallback_base_url
        self.default_headers = default_headers or HEADERS
        self.rf_auth = rf_auth

    def create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self.default_headers)
        return session

    def discover_latest_snapshot(self, session: requests.Session) -> Snapshot:
        logger.info("Fonte primária: WebDAV Receita Federal")
        return _discover_webdav(session)

    def discover_fallback_snapshot(self, session: requests.Session) -> Snapshot:
        return _discover_html(session, self.fallback_base_url)

    def discover_latest_snapshot_with_fallback(
        self,
        session: Optional[requests.Session] = None,
    ) -> Snapshot:
        own_session = session is None
        if own_session:
            session = self.create_session()

        try:
            try:
                return self.discover_latest_snapshot(session)
            except Exception as exc:
                logger.warning("WebDAV RF falhou ({}), tentando fallback HTML...", exc)
                return self.discover_fallback_snapshot(session)
        finally:
            if own_session:
                session.close()


def discover_latest_snapshot_with_fallback(
    session: Optional[requests.Session] = None,
) -> Snapshot:
    """
    Tenta a fonte primária (WebDAV RF), cai no fallback (HTML casadosdados) se falhar.
    """
    return SnapshotCrawler().discover_latest_snapshot_with_fallback(session=session)
