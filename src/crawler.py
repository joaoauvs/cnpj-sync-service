"""
Crawler: discovers the latest snapshot and lists its files.

Responsibilities
----------------
* Fetch the main index page and parse all dated directories.
* Identify the most recent snapshot by folder name (YYYY-MM-DD).
* Enter that folder and enumerate every ZIP file with its metadata.
* Return a fully-populated ``Snapshot`` model.

For Receita Federal source: Uses WebDAV API instead of HTML parsing.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import (
    BACKOFF_FACTOR,
    FALLBACK_BASE_URL,
    HEADERS,
    MAX_RETRIES,
    PARTITIONED_GROUPS,
    PRIMARY_BASE_URL,
    REFERENCE_FILES,
    REQUEST_TIMEOUT,
)
from src.logger import logger
from src.models import RemoteFile, Snapshot

# WebDAV namespace for parsing PROPFIND responses
DAV_NS = "{DAV:}"

# Regex patterns for HTML directory listings
_DIR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})/?$")
_SIZE_MAP = {"K": 1_024, "M": 1_024**2, "G": 1_024**3}


def _parse_size(raw: str) -> Optional[int]:
    """Convert human-readable size like '486M' to bytes."""
    raw = raw.strip()
    if not raw or raw == "-":
        return None
    match = re.match(r"^([\d.]+)\s*([KMG]?)$", raw, re.I)
    if not match:
        return None
    value, unit = float(match.group(1)), match.group(2).upper()
    return int(value * _SIZE_MAP.get(unit, 1))


def _parse_date(raw: str) -> Optional[datetime]:
    """Parse dates like '2026-03-24 15:16' from Apache/nginx directory listings."""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _parse_webdav_date(date_str: str) -> Optional[datetime]:
    """Parse WebDAV date format (RFC 1123/RFC 822)."""
    # Example: "Mon, 24 Mar 2026 15:16:00 GMT"
    try:
        return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
    except ValueError:
        return None


def _parse_webdav_response(xml_content: str, base_url: str) -> list[RemoteFile]:
    """
    Parse WebDAV PROPFIND response to extract file listings.

    Returns a list of RemoteFile objects with metadata.
    """
    files: list[RemoteFile] = []

    try:
        root = ET.fromstring(xml_content)

        # Find all response elements
        for response in root.findall(f".//{DAV_NS}response"):
            # Get the href (path)
            href_elem = response.find(f"{DAV_NS}href")
            if href_elem is None or href_elem.text is None:
                continue

            href = href_elem.text.strip()

            # Skip if it's a directory (ends with /)
            if href.endswith("/"):
                continue

            # Extract filename from href
            filename = href.rstrip("/").split("/")[-1]

            # Only process ZIP files
            if not filename.endswith(".zip"):
                continue

            # Extract metadata from propstat
            size_bytes = None
            last_modified = None

            propstat = response.find(f"{DAV_NS}propstat")
            if propstat is not None:
                prop = propstat.find(f"{DAV_NS}prop")
                if prop is not None:
                    # Get file size
                    size_elem = prop.find(f"{DAV_NS}getcontentlength")
                    if size_elem is not None and size_elem.text:
                        try:
                            size_bytes = int(size_elem.text)
                        except ValueError:
                            pass

                    # Get last modified date
                    date_elem = prop.find(f"{DAV_NS}getlastmodified")
                    if date_elem is not None and date_elem.text:
                        last_modified = _parse_webdav_date(date_elem.text)

            # Determine group and partition
            group, partition = _group_and_partition(filename)

            # Construct full URL
            # WebDAV href is relative to the base WebDAV endpoint
            # We need to construct the download URL
            url = f"https://arquivos.receitafederal.gov.br/public.php/webdav/{href}"

            files.append(
                RemoteFile(
                    name=filename,
                    url=url,
                    group=group,
                    partition=partition,
                    size_bytes=size_bytes,
                    last_modified=last_modified,
                )
            )

    except ET.ParseError as e:
        logger.error("Failed to parse WebDAV XML: {}", e)
        return []

    return files


def _fetch_webdav(url: str, session: requests.Session, depth: str = "1") -> str:
    """
    Perform WebDAV PROPFIND request to list directory contents.

    Parameters
    ----------
    url : str
        WebDAV endpoint URL
    session : requests.Session
        HTTP session
    depth : str
        Depth header: "0" (self), "1" (immediate children), "infinity"

    Returns
    -------
    str
        XML response content
    """
    headers = {
        "Depth": depth,
        "Content-Type": "application/xml; charset=utf-8",
    }

    # WebDAV PROPFIND request body
    body = f"""<?xml version="1.0" encoding="utf-8" ?>
<propfind xmlns="DAV:">
    <prop>
        <getcontentlength xmlns="DAV:"/>
        <getlastmodified xmlns="DAV:"/>
        <resourcetype xmlns="DAV:"/>
    </prop>
</propfind>"""

    logger.debug("PROPFIND {} (Depth: {})", url, depth)
    resp = session.request("PROPFIND", url, headers=headers, data=body, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _group_and_partition(filename: str) -> tuple[str, Optional[int]]:
    """
    Derive the logical group and partition index from a filename.

    Examples
    --------
    'Empresas3.zip' → ('Empresas', 3)
    'Simples.zip'   → ('Simples', None)
    'Cnaes.zip'     → ('Cnaes', None)
    """
    stem = filename.replace(".zip", "")
    for group in PARTITIONED_GROUPS:
        if stem.startswith(group):
            suffix = stem[len(group):]
            if suffix.isdigit():
                return group, int(suffix)
    return stem, None


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=60),
    reraise=True,
)
def _fetch(url: str, session: requests.Session) -> str:
    """HTTP GET with retry logic. Returns response text."""
    logger.debug("GET {}", url)
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _parse_index(html: str, base_url: str) -> list[tuple[str, str]]:
    """
    Parse an Apache/nginx-style directory listing.

    Returns a list of (name, href) for all entries.
    """
    soup = BeautifulSoup(html, "lxml")
    entries: list[tuple[str, str]] = []

    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"]
        # Skip parent directory links
        if href in ("../", "/", "#") or href.startswith("?"):
            continue
        name = a_tag.get_text(strip=True)
        entries.append((name, href))

    return entries


def _parse_snapshot_listing(
    html: str, snapshot_url: str
) -> list[RemoteFile]:
    """
    Parse the file listing of a snapshot folder.

    Handles both:
    - Apache table-style listings (<tr><td> per file)
    - Apache pre-formatted listings (<pre> with inline text)

    Returns a list of ``RemoteFile`` instances.
    """
    soup = BeautifulSoup(html, "lxml")
    files: list[RemoteFile] = []

    # Strategy 1: Apache <pre> block — each line is:
    #   <a href="Cnaes.zip">Cnaes.zip</a>  2026-03-24 15:16   22K
    # We iterate <a> tags inside <pre> and parse the trailing text.
    pre_tag = soup.find("pre")
    if pre_tag:
        for a_tag in pre_tag.find_all("a", href=True):
            href: str = a_tag["href"]
            if not href.endswith(".zip"):
                continue

            filename = href.split("/")[-1]

            # Text immediately after the closing </a> tag on the same line
            sibling_text = ""
            for sibling in a_tag.next_siblings:
                text = sibling if isinstance(sibling, str) else sibling.get_text()
                sibling_text += text
                if "\n" in sibling_text:
                    break

            # Extract date (YYYY-MM-DD HH:MM) and size from trailing text
            date_match = re.search(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})", sibling_text)
            size_match = re.search(r"(\d+(?:\.\d+)?\s*[KMG])", sibling_text, re.I)

            raw_date = date_match.group(1) if date_match else ""
            raw_size = size_match.group(1) if size_match else ""

            group, partition = _group_and_partition(filename)
            url = snapshot_url.rstrip("/") + "/" + filename

            files.append(
                RemoteFile(
                    name=filename,
                    url=url,
                    group=group,
                    partition=partition,
                    size_bytes=_parse_size(raw_size),
                    last_modified=_parse_date(raw_date),
                )
            )

        if files:
            return files
        logger.debug("<pre> block found but no .zip links parsed; trying table strategy")

    # Strategy 2: Apache table-style (<tr><td> per file)
    # Apache auto-index uses 5 columns: [icon][name][date][size][description]
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        # Link is in column 1 (column 0 is the icon)
        a_tag = cells[1].find("a")
        if not a_tag:
            continue
        href = a_tag.get("href", "")
        if not href.endswith(".zip"):
            continue

        filename = href.split("/")[-1]
        raw_date = cells[2].get_text(strip=True)
        raw_size = cells[3].get_text(strip=True)

        group, partition = _group_and_partition(filename)
        url = snapshot_url.rstrip("/") + "/" + filename

        files.append(
            RemoteFile(
                name=filename,
                url=url,
                group=group,
                partition=partition,
                size_bytes=_parse_size(raw_size),
                last_modified=_parse_date(raw_date),
            )
        )

    if files:
        return files

    # Strategy 3: bare link scan (last resort — no size/date metadata)
    logger.warning("Structured parsing found no files; falling back to bare link scan")
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if not href.endswith(".zip"):
            continue
        filename = href.split("/")[-1]
        group, partition = _group_and_partition(filename)
        url = snapshot_url.rstrip("/") + "/" + filename
        files.append(
            RemoteFile(
                name=filename,
                url=url,
                group=group,
                partition=partition,
                size_bytes=None,
                last_modified=None,
            )
        )

    return files


def discover_latest_snapshot_with_fallback(
    session: Optional[requests.Session] = None,
) -> Snapshot:
    """
    Try to discover the latest snapshot from primary source, fall back to secondary.

    Returns the first successful snapshot from either source.
    """
    sources = [
        ("primary", PRIMARY_BASE_URL),
        ("fallback", FALLBACK_BASE_URL),
    ]

    last_error = None
    for source_name, base_url in sources:
        try:
            logger.info("Trying {} source: {}", source_name, base_url)
            snapshot = _discover_latest_snapshot_single(session, base_url)
            logger.info("Successfully retrieved snapshot from {} source", source_name)
            return snapshot
        except Exception as e:
            logger.warning("Failed to retrieve from {} source: {}", source_name, str(e))
            last_error = e
            # Continue to try next source

    raise RuntimeError(
        f"Failed to retrieve snapshot from all sources. Last error: {last_error}"
    )


def _discover_latest_snapshot_single(
    session: Optional[requests.Session] = None,
    base_url: str = PRIMARY_BASE_URL,
) -> Snapshot:
    """
    Crawl the index page and return the most recent ``Snapshot``.

    Parameters
    ----------
    session:
        Optional pre-configured ``requests.Session``.  A new one is
        created if not provided.
    base_url:
        Root URL of the data server.

    Returns
    -------
    Snapshot
        Fully-populated with all ``RemoteFile`` entries.
    """
    own_session = session is None
    if own_session:
        session = requests.Session()
        session.headers.update(HEADERS)

    try:
        # Check if this is the Receita Federal source (WebDAV)
        if "arquivos.receitafederal.gov.br" in base_url:
            return _discover_latest_snapshot_webdav(session, base_url)
        else:
            return _discover_latest_snapshot_html(session, base_url)

    finally:
        if own_session:
            session.close()


def _discover_latest_snapshot_webdav(
    session: requests.Session,
    base_url: str,
) -> Snapshot:
    """
    Discover latest snapshot using WebDAV API (Receita Federal source).
    """
    # Convert the Nextcloud share URL to WebDAV endpoint
    # Example: https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK?dir=/Dados/Cadastros/CNPJ
    # WebDAV endpoint: https://arquivos.receitafederal.gov.br/public.php/webdav/Dados/Cadastros/CNPJ/

    # Extract the path from the URL
    if "?dir=" in base_url:
        # Extract the directory path from query parameter
        import urllib.parse
        parsed = urllib.parse.urlparse(base_url)
        query_params = urllib.parse.parse_qs(parsed.query)
        dir_path = query_params.get("dir", [""])[0]
    else:
        # Default path
        dir_path = "/Dados/Cadastros/CNPJ"

    # Construct WebDAV base URL
    webdav_base = "https://arquivos.receitafederal.gov.br/public.php/webdav"
    webdav_url = webdav_base + dir_path.rstrip("/") + "/"

    logger.info("Using WebDAV API: {}", webdav_url)

    # List directories at the base path
    xml_response = _fetch_webdav(webdav_url, session, depth="1")

    # Parse XML to get directory listings
    root = ET.fromstring(xml_response)
    dated_dirs: list[str] = []

    for response in root.findall(f".//{DAV_NS}response"):
        href_elem = response.find(f"{DAV_NS}href")
        if href_elem is None or href_elem.text is None:
            continue

        href = href_elem.text.strip()

        # Check if it's a directory (ends with /)
        if not href.endswith("/"):
            continue

        # Extract directory name
        dir_name = href.rstrip("/").split("/")[-1]

        # Check if it matches YYYY-MM pattern
        if re.match(r"^\d{4}-\d{2}$", dir_name):
            dated_dirs.append(dir_name)

    if not dated_dirs:
        raise RuntimeError(
            f"No dated directories found at {webdav_url}. "
            "The directory structure may have changed."
        )

    dated_dirs.sort()  # lexicographic = chronological for YYYY-MM
    latest_date = dated_dirs[-1]
    logger.info(
        "Found {} snapshots. Latest: {} (from {})",
        len(dated_dirs),
        latest_date,
        dated_dirs[0],
    )

    # List files in the latest directory
    snapshot_webdav_url = webdav_url.rstrip("/") + "/" + latest_date + "/"
    snapshot_xml = _fetch_webdav(snapshot_webdav_url, session, depth="1")
    files = _parse_webdav_response(snapshot_xml, snapshot_webdav_url)

    if not files:
        raise RuntimeError(
            f"No ZIP files found in snapshot {latest_date}. "
            "Check the URL or directory structure."
        )

    # Construct the public download URL (not WebDAV)
    snapshot_url = f"https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK?dir=/Dados/Cadastros/CNPJ/{latest_date}"

    logger.info(
        "Snapshot {}: {} files, ~{:.1f} GB compressed",
        latest_date,
        len(files),
        sum(f.size_bytes or 0 for f in files) / 1_024**3,
    )

    return Snapshot(date=latest_date, url=snapshot_url, files=files)


def _discover_latest_snapshot_html(
    session: requests.Session,
    base_url: str,
) -> Snapshot:
    """
    Discover latest snapshot using HTML parsing (fallback source).
    """
    logger.info("Fetching index: {}", base_url)
    html = _fetch(base_url, session)
    entries = _parse_index(html, base_url)

    # Identify dated directories
    dated_dirs: list[str] = []
    for name, href in entries:
        clean = href.rstrip("/").split("/")[-1]
        if _DIR_RE.match(clean):
            dated_dirs.append(clean)

    if not dated_dirs:
        raise RuntimeError(
            f"No dated directories found at {base_url}. "
            "The page structure may have changed."
        )

    dated_dirs.sort()  # lexicographic = chronological for YYYY-MM-DD
    latest_date = dated_dirs[-1]
    logger.info(
        "Found {} snapshots. Latest: {} (from {})",
        len(dated_dirs),
        latest_date,
        dated_dirs[0],
    )

    snapshot_url = base_url.rstrip("/") + "/" + latest_date + "/"
    logger.info("Fetching snapshot listing: {}", snapshot_url)
    snap_html = _fetch(snapshot_url, session)
    files = _parse_snapshot_listing(snap_html, snapshot_url)

    if not files:
        raise RuntimeError(
            f"No ZIP files found in snapshot {latest_date}. "
            "Check the URL or page structure."
        )

    logger.info(
        "Snapshot {}: {} files, ~{:.1f} GB compressed",
        latest_date,
        len(files),
        sum(f.size_bytes or 0 for f in files) / 1_024**3,
    )

    return Snapshot(date=latest_date, url=snapshot_url, files=files)


def discover_latest_snapshot(
    session: Optional[requests.Session] = None,
    base_url: str = PRIMARY_BASE_URL,
) -> Snapshot:
    """
    Crawl the index page and return the most recent ``Snapshot``.

    This is a wrapper that maintains backward compatibility.
    For fallback logic, use discover_latest_snapshot_with_fallback().
    """
    return _discover_latest_snapshot_single(session, base_url)


def list_all_snapshots_with_fallback(
    session: Optional[requests.Session] = None,
) -> list[str]:
    """
    Try to list all snapshots from primary source, fall back to secondary.

    Returns the first successful result from either source.
    """
    sources = [
        ("primary", PRIMARY_BASE_URL),
        ("fallback", FALLBACK_BASE_URL),
    ]

    last_error = None
    for source_name, base_url in sources:
        try:
            logger.info("Trying {} source for listing: {}", source_name, base_url)
            dates = _list_all_snapshots_single(session, base_url)
            logger.info("Successfully retrieved snapshot list from {} source", source_name)
            return dates
        except Exception as e:
            logger.warning("Failed to list from {} source: {}", source_name, str(e))
            last_error = e
            # Continue to try next source

    raise RuntimeError(
        f"Failed to list snapshots from all sources. Last error: {last_error}"
    )


def _list_all_snapshots_single(
    session: Optional[requests.Session] = None,
    base_url: str = PRIMARY_BASE_URL,
) -> list[str]:
    """Return all available snapshot dates (YYYY-MM-DD), oldest first."""
    own_session = session is None
    if own_session:
        session = requests.Session()
        session.headers.update(HEADERS)

    try:
        # Check if this is the Receita Federal source (WebDAV)
        if "arquivos.receitafederal.gov.br" in base_url:
            return _list_all_snapshots_webdav(session, base_url)
        else:
            return _list_all_snapshots_html(session, base_url)

    finally:
        if own_session:
            session.close()


def _list_all_snapshots_webdav(
    session: requests.Session,
    base_url: str,
) -> list[str]:
    """List all snapshots using WebDAV API."""
    # Extract the path from the URL
    if "?dir=" in base_url:
        import urllib.parse
        parsed = urllib.parse.urlparse(base_url)
        query_params = urllib.parse.parse_qs(parsed.query)
        dir_path = query_params.get("dir", [""])[0]
    else:
        dir_path = "/Dados/Cadastros/CNPJ"

    # Construct WebDAV base URL
    webdav_base = "https://arquivos.receitafederal.gov.br/public.php/webdav"
    webdav_url = webdav_base + dir_path.rstrip("/") + "/"

    # List directories at the base path
    xml_response = _fetch_webdav(webdav_url, session, depth="1")

    # Parse XML to get directory listings
    root = ET.fromstring(xml_response)
    dated_dirs: list[str] = []

    for response in root.findall(f".//{DAV_NS}response"):
        href_elem = response.find(f"{DAV_NS}href")
        if href_elem is None or href_elem.text is None:
            continue

        href = href_elem.text.strip()

        # Check if it's a directory (ends with /)
        if not href.endswith("/"):
            continue

        # Extract directory name
        dir_name = href.rstrip("/").split("/")[-1]

        # Check if it matches YYYY-MM pattern
        if re.match(r"^\d{4}-\d{2}$", dir_name):
            dated_dirs.append(dir_name)

    return sorted(dated_dirs)


def _list_all_snapshots_html(
    session: requests.Session,
    base_url: str,
) -> list[str]:
    """List all snapshots using HTML parsing."""
    html = _fetch(base_url, session)
    entries = _parse_index(html, base_url)
    dated = sorted(
        [
            href.rstrip("/").split("/")[-1]
            for _, href in entries
            if _DIR_RE.match(href.rstrip("/").split("/")[-1])
        ]
    )
    return dated


def list_all_snapshots(
    session: Optional[requests.Session] = None,
    base_url: str = PRIMARY_BASE_URL,
) -> list[str]:
    """
    Return all available snapshot dates (YYYY-MM-DD), oldest first.

    This is a wrapper that maintains backward compatibility.
    For fallback logic, use list_all_snapshots_with_fallback().
    """
    return _list_all_snapshots_single(session, base_url)
