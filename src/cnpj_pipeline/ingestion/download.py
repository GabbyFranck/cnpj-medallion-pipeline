"""Streaming, resumable-by-skip HTTP downloads.

Files are streamed to disk in chunks (``Estabelecimentos0.zip`` alone is >2 GB), written
to a ``.part`` file and atomically renamed, so a crashed task never leaves a truncated
zip that a retry would mistake for a finished one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cnpj_pipeline.logging_utils import get_logger

logger = get_logger(__name__)


def build_http_session(retries: int = 5, backoff_factor: float = 2.0) -> requests.Session:
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "cnpj-medallion-pipeline/1.0"
    return session


def remote_size(session: requests.Session, url: str, timeout: int) -> Optional[int]:
    try:
        response = session.head(url, allow_redirects=True, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("HEAD %s failed (%s); size check skipped", url, exc)
        return None
    length = response.headers.get("Content-Length")
    return int(length) if length and length.isdigit() else None


def download_file(
    session: requests.Session,
    url: str,
    destination: Path,
    timeout: int = 60,
    chunk_bytes: int = 8 * 1024 * 1024,
) -> Path:
    """Download ``url`` to ``destination``; skip if an identical-size copy already exists."""
    expected_size = remote_size(session, url, timeout)
    if destination.exists() and expected_size == destination.stat().st_size:
        logger.info("Skipping %s: already downloaded (%d bytes)", destination.name, expected_size)
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    written = 0

    logger.info("Downloading %s -> %s", url, destination)
    with session.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with partial.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_bytes):
                handle.write(chunk)
                written += len(chunk)

    if expected_size is not None and written != expected_size:
        partial.unlink(missing_ok=True)
        raise IOError(f"Incomplete download of {url}: got {written} of {expected_size} bytes")

    partial.replace(destination)
    logger.info("Downloaded %s (%.1f MB)", destination.name, written / 1024**2)
    return destination
