"""Discovery of monthly releases published by the Casa dos Dados mirror.

The mirror is an Apache directory listing with one folder per release
(``/arquivos/2026-10-13/``) containing ``Empresas0.zip`` .. ``Socios9.zip``.
"""

from __future__ import annotations

import re
from datetime import date
from typing import List, Optional, Sequence, Tuple

import requests

from cnpj_pipeline.schemas import EntitySpec

_RELEASE_HREF = re.compile(r'href="(\d{4}-\d{2}-\d{2})/"')


class ReleaseNotFoundError(RuntimeError):
    pass


def parse_release_dates(listing_html: str) -> List[date]:
    """Return every release folder found in the listing, oldest first."""
    found = {date.fromisoformat(m) for m in _RELEASE_HREF.findall(listing_html)}
    return sorted(found)


def resolve_release(
    session: requests.Session,
    base_url: str,
    requested: Optional[str] = None,
    timeout: int = 60,
) -> date:
    """Pick the requested release (validated against the mirror) or the latest one."""
    response = session.get(base_url, timeout=timeout)
    response.raise_for_status()
    available = parse_release_dates(response.text)
    if not available:
        raise ReleaseNotFoundError(f"No release folders found at {base_url}")

    if requested is None:
        return available[-1]

    wanted = date.fromisoformat(requested)
    if wanted not in available:
        raise ReleaseNotFoundError(
            f"Release {wanted} not published at {base_url}; latest is {available[-1]}"
        )
    return wanted


def release_url(base_url: str, release: date) -> str:
    return f"{base_url.rstrip('/')}/{release.isoformat()}/"


def source_files(
    spec: EntitySpec, base_url: str, release: date, parts: Sequence[int]
) -> List[Tuple[Optional[int], str, str]]:
    """``(part, filename, url)`` for every file of ``spec`` that should be ingested."""
    selected: Sequence[Optional[int]] = parts if spec.numbered_files else [None]
    files = []
    for part in selected:
        filename = spec.source_filename(part)
        files.append((part, filename, release_url(base_url, release) + filename))
    return files
