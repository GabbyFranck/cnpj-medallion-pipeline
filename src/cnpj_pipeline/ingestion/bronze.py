"""Landing zip -> Bronze CSV.

Bronze keeps the source bytes untouched (same delimiter, quoting and ISO-8859-1
encoding); only the location changes to a date-partitioned, bucket-like layout.
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from cnpj_pipeline.logging_utils import get_logger

logger = get_logger(__name__)

COPY_BUFFER_BYTES = 16 * 1024 * 1024


class InvalidSourceArchiveError(ValueError):
    pass


def extract_single_member(zip_path: Path, destination: Path) -> Path:
    """Stream the single CSV inside ``zip_path`` to ``destination`` without loading it in memory."""
    try:
        archive = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile as exc:
        raise InvalidSourceArchiveError(f"{zip_path} is not a valid zip file") from exc

    with archive:
        members = [m for m in archive.infolist() if not m.is_dir()]
        if len(members) != 1:
            names = [m.filename for m in members]
            raise InvalidSourceArchiveError(f"{zip_path} should hold one file, found {names}")
        member = members[0]

        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        with archive.open(member) as source, partial.open("wb") as target:
            shutil.copyfileobj(source, target, length=COPY_BUFFER_BYTES)
        partial.replace(destination)

    logger.info(
        "Extracted %s/%s -> %s (%.1f MB)",
        zip_path.name,
        member.filename,
        destination,
        member.file_size / 1024**2,
    )
    return destination
