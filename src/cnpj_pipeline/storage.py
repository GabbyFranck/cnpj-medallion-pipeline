"""Lake path conventions.

Locally the lake is a folder that mimics S3 (``<lake_root>/<bucket>/<table>/...``).
Setting ``LAKE_ROOT=s3`` switches Spark paths to ``s3a://<bucket>/<table>`` without
touching any transformation code.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

from cnpj_pipeline.config import Settings

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"


def bucket_uri(settings: Settings, layer: str) -> str:
    bucket = {
        BRONZE: settings.bronze_bucket,
        SILVER: settings.silver_bucket,
        GOLD: settings.gold_bucket,
    }[layer]
    if settings.lake_root.lower() == "s3":
        return f"s3a://{bucket}"
    return str(Path(settings.lake_root).resolve() / bucket)


def table_uri(settings: Settings, layer: str, table: str) -> str:
    return f"{bucket_uri(settings, layer)}/{table}"


def date_partition(release: date) -> str:
    return f"year={release.year:04d}/month={release.month:02d}/day={release.day:02d}"


def bronze_partition_uri(settings: Settings, entity: str, release: date) -> str:
    return f"{table_uri(settings, BRONZE, entity)}/{date_partition(release)}"


def bronze_file_path(settings: Settings, entity: str, release: date, part: Optional[int]) -> Path:
    """``.../empresas/year=2026/month=10/day=13/empresas_20261013_1.csv``."""
    suffix = "" if part is None else f"_{part}"
    filename = f"{entity}_{release:%Y%m%d}{suffix}.csv"
    return Path(bronze_partition_uri(settings, entity, release)) / filename


def landing_zip_path(settings: Settings, release: date, source_filename: str) -> Path:
    return Path(settings.landing_dir).resolve() / release.isoformat() / source_filename


def control_marker_path(settings: Settings, release: date) -> Path:
    """Marker written after a release is fully published to Gold."""
    return Path(settings.lake_root).resolve() / "_control" / f"release={release.isoformat()}.done"
