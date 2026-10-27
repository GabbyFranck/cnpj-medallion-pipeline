"""Runtime settings, read once from environment variables.

Every knob that changes between environments (paths, URLs, volume, Spark sizing)
lives here so the rest of the code never calls ``os.getenv`` directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional, Tuple

ALL_FILE_PARTS: Tuple[int, ...] = tuple(range(10))


def parse_file_parts(raw: str) -> Tuple[int, ...]:
    """Parse which numbered source files (``Empresas0.zip`` .. ``Empresas9.zip``) to ingest.

    Accepts ``"all"``, a comma list (``"0,3"``), a range (``"0-4"``) or a mix of both.
    """
    value = raw.strip().lower()
    if value == "all":
        return ALL_FILE_PARTS

    parts = set()
    for token in filter(None, (t.strip() for t in value.split(","))):
        if "-" in token:
            start, end = (int(x) for x in token.split("-", 1))
            parts.update(range(start, end + 1))
        else:
            parts.add(int(token))

    invalid = sorted(p for p in parts if p not in ALL_FILE_PARTS)
    if invalid or not parts:
        raise ValueError(f"Invalid CNPJ_FILE_PARTS={raw!r}: parts must be within 0-9")
    return tuple(sorted(parts))


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip() or default


@dataclass(frozen=True)
class Settings:
    lake_root: str = "data/lake"
    landing_dir: str = "data/landing"
    bronze_bucket: str = "datacorp-bronze"
    silver_bucket: str = "datacorp-silver"
    gold_bucket: str = "datacorp-gold"
    source_base_url: str = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
    release: Optional[str] = None
    file_parts: Tuple[int, ...] = field(default=(1,))
    http_timeout_seconds: int = 60
    download_chunk_bytes: int = 8 * 1024 * 1024
    spark_master: str = "local[*]"
    spark_driver_memory: str = "4g"
    spark_shuffle_partitions: int = 64
    spark_ivy_dir: Optional[str] = None
    max_corrupt_ratio: float = 0.001
    max_orphan_ratio: float = 0.05

    @classmethod
    def from_env(cls) -> "Settings":
        defaults = cls()
        return cls(
            lake_root=_env("LAKE_ROOT", defaults.lake_root),
            landing_dir=_env("LANDING_DIR", defaults.landing_dir),
            bronze_bucket=_env("BRONZE_BUCKET", defaults.bronze_bucket),
            silver_bucket=_env("SILVER_BUCKET", defaults.silver_bucket),
            gold_bucket=_env("GOLD_BUCKET", defaults.gold_bucket),
            source_base_url=_env("CNPJ_BASE_URL", defaults.source_base_url),
            release=os.getenv("CNPJ_RELEASE", "").strip() or None,
            file_parts=parse_file_parts(_env("CNPJ_FILE_PARTS", "1")),
            http_timeout_seconds=int(_env("HTTP_TIMEOUT_SECONDS", "60")),
            download_chunk_bytes=int(_env("DOWNLOAD_CHUNK_BYTES", str(8 * 1024 * 1024))),
            spark_master=_env("SPARK_MASTER", defaults.spark_master),
            spark_driver_memory=_env("SPARK_DRIVER_MEMORY", defaults.spark_driver_memory),
            spark_shuffle_partitions=int(_env("SPARK_SHUFFLE_PARTITIONS", "64")),
            spark_ivy_dir=os.getenv("SPARK_IVY_DIR", "").strip() or None,
            max_corrupt_ratio=float(_env("DQ_MAX_CORRUPT_RATIO", "0.001")),
            max_orphan_ratio=float(_env("DQ_MAX_ORPHAN_RATIO", "0.05")),
        )
