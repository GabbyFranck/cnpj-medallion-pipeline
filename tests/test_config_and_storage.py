from datetime import date

import pytest

from cnpj_pipeline import storage
from cnpj_pipeline.config import ALL_FILE_PARTS, Settings, parse_file_parts


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("all", ALL_FILE_PARTS),
        ("1", (1,)),
        ("0,3", (0, 3)),
        ("0-2,5", (0, 1, 2, 5)),
        (" 2 , 2 ", (2,)),
    ],
)
def test_parse_file_parts(raw, expected):
    assert parse_file_parts(raw) == expected


@pytest.mark.parametrize("raw", ["", "10", "a", "-1"])
def test_parse_file_parts_rejects_invalid(raw):
    with pytest.raises(ValueError):
        parse_file_parts(raw)


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("CNPJ_FILE_PARTS", "0-1")
    monkeypatch.setenv("CNPJ_RELEASE", "2026-10-13")
    monkeypatch.setenv("SPARK_DRIVER_MEMORY", "2g")
    settings = Settings.from_env()
    assert settings.file_parts == (0, 1)
    assert settings.release == "2026-10-13"
    assert settings.spark_driver_memory == "2g"


def test_bronze_path_follows_bucket_partition_layout(tmp_path):
    settings = Settings(lake_root=str(tmp_path))
    path = storage.bronze_file_path(settings, "empresas", date(2024, 1, 15), part=3)
    assert path.relative_to(tmp_path).as_posix() == (
        "datacorp-bronze/empresas/year=2024/month=01/day=15/empresas_20240115_3.csv"
    )


def test_s3_lake_root_switches_to_s3a_uris():
    settings = Settings(lake_root="s3")
    assert storage.table_uri(settings, storage.GOLD, "dim_empresas") == (
        "s3a://datacorp-gold/dim_empresas"
    )
