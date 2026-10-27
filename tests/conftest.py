from __future__ import annotations

import zipfile
from dataclasses import replace
from pathlib import Path
from typing import Iterable, Sequence

import pytest

from cnpj_pipeline.config import Settings
from cnpj_pipeline.spark_session import build_spark


@pytest.fixture(scope="session")
def spark():
    settings = replace(
        Settings.from_env(),
        spark_master="local[2]",
        spark_driver_memory="1g",
        spark_shuffle_partitions=2,
    )
    session = build_spark("cnpj-tests", settings)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        lake_root=str(tmp_path / "lake"),
        landing_dir=str(tmp_path / "landing"),
        source_base_url="https://mirror.test/arquivos/",
        file_parts=(0,),
        spark_master="local[2]",
        spark_driver_memory="1g",
        spark_shuffle_partitions=2,
    )


def receita_lines(rows: Iterable[Sequence[str]]) -> str:
    """Render rows the way Receita publishes them: quoted, ``;``-separated, no header."""
    return "".join(";".join(f'"{v}"' for v in row) + "\n" for row in rows)


def write_receita_zip(path: Path, member_name: str, rows: Iterable[Sequence[str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, receita_lines(rows).encode("iso-8859-1"))
    return path
