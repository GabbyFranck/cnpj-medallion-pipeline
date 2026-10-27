"""Runs landing -> Bronze -> Silver -> Gold on tiny Receita-formatted zips (no network)."""

from contextlib import contextmanager
from datetime import date

import pytest

from cnpj_pipeline import storage, tasks
from cnpj_pipeline.quality.checks import DataQualityError
from cnpj_pipeline.schemas import ESTABELECIMENTOS

from .conftest import write_receita_zip

RELEASE = "2026-10-13"


@pytest.fixture(autouse=True)
def shared_spark(spark, monkeypatch):
    """Tasks normally own (and stop) their session; in tests they reuse the shared one."""

    @contextmanager
    def reuse(app_name, settings):
        yield spark

    monkeypatch.setattr(tasks, "spark_session", reuse)


def estab(basico, ordem, situacao, cnae, uf):
    row = [""] * len(ESTABELECIMENTOS.columns)
    for name, value in {
        "cnpj_basico": basico,
        "cnpj_ordem": ordem,
        "cnpj_dv": "10",
        "identificador_matriz_filial": "1" if ordem == "0001" else "2",
        "situacao_cadastral": situacao,
        "data_inicio_atividade": "20150601",
        "cnae_fiscal_principal": cnae,
        "uf": uf,
        "municipio": "7107",
    }.items():
        row[ESTABELECIMENTOS.columns.index(name)] = value
    return row


def land_fixture_release(settings):
    release = date.fromisoformat(RELEASE)
    land = lambda name: storage.landing_zip_path(settings, release, name)  # noqa: E731

    write_receita_zip(
        land("Empresas0.zip"),
        "EMPRECSV",
        [
            ["11111111", "PADARIA SÃO JOÃO LTDA", "2062", "49", "10.000,00", "01", ""],
            ["22222222", "SOFTWARE ÁGIL SA", "2054", "10", "250.000,00", "05", ""],
        ],
    )
    write_receita_zip(
        land("Estabelecimentos0.zip"),
        "ESTABELE",
        [
            estab("11111111", "0001", "02", "1091102", "SP"),
            estab("11111111", "0002", "02", "1091102", "SP"),
            estab("22222222", "0001", "02", "6201501", "SP"),
            estab("22222222", "0002", "08", "6201501", "MG"),
        ],
    )
    write_receita_zip(
        land("Socios0.zip"),
        "SOCIOCSV",
        [
            [
                "11111111",
                "2",
                "JOÃO DA SILVA",
                "***111222**",
                "49",
                "20100101",
                "",
                "",
                "",
                "0",
                "6",
            ],
            [
                "22222222",
                "1",
                "HOLDING XYZ",
                "33333333000199",
                "22",
                "20180505",
                "",
                "",
                "",
                "0",
                "0",
            ],
        ],
    )
    write_receita_zip(
        land("Cnaes.zip"),
        "CNAECSV",
        [
            ["1091102", "Fabricação de produtos de panificação"],
            ["6201501", "Desenvolvimento de software"],
        ],
    )


def test_full_pipeline_on_fixture_release(spark, settings):
    land_fixture_release(settings)

    for entity in ("empresas", "estabelecimentos", "socios", "cnaes"):
        bronze_files = tasks.load_bronze(entity, RELEASE, settings)
        assert all("year=2026/month=10/day=13" in f for f in bronze_files)

    metrics = {
        e: tasks.build_silver(e, RELEASE, settings)
        for e in ("empresas", "estabelecimentos", "socios", "cnaes")
    }
    assert metrics["estabelecimentos"]["silver_rows"] == 4
    assert metrics["empresas"]["corrupt_rows"] == 0

    counts = tasks.build_gold(RELEASE, settings)
    assert counts == {
        "dim_empresas": 2,
        "fact_estabelecimentos": 4,
        "rel_empresa_socio": 2,
        "agg_empresas_ativas_cnae_uf": 2,
    }

    empresas = spark.read.format("delta").load(
        storage.table_uri(settings, storage.SILVER, "empresas")
    )
    # Accents survive the ISO-8859-1 -> UTF-8 round trip.
    assert {r.razao_social for r in empresas.collect()} == {
        "PADARIA SÃO JOÃO LTDA",
        "SOFTWARE ÁGIL SA",
    }

    assert tasks.is_release_pending(RELEASE, settings=settings)
    tasks.mark_release_done(RELEASE, settings)
    assert not tasks.is_release_pending(RELEASE, force="False", settings=settings)
    assert tasks.is_release_pending(RELEASE, force="True", settings=settings)


def test_silver_fails_when_too_many_malformed_lines(spark, settings):
    release = date.fromisoformat(RELEASE)
    bronze = storage.bronze_file_path(settings, "empresas", release, 0)
    bronze.parent.mkdir(parents=True, exist_ok=True)
    bronze.write_text(
        '"11111111";"OK";"2062";"49";"1,00";"01";""\n"broken line with too few fields"\n',
        encoding="iso-8859-1",
    )
    with pytest.raises(DataQualityError, match="corrupt_lines"):
        tasks.build_silver("empresas", RELEASE, settings)
