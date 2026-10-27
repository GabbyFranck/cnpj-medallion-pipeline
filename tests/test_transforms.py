from datetime import date
from decimal import Decimal

from cnpj_pipeline.schemas import EMPRESAS, ESTABELECIMENTOS, SOCIOS
from cnpj_pipeline.transform import gold, silver


def raw_df(spark, spec, rows):
    """Bronze-shaped DataFrame: all strings, in source column order."""
    columns = list(spec.columns) + ["_source_file"]
    return spark.createDataFrame(
        [tuple(r) + ("bronze.csv",) for r in rows], ", ".join(f"{c} string" for c in columns)
    )


def estab_row(basico, ordem="0001", dv="00", matriz="1", situacao="02", cnae="4711302", uf="SP"):
    row = [""] * len(ESTABELECIMENTOS.columns)
    values = {
        "cnpj_basico": basico,
        "cnpj_ordem": ordem,
        "cnpj_dv": dv,
        "identificador_matriz_filial": matriz,
        "situacao_cadastral": situacao,
        "data_inicio_atividade": "20200110",
        "cnae_fiscal_principal": cnae,
        "cnae_fiscal_secundaria": "4712100,4713002",
        "uf": uf,
        "municipio": "7107",
        "ddd_1": "11",
        "telefone_1": "33334444",
        "correio_eletronico": " CONTATO@EXEMPLO.COM ",
    }
    for name, value in values.items():
        row[ESTABELECIMENTOS.columns.index(name)] = value
    return row


def test_transform_empresas_types_dedups_and_drops_invalid_keys(spark):
    rows = [
        ["12345678", " acme ltda ", "2062", "49", "1.500,50", "01", ""],
        ["12345678", "ACME LTDA", "2062", "49", "1.500,50", "01", ""],
        ["", "NO KEY", "2062", "49", "0,00", "05", ""],
    ]
    result = silver.transform_empresas(raw_df(spark, EMPRESAS, rows)).collect()

    assert len(result) == 1
    row = result[0]
    assert row.cnpj_basico == "12345678"
    assert row.razao_social == "ACME LTDA"
    assert row.natureza_juridica == 2062
    assert row.capital_social == Decimal("1500.50")
    assert row.porte_empresa_descricao == "MICRO EMPRESA"
    assert row.ente_federativo_responsavel is None


def test_transform_estabelecimentos_builds_cnpj_and_flags(spark):
    rows = [
        estab_row("1234567", situacao="02"),
        estab_row("1234567", situacao="02"),
        estab_row("22222222", ordem="2", dv="5", matriz="2", situacao="08", uf="rj"),
    ]
    result = {
        r.cnpj: r
        for r in silver.transform_estabelecimentos(raw_df(spark, ESTABELECIMENTOS, rows)).collect()
    }

    assert set(result) == {"01234567000100", "22222222000205"}
    active = result["01234567000100"]
    assert active.is_ativo is True
    assert active.situacao_cadastral_descricao == "ATIVA"
    assert active.matriz_filial_descricao == "MATRIZ"
    assert active.data_inicio_atividade == date(2020, 1, 10)
    assert active.cnae_fiscal_secundaria == ["4712100", "4713002"]
    assert active.telefone_1 == "1133334444"
    assert active.telefone_2 is None
    assert active.correio_eletronico == "contato@exemplo.com"

    closed = result["22222222000205"]
    assert closed.is_ativo is False
    assert closed.uf == "RJ"


def test_transform_socios_maps_codes_and_dedups_on_business_key(spark):
    row = ["12345678", "2", "fulano de tal", "***123456**", "49", "20190301", "", "", "", "0", "4"]
    result = silver.transform_socios(raw_df(spark, SOCIOS, [row, row])).collect()

    assert len(result) == 1
    assert result[0].tipo_socio == "PESSOA FISICA"
    assert result[0].faixa_etaria_descricao == "31 A 40 ANOS"
    assert result[0].data_entrada_sociedade == date(2019, 3, 1)


def silver_frames(spark):
    ref = date(2026, 10, 13)
    empresas = silver.with_lineage(
        silver.transform_empresas(
            raw_df(
                spark,
                EMPRESAS,
                [
                    ["11111111", "ALFA", "2062", "49", "100,00", "01", ""],
                    ["22222222", "BETA", "2062", "49", "100,00", "05", ""],
                ],
            )
        ),
        ref,
    )
    estabelecimentos = silver.with_lineage(
        silver.transform_estabelecimentos(
            raw_df(
                spark,
                ESTABELECIMENTOS,
                [
                    estab_row("11111111", "0001", cnae="4711302", uf="SP"),
                    estab_row("11111111", "0002", matriz="2", cnae="4711302", uf="SP"),
                    estab_row("22222222", "0001", cnae="6201501", uf="SP"),
                    estab_row(
                        "22222222", "0002", matriz="2", cnae="6201501", uf="RJ", situacao="08"
                    ),
                    estab_row("33333333", "0001", cnae="6201501", uf="RJ"),
                ],
            )
        ),
        ref,
    )
    socios = silver.with_lineage(
        silver.transform_socios(
            raw_df(
                spark,
                SOCIOS,
                [["11111111", "2", "SOCIO A", "***1**", "49", "20200101", "", "", "", "0", "4"]],
            )
        ),
        ref,
    )
    cnaes = spark.createDataFrame(
        [("4711302", "Comercio varejista"), ("6201501", "Desenvolvimento de software")],
        "cnae string, cnae_descricao string",
    )
    return empresas, estabelecimentos, socios, cnaes


def test_dim_empresas_counts_and_activity(spark):
    empresas, estabelecimentos, socios, _ = silver_frames(spark)
    dim = {
        r.cnpj_basico: r
        for r in gold.build_dim_empresas(empresas, estabelecimentos, socios).collect()
    }

    assert dim["11111111"].qtd_estabelecimentos == 2
    assert dim["11111111"].qtd_estabelecimentos_ativos == 2
    assert dim["11111111"].qtd_socios == 1
    assert dim["11111111"].uf_matriz == "SP"
    assert dim["22222222"].qtd_estabelecimentos_ativos == 1
    assert dim["22222222"].qtd_socios == 0
    assert dim["22222222"].is_ativa is True


def test_fact_and_agg_answer_active_companies_by_cnae_and_uf(spark):
    empresas, estabelecimentos, _, cnaes = silver_frames(spark)
    fact = gold.build_fact_estabelecimentos(estabelecimentos, empresas, cnaes)

    orphan = fact.filter("cnpj_basico = '33333333'").first()
    assert orphan.razao_social is None
    assert orphan.cnae_fiscal_principal_descricao == "Desenvolvimento de software"

    agg = {
        (r.uf, r.cnae_fiscal_principal): r
        for r in gold.build_agg_empresas_ativas_cnae_uf(fact).collect()
    }
    # Two active establishments of the same company count once as a company.
    assert agg[("SP", "4711302")].qtd_empresas_ativas == 1
    assert agg[("SP", "4711302")].qtd_estabelecimentos_ativos == 2
    assert agg[("SP", "4711302")].pct_empresas_ativas_uf == 50.0
    # The closed RJ branch of 22222222 is excluded.
    assert agg[("RJ", "6201501")].qtd_empresas_ativas == 1
    assert agg[("RJ", "6201501")].pct_empresas_ativas_uf == 100.0
    assert agg[("RJ", "6201501")].rank_cnae_uf == 1
    assert len(agg) == 3
