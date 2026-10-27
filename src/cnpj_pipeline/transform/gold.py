"""Silver -> Gold: a small star schema plus the business-question aggregate.

* ``dim_empresas``          one row per company (cnpj_basico) with establishment/partner counts
* ``fact_estabelecimentos`` one row per establishment (cnpj) with company attributes
* ``rel_empresa_socio``     bridge company <-> partner
* ``agg_empresas_ativas_cnae_uf`` answers: "how are active companies distributed by CNAE in
  each state?"
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_dim_empresas(
    empresas: DataFrame, estabelecimentos: DataFrame, socios: DataFrame
) -> DataFrame:
    estab_counts = estabelecimentos.groupBy("cnpj_basico").agg(
        F.count(F.lit(1)).alias("qtd_estabelecimentos"),
        F.sum(F.col("is_ativo").cast("int")).alias("qtd_estabelecimentos_ativos"),
        F.max(F.when(F.col("identificador_matriz_filial") == 1, F.col("uf"))).alias("uf_matriz"),
    )
    socio_counts = socios.groupBy("cnpj_basico").agg(F.count(F.lit(1)).alias("qtd_socios"))

    return (
        empresas.join(estab_counts, "cnpj_basico", "left")
        .join(socio_counts, "cnpj_basico", "left")
        .select(
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "porte_empresa_descricao",
            "ente_federativo_responsavel",
            "uf_matriz",
            F.coalesce("qtd_estabelecimentos", F.lit(0)).alias("qtd_estabelecimentos"),
            F.coalesce("qtd_estabelecimentos_ativos", F.lit(0)).alias(
                "qtd_estabelecimentos_ativos"
            ),
            F.coalesce("qtd_socios", F.lit(0)).alias("qtd_socios"),
            (F.coalesce("qtd_estabelecimentos_ativos", F.lit(0)) > 0).alias("is_ativa"),
            "reference_date",
        )
    )


def build_fact_estabelecimentos(
    estabelecimentos: DataFrame, empresas: DataFrame, cnaes: DataFrame
) -> DataFrame:
    company_attrs = empresas.select(
        "cnpj_basico", "razao_social", "porte_empresa", "natureza_juridica", "capital_social"
    )
    cnae_labels = F.broadcast(
        cnaes.select(
            F.col("cnae").alias("cnae_fiscal_principal"),
            F.col("cnae_descricao").alias("cnae_fiscal_principal_descricao"),
        )
    )
    return (
        estabelecimentos.join(company_attrs, "cnpj_basico", "left")
        .join(cnae_labels, "cnae_fiscal_principal", "left")
        .select(
            "cnpj",
            "cnpj_basico",
            "identificador_matriz_filial",
            "razao_social",
            "nome_fantasia",
            "situacao_cadastral",
            "situacao_cadastral_descricao",
            "is_ativo",
            "data_inicio_atividade",
            F.year("data_inicio_atividade").alias("ano_inicio_atividade"),
            "cnae_fiscal_principal",
            "cnae_fiscal_principal_descricao",
            F.size("cnae_fiscal_secundaria").alias("qtd_cnaes_secundarios"),
            "porte_empresa",
            "natureza_juridica",
            "capital_social",
            "municipio",
            "uf",
            "reference_date",
        )
    )


def build_rel_empresa_socio(socios: DataFrame) -> DataFrame:
    return socios.select(
        "cnpj_basico",
        "identificador_socio",
        "tipo_socio",
        "nome_socio_razao_social",
        "cpf_cnpj_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "faixa_etaria",
        "faixa_etaria_descricao",
        "reference_date",
    )


def build_agg_empresas_ativas_cnae_uf(fact_estabelecimentos: DataFrame) -> DataFrame:
    """Active establishments and distinct active companies per (UF, CNAE), with the
    CNAE's share and rank inside the state."""
    per_uf = Window.partitionBy("uf")
    ranking = Window.partitionBy("uf").orderBy(
        F.desc("qtd_empresas_ativas"), F.asc("cnae_fiscal_principal")
    )
    return (
        fact_estabelecimentos.filter(F.col("is_ativo") & F.col("uf").isNotNull())
        .groupBy("uf", "cnae_fiscal_principal", "cnae_fiscal_principal_descricao")
        .agg(
            F.countDistinct("cnpj_basico").alias("qtd_empresas_ativas"),
            F.count(F.lit(1)).alias("qtd_estabelecimentos_ativos"),
        )
        .withColumn(
            "pct_empresas_ativas_uf",
            F.round(
                F.col("qtd_empresas_ativas") * 100 / F.sum("qtd_empresas_ativas").over(per_uf), 4
            ),
        )
        .withColumn("rank_cnae_uf", F.row_number().over(ranking))
    )
