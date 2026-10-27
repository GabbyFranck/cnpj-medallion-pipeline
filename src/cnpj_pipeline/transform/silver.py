"""Bronze CSV -> Silver Delta.

Silver is the typed, cleaned, de-duplicated version of each source table:
codes become integers with human-readable labels, dates and money are parsed,
keys are normalized, and every row carries lineage columns.
"""

from __future__ import annotations

from datetime import date
from typing import Callable, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_pipeline.schemas import (
    CNAES,
    CORRUPT_RECORD_COLUMN,
    EMPRESAS,
    ESTABELECIMENTOS,
    FAIXA_ETARIA,
    IDENTIFICADOR_SOCIO,
    MATRIZ_FILIAL,
    PORTE_EMPRESA,
    SITUACAO_ATIVA,
    SITUACAO_CADASTRAL,
    SOCIOS,
    EntitySpec,
)
from cnpj_pipeline.transform.parsing import (
    clean_string,
    digits_only,
    map_code,
    parse_date,
    parse_decimal_br,
    parse_int,
    split_codes,
)

SOURCE_ENCODING = "ISO-8859-1"
SOURCE_DELIMITER = ";"


def read_bronze_csv(spark: SparkSession, spec: EntitySpec, path: str) -> DataFrame:
    """Read raw files with the fixed source schema; malformed lines land in ``_corrupt_record``."""
    return (
        spark.read.schema(spec.raw_schema())
        .option("sep", SOURCE_DELIMITER)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", SOURCE_ENCODING)
        .option("header", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .csv(path)
        .withColumn("_source_file", F.input_file_name())
    )


def split_corrupt(raw: DataFrame) -> DataFrame:
    """Drop malformed lines (wrong number of fields) and the helper column."""
    return raw.filter(F.col(CORRUPT_RECORD_COLUMN).isNull()).drop(CORRUPT_RECORD_COLUMN)


def with_lineage(df: DataFrame, release: date) -> DataFrame:
    return df.withColumn("reference_date", F.lit(release).cast("date")).withColumn(
        "ingested_at", F.current_timestamp()
    )


def transform_empresas(df: DataFrame) -> DataFrame:
    porte = parse_int(F.col("porte_empresa"))
    return (
        df.select(
            digits_only(F.col("cnpj_basico"), 8).alias("cnpj_basico"),
            F.upper(clean_string(F.col("razao_social"))).alias("razao_social"),
            parse_int(F.col("natureza_juridica")).alias("natureza_juridica"),
            parse_int(F.col("qualificacao_responsavel")).alias("qualificacao_responsavel"),
            parse_decimal_br(F.col("capital_social")).alias("capital_social"),
            porte.alias("porte_empresa"),
            map_code(porte, PORTE_EMPRESA).alias("porte_empresa_descricao"),
            clean_string(F.col("ente_federativo_responsavel")).alias("ente_federativo_responsavel"),
            "_source_file",
        )
        .filter(F.col("cnpj_basico").isNotNull())
        .dropDuplicates(["cnpj_basico"])
    )


def transform_estabelecimentos(df: DataFrame) -> DataFrame:
    situacao = parse_int(F.col("situacao_cadastral"))
    matriz_filial = parse_int(F.col("identificador_matriz_filial"))
    cnpj_basico = digits_only(F.col("cnpj_basico"), 8)
    cnpj_ordem = digits_only(F.col("cnpj_ordem"), 4)
    cnpj_dv = digits_only(F.col("cnpj_dv"), 2)
    return (
        df.select(
            F.concat(cnpj_basico, cnpj_ordem, cnpj_dv).alias("cnpj"),
            cnpj_basico.alias("cnpj_basico"),
            cnpj_ordem.alias("cnpj_ordem"),
            cnpj_dv.alias("cnpj_dv"),
            matriz_filial.alias("identificador_matriz_filial"),
            map_code(matriz_filial, MATRIZ_FILIAL).alias("matriz_filial_descricao"),
            F.upper(clean_string(F.col("nome_fantasia"))).alias("nome_fantasia"),
            situacao.alias("situacao_cadastral"),
            map_code(situacao, SITUACAO_CADASTRAL).alias("situacao_cadastral_descricao"),
            (F.coalesce(situacao, F.lit(-1)) == SITUACAO_ATIVA).alias("is_ativo"),
            parse_date(F.col("data_situacao_cadastral")).alias("data_situacao_cadastral"),
            parse_int(F.col("motivo_situacao_cadastral")).alias("motivo_situacao_cadastral"),
            clean_string(F.col("nome_cidade_exterior")).alias("nome_cidade_exterior"),
            parse_int(F.col("pais")).alias("pais"),
            parse_date(F.col("data_inicio_atividade")).alias("data_inicio_atividade"),
            digits_only(F.col("cnae_fiscal_principal"), 7).alias("cnae_fiscal_principal"),
            split_codes(F.col("cnae_fiscal_secundaria"), 7).alias("cnae_fiscal_secundaria"),
            clean_string(F.col("tipo_logradouro")).alias("tipo_logradouro"),
            clean_string(F.col("logradouro")).alias("logradouro"),
            clean_string(F.col("numero")).alias("numero"),
            clean_string(F.col("complemento")).alias("complemento"),
            clean_string(F.col("bairro")).alias("bairro"),
            digits_only(F.col("cep"), 8).alias("cep"),
            F.upper(clean_string(F.col("uf"))).alias("uf"),
            parse_int(F.col("municipio")).alias("municipio"),
            F.concat_ws("", clean_string(F.col("ddd_1")), clean_string(F.col("telefone_1"))).alias(
                "telefone_1"
            ),
            F.concat_ws("", clean_string(F.col("ddd_2")), clean_string(F.col("telefone_2"))).alias(
                "telefone_2"
            ),
            F.lower(clean_string(F.col("correio_eletronico"))).alias("correio_eletronico"),
            clean_string(F.col("situacao_especial")).alias("situacao_especial"),
            parse_date(F.col("data_situacao_especial")).alias("data_situacao_especial"),
            "_source_file",
        )
        .filter(F.length("cnpj") == 14)
        .dropDuplicates(["cnpj"])
        # Normalize empty phone concatenations to NULL.
        .withColumn("telefone_1", clean_string(F.col("telefone_1")))
        .withColumn("telefone_2", clean_string(F.col("telefone_2")))
    )


def transform_socios(df: DataFrame) -> DataFrame:
    identificador = parse_int(F.col("identificador_socio"))
    faixa = parse_int(F.col("faixa_etaria"))
    business_key = [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cpf_cnpj_socio",
        "qualificacao_socio",
    ]
    return (
        df.select(
            digits_only(F.col("cnpj_basico"), 8).alias("cnpj_basico"),
            identificador.alias("identificador_socio"),
            map_code(identificador, IDENTIFICADOR_SOCIO).alias("tipo_socio"),
            F.upper(clean_string(F.col("nome_socio_razao_social"))).alias(
                "nome_socio_razao_social"
            ),
            # CPFs arrive masked (***123456**) by Receita: keep as published.
            clean_string(F.col("cpf_cnpj_socio")).alias("cpf_cnpj_socio"),
            parse_int(F.col("qualificacao_socio")).alias("qualificacao_socio"),
            parse_date(F.col("data_entrada_sociedade")).alias("data_entrada_sociedade"),
            parse_int(F.col("pais")).alias("pais"),
            clean_string(F.col("representante_legal")).alias("representante_legal"),
            F.upper(clean_string(F.col("nome_representante"))).alias("nome_representante"),
            parse_int(F.col("qualificacao_representante_legal")).alias(
                "qualificacao_representante_legal"
            ),
            faixa.alias("faixa_etaria"),
            map_code(faixa, FAIXA_ETARIA).alias("faixa_etaria_descricao"),
            "_source_file",
        )
        .filter(F.col("cnpj_basico").isNotNull())
        .dropDuplicates(business_key)
    )


def transform_cnaes(df: DataFrame) -> DataFrame:
    return (
        df.select(
            digits_only(F.col("codigo"), 7).alias("cnae"),
            clean_string(F.col("descricao")).alias("cnae_descricao"),
            "_source_file",
        )
        .filter(F.col("cnae").isNotNull())
        .dropDuplicates(["cnae"])
    )


TRANSFORMS: Dict[str, Callable[[DataFrame], DataFrame]] = {
    EMPRESAS.name: transform_empresas,
    ESTABELECIMENTOS.name: transform_estabelecimentos,
    SOCIOS.name: transform_socios,
    CNAES.name: transform_cnaes,
}

PRIMARY_KEYS: Dict[str, list] = {
    EMPRESAS.name: ["cnpj_basico"],
    ESTABELECIMENTOS.name: ["cnpj"],
    SOCIOS.name: [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cpf_cnpj_socio",
        "qualificacao_socio",
    ],
    CNAES.name: ["cnae"],
}


def write_delta(df: DataFrame, path: str, partition_by=()) -> None:
    """Full-snapshot overwrite. Each Receita release is complete, not incremental;
    Delta time travel keeps previous releases queryable (``VERSION AS OF``)."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
