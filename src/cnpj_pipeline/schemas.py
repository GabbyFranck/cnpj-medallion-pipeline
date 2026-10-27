"""Source layouts and code tables from the Receita Federal CNPJ data dictionary.

The source files are headerless, ``;``-separated, ``"``-quoted and ISO-8859-1 encoded,
so the column order below is the contract with the source.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from pyspark.sql.types import StringType, StructField, StructType

CORRUPT_RECORD_COLUMN = "_corrupt_record"


@dataclass(frozen=True)
class EntitySpec:
    name: str
    source_prefix: str
    columns: Tuple[str, ...]
    numbered_files: bool = True
    silver_partitions: Tuple[str, ...] = ()

    def raw_schema(self) -> StructType:
        """Everything is read as string; typing happens explicitly in Silver."""
        fields = [StructField(c, StringType(), True) for c in self.columns]
        fields.append(StructField(CORRUPT_RECORD_COLUMN, StringType(), True))
        return StructType(fields)

    def source_filename(self, part: Optional[int] = None) -> str:
        if self.numbered_files:
            return f"{self.source_prefix}{part}.zip"
        return f"{self.source_prefix}.zip"


EMPRESAS = EntitySpec(
    name="empresas",
    source_prefix="Empresas",
    columns=(
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ),
)

ESTABELECIMENTOS = EntitySpec(
    name="estabelecimentos",
    source_prefix="Estabelecimentos",
    columns=(
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ),
    silver_partitions=("uf",),
)

SOCIOS = EntitySpec(
    name="socios",
    source_prefix="Socios",
    columns=(
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cpf_cnpj_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ),
)

# Small lookup table, used by Gold to label CNAE codes.
CNAES = EntitySpec(
    name="cnaes",
    source_prefix="Cnaes",
    columns=("codigo", "descricao"),
    numbered_files=False,
)

ENTITIES: Dict[str, EntitySpec] = {
    spec.name: spec for spec in (EMPRESAS, ESTABELECIMENTOS, SOCIOS, CNAES)
}

# --- Code tables (Receita Federal "Metadados CNPJ") ---------------------------------

SITUACAO_ATIVA = 2
SITUACAO_CADASTRAL = {1: "NULA", 2: "ATIVA", 3: "SUSPENSA", 4: "INAPTA", 8: "BAIXADA"}

PORTE_EMPRESA = {
    0: "NAO INFORMADO",
    1: "MICRO EMPRESA",
    3: "EMPRESA DE PEQUENO PORTE",
    5: "DEMAIS",
}

MATRIZ_FILIAL = {1: "MATRIZ", 2: "FILIAL"}

IDENTIFICADOR_SOCIO = {1: "PESSOA JURIDICA", 2: "PESSOA FISICA", 3: "ESTRANGEIRO"}

FAIXA_ETARIA = {
    0: "NAO SE APLICA",
    1: "0 A 12 ANOS",
    2: "13 A 20 ANOS",
    3: "21 A 30 ANOS",
    4: "31 A 40 ANOS",
    5: "41 A 50 ANOS",
    6: "51 A 60 ANOS",
    7: "61 A 70 ANOS",
    8: "71 A 80 ANOS",
    9: "MAIORES DE 80 ANOS",
}

# "EX" is used by Receita for establishments located abroad.
UFS = (
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "EX",
)  # fmt: skip
