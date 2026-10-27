"""Task entry points called by the Airflow DAG.

Each function is idempotent for a given ``release`` (ISO date string), reads its
configuration from the environment and can also be run outside Airflow, e.g.
``python -c "from cnpj_pipeline import tasks; tasks.build_gold('2026-10-13')"``.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import functions as F

from cnpj_pipeline import storage
from cnpj_pipeline.config import Settings
from cnpj_pipeline.ingestion import release as release_api
from cnpj_pipeline.ingestion.bronze import extract_single_member
from cnpj_pipeline.ingestion.download import build_http_session, download_file
from cnpj_pipeline.logging_utils import get_logger
from cnpj_pipeline.quality import checks
from cnpj_pipeline.schemas import (
    CNAES,
    CORRUPT_RECORD_COLUMN,
    EMPRESAS,
    ENTITIES,
    ESTABELECIMENTOS,
    SITUACAO_CADASTRAL,
    SOCIOS,
    UFS,
)
from cnpj_pipeline.spark_session import spark_session
from cnpj_pipeline.transform import gold, silver

logger = get_logger(__name__)

GOLD_TABLES = (
    "dim_empresas",
    "fact_estabelecimentos",
    "rel_empresa_socio",
    "agg_empresas_ativas_cnae_uf",
)


def _settings(settings: Optional[Settings]) -> Settings:
    return settings or Settings.from_env()


# --- Orchestration helpers --------------------------------------------------------


def resolve_release(requested: Optional[str] = None, settings: Optional[Settings] = None) -> str:
    """Release to process: DAG param > ``CNPJ_RELEASE`` env > latest on the mirror."""
    settings = _settings(settings)
    with build_http_session() as session:
        chosen = release_api.resolve_release(
            session,
            settings.source_base_url,
            requested or settings.release,
            settings.http_timeout_seconds,
        )
    logger.info("Release selected: %s", chosen)
    return chosen.isoformat()


def _as_bool(value) -> bool:
    """Airflow renders templated kwargs as strings, so ``"False"`` must not be truthy."""
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def is_release_pending(release: str, force=False, settings: Optional[Settings] = None) -> bool:
    marker = storage.control_marker_path(_settings(settings), date.fromisoformat(release))
    if _as_bool(force) or not marker.exists():
        return True
    logger.info("Release %s already published (%s); skipping run", release, marker)
    return False


def mark_release_done(release: str, settings: Optional[Settings] = None) -> str:
    settings = _settings(settings)
    marker = storage.control_marker_path(settings, date.fromisoformat(release))
    marker.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "release": release,
        "file_parts": list(settings.file_parts),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    marker.write_text(json.dumps(payload, indent=2))
    return str(marker)


# --- Raw / Bronze ------------------------------------------------------------------


def download_entity(entity: str, release: str, settings: Optional[Settings] = None) -> List[str]:
    """Download every selected zip of ``entity`` into the landing area."""
    settings = _settings(settings)
    spec = ENTITIES[entity]
    release_date = date.fromisoformat(release)
    files = release_api.source_files(
        spec, settings.source_base_url, release_date, settings.file_parts
    )
    downloaded = []
    with build_http_session() as session:
        for _, filename, url in files:
            target = storage.landing_zip_path(settings, release_date, filename)
            download_file(
                session,
                url,
                target,
                timeout=settings.http_timeout_seconds,
                chunk_bytes=settings.download_chunk_bytes,
            )
            downloaded.append(str(target))
    return downloaded


def load_bronze(entity: str, release: str, settings: Optional[Settings] = None) -> List[str]:
    """Unzip landed files into the date-partitioned Bronze table of ``entity``."""
    settings = _settings(settings)
    spec = ENTITIES[entity]
    release_date = date.fromisoformat(release)
    written = []
    for part, filename, _ in release_api.source_files(
        spec, settings.source_base_url, release_date, settings.file_parts
    ):
        zip_path = storage.landing_zip_path(settings, release_date, filename)
        if not zip_path.exists():
            raise FileNotFoundError(f"{zip_path} missing; did download_{entity} run?")
        target = storage.bronze_file_path(settings, entity, release_date, part)
        written.append(str(extract_single_member(zip_path, target)))
    return written


# --- Silver ------------------------------------------------------------------------


def build_silver(entity: str, release: str, settings: Optional[Settings] = None) -> Dict:
    settings = _settings(settings)
    spec = ENTITIES[entity]
    release_date = date.fromisoformat(release)
    source = storage.bronze_partition_uri(settings, entity, release_date)
    target = storage.table_uri(settings, storage.SILVER, entity)
    keys = silver.PRIMARY_KEYS[entity]

    with spark_session(f"silver_{entity}", settings) as spark:
        raw = silver.read_bronze_csv(spark, spec, source)
        # Spark refuses queries touching only the corrupt-record column, so we also
        # count the first business column in the same pass.
        profile = raw.agg(
            F.count(F.lit(1)).alias("total"),
            F.count(spec.columns[0]).alias("with_key"),
            F.count(CORRUPT_RECORD_COLUMN).alias("corrupt"),
        ).first()

        cleaned = silver.with_lineage(
            silver.TRANSFORMS[entity](silver.split_corrupt(raw)), release_date
        )
        silver.write_delta(cleaned, target, spec.silver_partitions)

        written = spark.read.format("delta").load(target)
        silver_rows = written.count()
        results = [
            checks.check_not_empty(written, f"silver.{entity}"),
            checks.check_ratio(
                f"silver.{entity}.corrupt_lines",
                profile["corrupt"],
                profile["total"],
                settings.max_corrupt_ratio,
            ),
            checks.check_not_null(written, f"silver.{entity}", keys[:1]),
            checks.check_unique(written, f"silver.{entity}", keys),
        ]
        if entity == ESTABELECIMENTOS.name:
            results += [
                checks.check_accepted_values(written, "silver.estabelecimentos", "uf", UFS),
                checks.check_accepted_values(
                    written, "silver.estabelecimentos", "situacao_cadastral", SITUACAO_CADASTRAL
                ),
            ]
        checks.enforce(results)

    metrics = {
        "entity": entity,
        "bronze_rows": profile["total"],
        "corrupt_rows": profile["corrupt"],
        "silver_rows": silver_rows,
        "dropped_rows": profile["total"] - profile["corrupt"] - silver_rows,
        "path": target,
    }
    logger.info("Silver %s done: %s", entity, metrics)
    return metrics


# --- Gold --------------------------------------------------------------------------


def build_gold(release: str, settings: Optional[Settings] = None) -> Dict:
    settings = _settings(settings)

    def silver_table(spark, entity):
        return spark.read.format("delta").load(storage.table_uri(settings, storage.SILVER, entity))

    def gold_path(table):
        return storage.table_uri(settings, storage.GOLD, table)

    with spark_session("gold", settings) as spark:
        empresas = silver_table(spark, EMPRESAS.name)
        estabelecimentos = silver_table(spark, ESTABELECIMENTOS.name)
        socios = silver_table(spark, SOCIOS.name)
        cnaes = silver_table(spark, CNAES.name)

        silver.write_delta(
            gold.build_dim_empresas(empresas, estabelecimentos, socios), gold_path("dim_empresas")
        )
        silver.write_delta(
            gold.build_fact_estabelecimentos(estabelecimentos, empresas, cnaes),
            gold_path("fact_estabelecimentos"),
            partition_by=("uf",),
        )
        silver.write_delta(gold.build_rel_empresa_socio(socios), gold_path("rel_empresa_socio"))

        # Aggregate from the persisted fact: no recomputation of the joins.
        fact = spark.read.format("delta").load(gold_path("fact_estabelecimentos"))
        silver.write_delta(
            gold.build_agg_empresas_ativas_cnae_uf(fact), gold_path("agg_empresas_ativas_cnae_uf")
        )

        tables = {t: spark.read.format("delta").load(gold_path(t)) for t in GOLD_TABLES}
        fact_rows = tables["fact_estabelecimentos"].count()
        orphans = tables["fact_estabelecimentos"].filter(F.col("razao_social").isNull()).count()
        results = [checks.check_not_empty(df, f"gold.{name}") for name, df in tables.items()]
        results += [
            checks.check_unique(tables["dim_empresas"], "gold.dim_empresas", ["cnpj_basico"]),
            checks.check_unique(
                tables["fact_estabelecimentos"], "gold.fact_estabelecimentos", ["cnpj"]
            ),
            checks.check_not_null(
                tables["agg_empresas_ativas_cnae_uf"],
                "gold.agg_empresas_ativas_cnae_uf",
                ["uf", "cnae_fiscal_principal"],
            ),
            # Partial loads (CNPJ_FILE_PARTS != all) naturally miss companies, so warn only.
            checks.check_ratio(
                "gold.fact_estabelecimentos.orphan_establishments",
                orphans,
                fact_rows,
                settings.max_orphan_ratio,
                severity=checks.WARN,
            ),
        ]
        checks.enforce(results)
        counts = {name: df.count() for name, df in tables.items()}

    logger.info("Gold for release %s done: %s", release, counts)
    return counts
