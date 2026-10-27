from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from cnpj_pipeline.config import Settings


def build_spark(app_name: str, settings: Settings) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        # Invalid source dates (e.g. 20230231) become NULL instead of failing the job.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.ansi.enabled", "false")
        # With CSV column pruning, lines with a wrong field count are only flagged as
        # corrupt when every column is selected; we need consistent detection for DQ.
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@contextmanager
def spark_session(app_name: str, settings: Settings) -> Iterator[SparkSession]:
    """One short-lived local session per Airflow task; always released on exit."""
    spark = build_spark(app_name, settings)
    try:
        yield spark
    finally:
        spark.stop()
