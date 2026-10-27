"""Lightweight data quality gates run between layers.

Each check returns a ``CheckResult``; ``enforce`` logs all of them and fails the task
only for ``ERROR`` severity, so warnings show up in the Airflow log without blocking.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cnpj_pipeline.logging_utils import get_logger

logger = get_logger(__name__)

ERROR = "error"
WARN = "warn"


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str
    severity: str = ERROR


def check_not_empty(df: DataFrame, table: str) -> CheckResult:
    has_rows = len(df.take(1)) > 0
    return CheckResult(f"{table}.not_empty", has_rows, "table has rows" if has_rows else "empty")


def check_not_null(df: DataFrame, table: str, columns: Sequence[str]) -> CheckResult:
    null_counts = df.select(
        [F.sum(F.col(c).isNull().cast("long")).alias(c) for c in columns]
    ).first()
    offenders = {c: null_counts[c] for c in columns if null_counts[c]}
    return CheckResult(
        f"{table}.not_null({','.join(columns)})",
        not offenders,
        f"null counts: {offenders}" if offenders else "no nulls",
    )


def check_unique(df: DataFrame, table: str, columns: Sequence[str]) -> CheckResult:
    duplicates = df.groupBy(*columns).count().filter(F.col("count") > 1).count()
    return CheckResult(
        f"{table}.unique({','.join(columns)})",
        duplicates == 0,
        f"{duplicates} duplicated keys",
    )


def check_accepted_values(
    df: DataFrame, table: str, column: str, accepted: Iterable, severity: str = WARN
) -> CheckResult:
    invalid = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(list(accepted))).count()
    return CheckResult(
        f"{table}.accepted_values({column})", invalid == 0, f"{invalid} rows", severity
    )


def check_ratio(
    name: str, numerator: int, denominator: int, max_ratio: float, severity: str = ERROR
) -> CheckResult:
    ratio = numerator / denominator if denominator else 0.0
    return CheckResult(
        name,
        ratio <= max_ratio,
        f"{numerator}/{denominator} = {ratio:.4%} (max {max_ratio:.4%})",
        severity,
    )


def enforce(results: List[CheckResult]) -> List[CheckResult]:
    failures = []
    for result in results:
        status = "PASS" if result.passed else result.severity.upper()
        log = logger.info if result.passed else logger.warning
        log("[DQ %s] %s: %s", status, result.name, result.detail)
        if not result.passed and result.severity == ERROR:
            failures.append(result)
    if failures:
        names = ", ".join(f.name for f in failures)
        raise DataQualityError(f"{len(failures)} data quality check(s) failed: {names}")
    return results
