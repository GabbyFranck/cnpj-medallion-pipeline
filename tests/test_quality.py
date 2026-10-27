import pytest

from cnpj_pipeline.quality import checks


@pytest.fixture
def df(spark):
    return spark.createDataFrame([("1", "SP"), ("2", "XX"), ("2", None)], "id string, uf string")


def test_not_empty(spark, df):
    assert checks.check_not_empty(df, "t").passed
    assert not checks.check_not_empty(df.limit(0), "t").passed


def test_not_null_reports_offending_columns(df):
    result = checks.check_not_null(df, "t", ["id", "uf"])
    assert not result.passed
    assert "'uf': 1" in result.detail


def test_unique(df):
    assert not checks.check_unique(df, "t", ["id"]).passed
    assert checks.check_unique(df, "t", ["id", "uf"]).passed


def test_accepted_values_ignores_nulls_and_defaults_to_warning(df):
    result = checks.check_accepted_values(df, "t", "uf", ["SP"])
    assert not result.passed
    assert result.severity == checks.WARN
    assert result.detail == "1 rows"


def test_ratio():
    assert checks.check_ratio("r", 1, 1000, 0.001).passed
    assert not checks.check_ratio("r", 2, 1000, 0.001).passed
    assert checks.check_ratio("r", 0, 0, 0.0).passed


def test_enforce_raises_only_on_errors():
    ok = checks.CheckResult("ok", True, "")
    warning = checks.CheckResult("w", False, "", checks.WARN)
    error = checks.CheckResult("e", False, "")

    assert checks.enforce([ok, warning]) == [ok, warning]
    with pytest.raises(checks.DataQualityError, match="e"):
        checks.enforce([ok, warning, error])
