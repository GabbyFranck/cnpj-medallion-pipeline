from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from cnpj_pipeline.schemas import SITUACAO_CADASTRAL
from cnpj_pipeline.transform import parsing


def apply(spark, fn, values):
    df = spark.createDataFrame([(v,) for v in values], "value string")
    return [row[0] for row in df.select(fn(F.col("value"))).collect()]


def test_clean_string(spark):
    assert apply(spark, parsing.clean_string, ["  abc ", "   ", "", None]) == [
        "abc",
        None,
        None,
        None,
    ]


def test_digits_only_pads_and_strips(spark):
    fn = lambda c: parsing.digits_only(c, 8)  # noqa: E731
    assert apply(spark, fn, ["191", "33.000.167", "", None]) == ["00000191", "33000167", None, None]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("20240115", date(2024, 1, 15)),
        (" 19991231 ", date(1999, 12, 31)),
        ("00000000", None),
        ("0", None),
        ("", None),
        (None, None),
        ("20230231", None),
        ("2024-01-15", None),
    ],
)
def test_parse_date(spark, raw, expected):
    assert apply(spark, parsing.parse_date, [raw]) == [expected]


def test_parse_decimal_br(spark):
    assert apply(spark, parsing.parse_decimal_br, ["1000,00", "1.234.567,89", "0,5", "", None]) == [
        Decimal("1000.00"),
        Decimal("1234567.89"),
        Decimal("0.50"),
        None,
        None,
    ]


def test_map_code(spark):
    fn = lambda c: parsing.map_code(parsing.parse_int(c), SITUACAO_CADASTRAL)  # noqa: E731
    assert apply(spark, fn, ["02", "8", "99", None]) == ["ATIVA", "BAIXADA", None, None]


def test_split_codes(spark):
    fn = lambda c: parsing.split_codes(c, 7)  # noqa: E731
    assert apply(spark, fn, ["4711302,4712100", "", None, "123456,"]) == [
        ["4711302", "4712100"],
        [],
        [],
        ["0123456"],
    ]
