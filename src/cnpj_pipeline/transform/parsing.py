"""Column-level parsers for the Receita Federal text formats.

Each function takes and returns a Spark ``Column`` so it composes inside a single
``select`` and stays trivially unit-testable.
"""

from __future__ import annotations

from itertools import chain
from typing import Mapping

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType


def clean_string(col: Column) -> Column:
    """Trim and turn empty strings into NULL."""
    trimmed = F.trim(col)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def digits_only(col: Column, length: int) -> Column:
    """Keep digits and left-pad with zeros (``"191"`` -> ``"00000191"``); NULL if no digits."""
    digits = F.regexp_replace(F.coalesce(col, F.lit("")), r"[^0-9]", "")
    return F.when(digits == "", F.lit(None)).otherwise(F.lpad(digits, length, "0"))


def parse_int(col: Column) -> Column:
    return clean_string(col).cast(IntegerType())


def parse_date(col: Column) -> Column:
    """``yyyyMMdd`` -> date. ``0``, ``00000000``, blanks and impossible dates -> NULL."""
    value = F.trim(col)
    is_candidate = value.rlike(r"^[0-9]{8}$") & (value != "00000000")
    return F.when(is_candidate, F.to_date(value, "yyyyMMdd"))


def parse_decimal_br(col: Column, precision: int = 18, scale: int = 2) -> Column:
    """Brazilian number format (``1.234,56``) -> decimal."""
    normalized = F.regexp_replace(F.regexp_replace(F.trim(col), r"\.", ""), ",", ".")
    return F.when(normalized != "", normalized.cast(DecimalType(precision, scale)))


def map_code(col: Column, mapping: Mapping[int, str]) -> Column:
    """Translate an integer code column using a static code table."""
    lookup = F.create_map([F.lit(x) for x in chain.from_iterable(mapping.items())])
    return lookup[col]


def split_codes(col: Column, length: int) -> Column:
    """``"4711302,4712100"`` -> ``["4711302", "4712100"]``; NULL/blank -> empty array."""
    items = F.split(F.coalesce(F.trim(col), F.lit("")), ",")
    padded = F.transform(items, lambda c: F.lpad(F.trim(c), length, "0"))
    return F.filter(padded, lambda c: F.regexp_replace(c, "0", "") != "")
