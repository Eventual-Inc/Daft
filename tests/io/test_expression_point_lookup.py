from __future__ import annotations

from daft import col
from daft.io.lance.point_lookup import detect_point_lookup_columns
from daft.logical.schema import DataType


def _cols(expr):
    return detect_point_lookup_columns([expr])


def test_equal_simple_point_lookup():
    assert _cols(col("id") == 2) == ["id"]


def test_cast_not_supported():
    # Currently cast is treated conservatively and not considered point-lookup
    assert _cols(col("id").cast(DataType.int64()) == 2) == []


def test_in_literal_list():
    assert _cols(col("id").is_in([1, 2])) == ["id"]


def test_in_non_literal_or_empty_list_returns_empty():
    assert _cols(col("id").is_in([])) == []
    assert _cols(col("id").is_in([col("id")])) == []


def test_or_and_not_are_not_supported():
    assert _cols((col("id") == 1) | (col("id") == 2)) == []
    assert _cols(~(col("id") == 1)) == []
    assert _cols(col("id") != 1) == []


def test_between_and_ranges_are_not_supported():
    assert _cols(col("id").between(1, 3)) == []
    assert _cols(col("id") > 1) == []
    assert _cols((col("id") == 2) & (col("id") > 1)) == []


def test_multi_column_and_simple_and():
    # simple conjunction combines multiple columns
    assert _cols((col("id") == 2) & (col("value") == "b")) == ["id", "value"]
