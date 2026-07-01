"""Tests for Spark-compatible string functions: find_in_set, overlay, url_encode, url_decode."""

from __future__ import annotations

import pytest

import daft
from daft import col, lit
from daft.functions import (
    find_in_set,
    levenshtein_distance,
    overlay,
    url_decode,
    url_encode,
)


class TestFindInSet:
    def test_basic(self):
        df = daft.from_pydict(
            {
                "x": ["ab", "a", "c", "d"],
                "y": ["abc,b,ab,c", "a,b,c", "a,b,c", "a,b,c"],
            }
        )
        result = df.with_column("idx", find_in_set(col("x"), col("y"))).collect()
        assert result.to_pydict()["idx"] == [3, 1, 3, 0]

    def test_needle_with_comma_returns_zero(self):
        df = daft.from_pydict({"x": ["a,b"], "y": ["a,b,c"]})
        result = df.with_column("idx", find_in_set(col("x"), col("y"))).collect()
        assert result.to_pydict()["idx"] == [0]

    def test_empty_needle_matches_empty_part(self):
        df = daft.from_pydict({"x": ["", ""], "y": ["a,,b", "abc"]})
        result = df.with_column("idx", find_in_set(col("x"), col("y"))).collect()
        # Empty needle matches the empty part at index 2; on "abc" with no commas
        # the only "part" is "abc" itself, so empty does not match -> 0.
        assert result.to_pydict()["idx"] == [2, 0]

    def test_null_handling(self):
        df = daft.from_pydict({"x": ["a", None, "a"], "y": ["a,b", "a,b", None]})
        result = df.with_column("idx", find_in_set(col("x"), col("y"))).collect()
        idx = result.to_pydict()["idx"]
        assert idx[0] == 1
        assert idx[1] is None
        assert idx[2] is None

    def test_broadcast_scalar(self):
        df = daft.from_pydict({"y": ["x,y,z", "a,b,c", "p,q,r"]})
        result = df.with_column("idx", find_in_set(lit("b"), col("y"))).collect()
        assert result.to_pydict()["idx"] == [0, 2, 0]


class TestOverlay:
    def test_spark_examples(self):
        df = daft.from_pydict({"x": ["Spark SQL"]})
        result = (
            df.with_column("a", overlay(col("x"), lit("_"), 6))
            .with_column("b", overlay(col("x"), lit("CORE"), 7))
            .with_column("c", overlay(col("x"), lit("ANSI "), 7, 0))
            .with_column("d", overlay(col("x"), lit("tructured"), 2, 4))
            .collect()
            .to_pydict()
        )
        assert result["a"] == ["Spark_SQL"]
        assert result["b"] == ["Spark CORE"]
        assert result["c"] == ["Spark ANSI SQL"]
        assert result["d"] == ["Structured SQL"]

    def test_pos_clamping(self):
        df = daft.from_pydict({"x": ["abcdef"]})
        result = (
            df.with_column("a", overlay(col("x"), lit("X"), 0, 1))
            .with_column("b", overlay(col("x"), lit("X"), -3, 2))
            .collect()
            .to_pydict()
        )
        assert result["a"] == ["Xbcdef"]
        assert result["b"] == ["Xcdef"]

    def test_negative_length_uses_replace_length(self):
        df = daft.from_pydict({"x": ["abcdef"]})
        result = df.with_column("a", overlay(col("x"), lit("XYZ"), 2, -1)).collect()
        assert result.to_pydict()["a"] == ["aXYZef"]

    def test_pos_beyond_appends(self):
        df = daft.from_pydict({"x": ["abc"]})
        result = df.with_column("a", overlay(col("x"), lit("XYZ"), 100)).collect()
        assert result.to_pydict()["a"] == ["abcXYZ"]

    def test_null_propagation(self):
        df = daft.from_pydict({"x": [None, "abc"], "r": ["XX", None]})
        result = df.with_column("a", overlay(col("x"), col("r"), 1, 1)).collect()
        out = result.to_pydict()["a"]
        assert out[0] is None
        assert out[1] is None

    def test_unicode(self):
        df = daft.from_pydict({"x": ["αβγδε"]})
        result = df.with_column("a", overlay(col("x"), lit("X"), 3, 1)).collect()
        assert result.to_pydict()["a"] == ["αβXδε"]


class TestUrlEncode:
    def test_basic(self):
        df = daft.from_pydict({"x": ["Spark SQL", "https://daft.ai", "hello"]})
        result = df.with_column("e", url_encode(col("x"))).collect()
        assert result.to_pydict()["e"] == [
            "Spark+SQL",
            "https%3A%2F%2Fdaft.ai",
            "hello",
        ]

    def test_unicode(self):
        df = daft.from_pydict({"x": ["中"]})
        result = df.with_column("e", url_encode(col("x"))).collect()
        assert result.to_pydict()["e"] == ["%E4%B8%AD"]

    def test_unreserved_passthrough(self):
        df = daft.from_pydict({"x": ["a-b_c.d*e"]})
        result = df.with_column("e", url_encode(col("x"))).collect()
        assert result.to_pydict()["e"] == ["a-b_c.d*e"]

    def test_null(self):
        df = daft.from_pydict({"x": ["a", None]})
        result = df.with_column("e", url_encode(col("x"))).collect()
        out = result.to_pydict()["e"]
        assert out[0] == "a"
        assert out[1] is None


class TestUrlDecode:
    def test_basic(self):
        df = daft.from_pydict(
            {
                "x": ["Spark+SQL", "https%3A%2F%2Fdaft.ai", "%E4%B8%AD"],
            }
        )
        result = df.with_column("d", url_decode(col("x"))).collect()
        assert result.to_pydict()["d"] == [
            "Spark SQL",
            "https://daft.ai",
            "中",
        ]

    def test_round_trip(self):
        cases = ["hello world", "a/b?c=1&d=2", "中文 测试", "x+y=z"]
        df = daft.from_pydict({"x": cases})
        result = df.with_column("d", url_decode(url_encode(col("x")))).collect()
        assert result.to_pydict()["d"] == cases

    def test_invalid_raises(self):
        df = daft.from_pydict({"x": ["%ZZ"]})
        with pytest.raises(Exception):
            df.with_column("d", url_decode(col("x"))).collect()


class TestLevenshteinAlias:
    """Verify that the Spark-style ``levenshtein`` alias resolves via SQL."""

    def test_sql_alias(self):
        df = daft.from_pydict({"a": ["kitten", "abc"], "b": ["sitting", "abc"]})
        # Both names should produce identical results.
        ref = df.with_column("d", levenshtein_distance(col("a"), col("b"))).collect()
        sql = daft.sql("SELECT a, b, levenshtein(a, b) AS d FROM df").collect()
        assert sql.to_pydict()["d"] == ref.to_pydict()["d"]
