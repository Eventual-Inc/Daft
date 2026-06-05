"""Tests for string distance/similarity functions (issue #6794)."""

from __future__ import annotations

import pytest

import daft
from daft import col, lit
from daft.functions import (
    damerau_levenshtein_distance,
    jaro_similarity,
    jaro_winkler_similarity,
    levenshtein_distance,
)


class TestLevenshteinDistance:
    def test_basic(self):
        df = daft.from_pydict({"a": ["kitten", "saturday", "abc"], "b": ["sitting", "sunday", "abc"]})
        result = df.with_column("dist", levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        assert distances == [3, 3, 0]

    def test_empty_strings(self):
        df = daft.from_pydict({"a": ["", "abc", ""], "b": ["abc", "", ""]})
        result = df.with_column("dist", levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        assert distances == [3, 3, 0]

    def test_null_handling(self):
        df = daft.from_pydict({"a": ["hello", None, "world"], "b": ["hallo", "test", None]})
        result = df.with_column("dist", levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        assert distances[0] == 1
        assert distances[1] is None
        assert distances[2] is None

    def test_identical_strings(self):
        df = daft.from_pydict({"a": ["foo", "bar", "baz"], "b": ["foo", "bar", "baz"]})
        result = df.with_column("dist", levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        assert distances == [0, 0, 0]

    def test_single_char_edits(self):
        df = daft.from_pydict(
            {
                "a": ["cat", "cat", "cat"],
                "b": ["hat", "cats", "at"],
            }
        )
        result = df.with_column("dist", levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        # substitution, insertion, deletion
        assert distances == [1, 1, 1]

    def test_expression_method(self):
        df = daft.from_pydict({"a": ["kitten"], "b": ["sitting"]})
        result = df.with_column("dist", col("a").levenshtein_distance(col("b"))).collect()
        assert result.to_pydict()["dist"] == [3]


class TestScalarBroadcast:
    """Scalar broadcasting on either side (column-scalar, scalar-column)."""

    def test_column_scalar(self):
        df = daft.from_pydict({"a": ["kitten", "sitting", "kitten"]})
        result = df.with_column("dist", levenshtein_distance(col("a"), lit("kitten"))).collect()
        assert result.to_pydict()["dist"] == [0, 3, 0]

    def test_scalar_column(self):
        df = daft.from_pydict({"a": ["kitten", "sitting", "kitten"]})
        result = df.with_column("dist", levenshtein_distance(lit("kitten"), col("a"))).collect()
        assert result.to_pydict()["dist"] == [0, 3, 0]

    def test_scalar_column_similarity(self):
        df = daft.from_pydict({"a": ["martha", "martha"]})
        result = df.with_column("sim", jaro_similarity(lit("marhta"), col("a"))).collect()
        sims = result.to_pydict()["sim"]
        assert sims[0] == pytest.approx(0.944444, rel=1e-4)
        assert sims[1] == pytest.approx(0.944444, rel=1e-4)

    def test_column_scalar_null_scalar(self):
        df = daft.from_pydict({"a": ["kitten", "sitting"]})
        result = df.with_column("dist", levenshtein_distance(col("a"), lit(None))).collect()
        assert result.to_pydict()["dist"] == [None, None]


class TestJaroSimilarity:
    def test_identical(self):
        df = daft.from_pydict({"a": ["hello", ""], "b": ["hello", ""]})
        result = df.with_column("sim", jaro_similarity(col("a"), col("b"))).collect()
        sims = result.to_pydict()["sim"]
        assert sims[0] == pytest.approx(1.0)
        assert sims[1] == pytest.approx(1.0)

    def test_completely_different(self):
        df = daft.from_pydict({"a": ["abc"], "b": ["xyz"]})
        result = df.with_column("sim", jaro_similarity(col("a"), col("b"))).collect()
        assert result.to_pydict()["sim"][0] == pytest.approx(0.0)

    def test_known_values(self):
        # Well-known test case: martha vs marhta -> 0.944444
        df = daft.from_pydict({"a": ["martha"], "b": ["marhta"]})
        result = df.with_column("sim", jaro_similarity(col("a"), col("b"))).collect()
        assert result.to_pydict()["sim"][0] == pytest.approx(0.944444, rel=1e-4)

    def test_null_handling(self):
        df = daft.from_pydict({"a": ["hello", None], "b": [None, "world"]})
        result = df.with_column("sim", jaro_similarity(col("a"), col("b"))).collect()
        sims = result.to_pydict()["sim"]
        assert sims[0] is None
        assert sims[1] is None

    def test_empty_vs_nonempty(self):
        df = daft.from_pydict({"a": ["", "abc"], "b": ["abc", ""]})
        result = df.with_column("sim", jaro_similarity(col("a"), col("b"))).collect()
        sims = result.to_pydict()["sim"]
        assert sims[0] == pytest.approx(0.0)
        assert sims[1] == pytest.approx(0.0)

    def test_expression_method(self):
        df = daft.from_pydict({"a": ["martha"], "b": ["marhta"]})
        result = df.with_column("sim", col("a").jaro_similarity(col("b"))).collect()
        assert result.to_pydict()["sim"][0] == pytest.approx(0.944444, rel=1e-4)


class TestJaroWinklerSimilarity:
    def test_identical(self):
        df = daft.from_pydict({"a": ["hello"], "b": ["hello"]})
        result = df.with_column("sim", jaro_winkler_similarity(col("a"), col("b"))).collect()
        assert result.to_pydict()["sim"][0] == pytest.approx(1.0)

    def test_prefix_bonus(self):
        # Jaro-Winkler should be >= Jaro for strings sharing a prefix
        df = daft.from_pydict({"a": ["martha"], "b": ["marhta"]})
        jaro_result = df.with_column("sim", jaro_similarity(col("a"), col("b"))).collect()
        jw_result = df.with_column("sim", jaro_winkler_similarity(col("a"), col("b"))).collect()
        jaro_val = jaro_result.to_pydict()["sim"][0]
        jw_val = jw_result.to_pydict()["sim"][0]
        assert jw_val >= jaro_val

    def test_known_values(self):
        # martha vs marhta: Jaro = 0.944444, prefix "mar" (len=3)
        # JW = 0.944444 + (3 * 0.1 * (1 - 0.944444)) = 0.961111
        df = daft.from_pydict({"a": ["martha"], "b": ["marhta"]})
        result = df.with_column("sim", jaro_winkler_similarity(col("a"), col("b"))).collect()
        assert result.to_pydict()["sim"][0] == pytest.approx(0.961111, rel=1e-4)

    def test_no_common_prefix(self):
        # No common prefix means JW == Jaro
        df = daft.from_pydict({"a": ["abc"], "b": ["xyz"]})
        jaro_result = df.with_column("sim", jaro_similarity(col("a"), col("b"))).collect()
        jw_result = df.with_column("sim", jaro_winkler_similarity(col("a"), col("b"))).collect()
        assert jw_result.to_pydict()["sim"][0] == pytest.approx(jaro_result.to_pydict()["sim"][0])

    def test_null_handling(self):
        df = daft.from_pydict({"a": ["hello", None], "b": [None, "world"]})
        result = df.with_column("sim", jaro_winkler_similarity(col("a"), col("b"))).collect()
        sims = result.to_pydict()["sim"]
        assert sims[0] is None
        assert sims[1] is None

    def test_expression_method(self):
        df = daft.from_pydict({"a": ["martha"], "b": ["marhta"]})
        result = df.with_column("sim", col("a").jaro_winkler_similarity(col("b"))).collect()
        assert result.to_pydict()["sim"][0] == pytest.approx(0.961111, rel=1e-4)


class TestDamerauLevenshteinDistance:
    def test_basic(self):
        df = daft.from_pydict({"a": ["abc", "abc"], "b": ["bac", "abc"]})
        result = df.with_column("dist", damerau_levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        assert distances[0] == 1  # abc -> bac: single transposition
        assert distances[1] == 0  # identical

    def test_transposition_vs_levenshtein(self):
        # "ab" -> "ba" should be 1 for Damerau-Levenshtein (transposition)
        # but 2 for standard Levenshtein (two substitutions)
        df = daft.from_pydict({"a": ["ab"], "b": ["ba"]})
        dl_result = df.with_column("dist", damerau_levenshtein_distance(col("a"), col("b"))).collect()
        lev_result = df.with_column("dist", levenshtein_distance(col("a"), col("b"))).collect()
        assert dl_result.to_pydict()["dist"][0] == 1
        assert lev_result.to_pydict()["dist"][0] == 2

    def test_empty_strings(self):
        df = daft.from_pydict({"a": ["", "abc", ""], "b": ["abc", "", ""]})
        result = df.with_column("dist", damerau_levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        assert distances == [3, 3, 0]

    def test_identical(self):
        df = daft.from_pydict({"a": ["hello"], "b": ["hello"]})
        result = df.with_column("dist", damerau_levenshtein_distance(col("a"), col("b"))).collect()
        assert result.to_pydict()["dist"][0] == 0

    def test_null_handling(self):
        df = daft.from_pydict({"a": ["hello", None], "b": [None, "world"]})
        result = df.with_column("dist", damerau_levenshtein_distance(col("a"), col("b"))).collect()
        distances = result.to_pydict()["dist"]
        assert distances[0] is None
        assert distances[1] is None

    def test_expression_method(self):
        df = daft.from_pydict({"a": ["abc"], "b": ["bac"]})
        result = df.with_column("dist", col("a").damerau_levenshtein_distance(col("b"))).collect()
        assert result.to_pydict()["dist"] == [1]
