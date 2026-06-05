"""Correctness tests for the sorted aligned asof join operator.

Verbatim copy of test_asof_join.py with _assume_sorted_and_aligned=True added
to every join_asof call. Runs under the native runner only.
"""

from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

import daft
from daft import col
from daft.api_annotations import APITypeError
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="AsofJoinNoSortNoGroupOperator is tested via native runner",
)


def get_n_partitions():
    """Returns the number of partitions to test."""
    return [1]


# ---------------------------------------------------------------------------
# 1. Parameter Validation
# ---------------------------------------------------------------------------


class TestAsofJoinParameterValidation:
    @pytest.mark.parametrize("extra_kwarg", [{"left_on": "ts"}, {"right_on": "ts"}])
    def test_on_with_left_on_or_right_on_raises(self, extra_kwarg):
        left = daft.from_pydict({"ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"ts": [1, 2], "w": [30, 40]})
        with pytest.raises(ValueError, match="`on` is set"):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", **extra_kwarg)

    def test_left_on_without_right_on_raises(self):
        left = daft.from_pydict({"ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"ts": [1, 2], "w": [30, 40]})
        with pytest.raises(ValueError, match="both `left_on` and `right_on` must be set"):
            left.join_asof(right, _assume_sorted_and_aligned=True, left_on="ts")

    def test_right_on_without_left_on_raises(self):
        left = daft.from_pydict({"ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"ts": [1, 2], "w": [30, 40]})
        with pytest.raises(ValueError, match="both `left_on` and `right_on` must be set"):
            left.join_asof(right, _assume_sorted_and_aligned=True, right_on="ts")

    def test_by_with_left_by_raises(self):
        left = daft.from_pydict({"ts": [1, 2], "g": ["a", "b"]})
        right = daft.from_pydict({"ts": [1, 2], "g": ["a", "b"]})
        with pytest.raises(ValueError, match="Cannot specify both"):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g", left_by="g", right_by="g")

    def test_left_by_without_right_by_raises(self):
        left = daft.from_pydict({"ts": [1], "g": ["a"]})
        right = daft.from_pydict({"ts": [1], "h": ["a"]})
        with pytest.raises(ValueError, match="Specify both"):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", left_by="g")

    def test_right_by_without_left_by_raises(self):
        left = daft.from_pydict({"ts": [1], "g": ["a"]})
        right = daft.from_pydict({"ts": [1], "h": ["a"]})
        with pytest.raises(ValueError, match="Specify both"):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", right_by="h")

    def test_left_by_right_by_length_mismatch_raises(self):
        left = daft.from_pydict({"ts": [1], "g1": ["a"], "g2": ["b"]})
        right = daft.from_pydict({"ts": [1], "h1": ["a"]})
        with pytest.raises(ValueError, match="same number of columns"):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", left_by=["g1", "g2"], right_by=["h1"])

    def test_no_keys_raises(self):
        left = daft.from_pydict({"ts": [1, 2]})
        right = daft.from_pydict({"ts": [1, 2]})
        with pytest.raises(ValueError):
            left.join_asof(right)

    # remove this test once we support more strategies
    def test_invalid_strategy_raises(self):
        left = daft.from_pydict({"ts": [1, 2]})
        right = daft.from_pydict({"ts": [1, 2]})
        with pytest.raises(APITypeError):
            left.join_asof(right, _assume_sorted_and_aligned=True, strategy="invalid_strategy")


# ---------------------------------------------------------------------------
# 2. Key Permutations
# ---------------------------------------------------------------------------


class TestAsofJoinKeyPermutations:
    """Every valid combination of on/left_on/right_on/by/left_by/right_by.

    Produces a result with correct schema and values.
    """

    def test_on_only(self):
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 20, 30], "w": [11, 22, 33]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    def test_on_with_single_by(self):
        left = daft.from_pydict({"entity": ["X", "X", "Y"], "ts": [100, 110, 100], "v": [7, 8, 9]})
        right = daft.from_pydict(
            {"entity": ["X", "X", "X", "Y", "Y"], "ts": [90, 100, 110, 90, 110], "w": [1.1, 2.2, 3.3, 4.4, 5.5]}
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort(["entity", "ts"])
        assert result.column_names == ["entity", "ts", "v", "w"]
        assert result.to_pydict() == {
            "entity": ["X", "X", "Y"],
            "ts": [100, 110, 100],
            "v": [7, 8, 9],
            "w": [2.2, 3.3, 4.4],
        }

    def test_on_with_multiple_by(self):
        left = daft.from_pydict({"g1": ["P", "P"], "g2": [3, 3], "ts": [50, 80], "v": [11, 22]})
        right = daft.from_pydict({"g1": ["P", "P", "P"], "g2": [3, 3, 7], "ts": [40, 70, 70], "w": [5, 9, 15]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by=["g1", "g2"]).sort("ts")
        assert result.column_names == ["g1", "g2", "ts", "v", "w"]
        assert result.to_pydict() == {
            "g1": ["P", "P"],
            "g2": [3, 3],
            "ts": [50, 80],
            "v": [11, 22],
            "w": [5, 9],
        }

    def test_left_on_right_on(self):
        left = daft.from_pydict({"ts_left": [5, 10, 15], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts_right": [3, 8, 12], "w": [100, 200, 300]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, left_on="ts_left", right_on="ts_right").sort(
            "ts_left"
        )
        assert result.column_names == ["ts_left", "v", "ts_right", "w"]
        assert result.to_pydict() == {
            "ts_left": [5, 10, 15],
            "v": [1, 2, 3],
            "ts_right": [3, 8, 12],
            "w": [100, 200, 300],
        }

    def test_left_on_right_on_with_left_by_right_by(self):
        left = daft.from_pydict({"grp_l": ["M", "N"], "ts_l": [50, 50], "v": [7, 8]})
        right = daft.from_pydict({"grp_r": ["M", "N"], "ts_r": [30, 40], "w": [70, 80]})
        result = left.join_asof(
            right, _assume_sorted_and_aligned=True, left_on="ts_l", right_on="ts_r", left_by="grp_l", right_by="grp_r"
        ).sort("grp_l")
        assert result.column_names == ["grp_l", "ts_l", "v", "ts_r", "w"]
        assert result.to_pydict() == {
            "grp_l": ["M", "N"],
            "ts_l": [50, 50],
            "v": [7, 8],
            "ts_r": [30, 40],
            "w": [70, 80],
        }

    def test_on_with_left_by_right_by(self):
        left = daft.from_pydict({"grp_l": ["M", "N"], "ts": [50, 50], "v": [7, 8]})
        right = daft.from_pydict({"grp_r": ["M", "N"], "ts": [30, 40], "w": [70, 80]})
        result = left.join_asof(
            right, _assume_sorted_and_aligned=True, on="ts", left_by="grp_l", right_by="grp_r"
        ).sort("grp_l")
        assert result.column_names == ["grp_l", "ts", "v", "w"]
        assert result.to_pydict() == {
            "grp_l": ["M", "N"],
            "ts": [50, 50],
            "v": [7, 8],
            "w": [70, 80],
        }

    def test_single_by_as_string_vs_list(self):
        left = daft.from_pydict({"entity": ["X", "X"], "ts": [5, 10], "v": [3, 6]})
        right = daft.from_pydict({"entity": ["X", "X"], "ts": [5, 10], "w": [30, 60]})
        result_str = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort("ts").to_pydict()
        result_list = (
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by=["entity"]).sort("ts").to_pydict()
        )
        assert result_str == result_list

    def test_two_left_by_right_by_different_names(self):
        """Multiple by keys with different column names on left and right sides."""
        left = daft.from_pydict(
            {
                "g1_l": ["A", "A", "B", "B"],
                "g2_l": [1, 1, 2, 2],
                "ts": [10, 20, 10, 20],
                "v": [1, 2, 3, 4],
            }
        )
        right = daft.from_pydict(
            {
                "g1_r": ["A", "A", "B", "B"],
                "g2_r": [1, 1, 2, 2],
                "ts": [5, 15, 8, 18],
                "w": [50, 150, 80, 180],
            }
        )
        result = left.join_asof(
            right,
            on="ts",
            left_by=["g1_l", "g2_l"],
            right_by=["g1_r", "g2_r"],
        ).sort(["g1_l", "ts"])
        assert result.column_names == ["g1_l", "g2_l", "ts", "v", "w"]
        assert result.to_pydict() == {
            "g1_l": ["A", "A", "B", "B"],
            "g2_l": [1, 1, 2, 2],
            "ts": [10, 20, 10, 20],
            "v": [1, 2, 3, 4],
            # (A,1)@10 -> right (A,1)@5=50;  (A,1)@20 -> right (A,1)@15=150
            # (B,2)@10 -> right (B,2)@8=80;  (B,2)@20 -> right (B,2)@18=180
            "w": [50, 150, 80, 180],
        }


# ---------------------------------------------------------------------------
# 3. Backward Match Correctness
# ---------------------------------------------------------------------------


class TestAsofJoinBackwardMatchCorrectness:
    def test_exact_match(self):
        """When right has exact timestamp, pick it."""
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 20, 30], "w": [11, 22, 33]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="backward").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    def test_closest_earlier_when_no_exact(self):
        """Picks the closest right value that is <= left value."""
        left = daft.from_pydict({"a": [1.0, 2.0, 3.0], "b": ["p", "q", "r"]})
        right = daft.from_pydict({"a": [0.4, 1.6, 2.7], "c": ["s1", "s2", "s3"]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="a").sort("a")
        assert result.to_pydict() == {
            "a": [1.0, 2.0, 3.0],
            "b": ["p", "q", "r"],
            "c": ["s1", "s2", "s3"],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_no_right_before_left_returns_null(self, make_df, n_partitions, with_default_morsel_size):
        """Left rows before all right rows get null."""
        left = make_df({"ts": [2, 6, 12], "v": [10, 20, 30]}, repartition=n_partitions)
        right = make_df({"ts": [4, 9], "w": [40, 90]}, repartition=n_partitions)
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {"ts": [2, 6, 12], "v": [10, 20, 30], "w": [None, 40, 90]}

    def test_all_left_before_all_right_all_nulls(self):
        """Every left row is before all right rows -- all nulls."""
        left = daft.from_pydict({"ts": [1, 2, 3], "v": [10, 20, 30]})
        right = daft.from_pydict({"ts": [100, 200], "w": [11, 22]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [None, None, None]}

    def test_all_right_before_all_left_match_last(self):
        """Every right row is before all left rows -- each left matches the last right row."""
        left = daft.from_pydict({"ts": [100, 200, 300], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 50], "w": [11, 55]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {"ts": [100, 200, 300], "v": [1, 2, 3], "w": [55, 55, 55]}

    def test_duplicate_left_timestamps(self):
        """Duplicate left timestamps each match independently."""
        left = daft.from_pydict({"ts": [6, 6, 11], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [4, 8], "w": [40, 80]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort(["ts", "v"])
        assert result.to_pydict() == {"ts": [6, 6, 11], "v": [1, 2, 3], "w": [40, 40, 80]}

    def test_duplicate_right_timestamps(self):
        """Multiple right rows at same timestamp -- one is picked."""
        left = daft.from_pydict({"ts": [7], "v": [1]})
        right = daft.from_pydict({"ts": [7, 7], "w": [70, 77]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts")
        assert result.to_pydict()["ts"] == [7]
        assert result.to_pydict()["v"] == [1]
        assert result.to_pydict()["w"][0] in [70, 77]

    def test_float_asof_key_with_by(self):
        """Float asof key with group-by column."""
        left = daft.from_pydict(
            {
                "b": [0.0, 1.25, 2.5, 3.75, 5.0, 6.25, 7.5],
                "c": ["x", "x", "x", "x", "y", "y", "y"],
            }
        )
        right = daft.from_pydict(
            {
                "val": [0.0, 3.0, 3.1, 3.2, 4.5, 5.5, 7.5],
                "c": ["x", "x", "x", "y", "y", "y", "y"],
                "b": [0.0, 3.0, 3.1, 3.2, 4.5, 5.5, 7.5],
            }
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="b", by="c").sort(["c", "b"])
        assert result.column_names == ["b", "c", "val"]
        assert result.to_pydict() == {
            "b": [0.0, 1.25, 2.5, 3.75, 5.0, 6.25, 7.5],
            "c": ["x", "x", "x", "x", "y", "y", "y"],
            "val": [0.0, 0.0, 0.0, 3.1, 4.5, 5.5, 7.5],
        }

    def test_null_in_asof_key_left_produces_null(self):
        """Null asof keys on left should produce null matches on the right side."""
        left = daft.from_pydict({"ts": [None, 2, None], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [1, 2, 3], "w": [10, 20, 30]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("v")
        pydict = result.to_pydict()
        assert pydict["v"] == [1, 2, 3]
        assert pydict["w"][0] is None
        assert pydict["w"][1] == 20
        assert pydict["w"][2] is None

    def test_null_in_by_key_no_cross_match(self):
        """Null by-keys do not match each other."""
        left = daft.from_pydict({"g": ["X", None], "ts": [5, 5], "v": [1, 2]})
        right = daft.from_pydict({"g": ["X", None], "ts": [3, 3], "w": [10, 20]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g").sort("v")
        assert result.column_names == ["g", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [10, None]

    def test_sandwiched_left_row_forward_filled_with_by(self):
        """A left row with no direct match is forward filled from its left neighbour within the same group."""
        left = daft.from_pydict({"entity": ["A", "A", "A"], "ts": [3, 7, 10], "v": [1, 2, 3]})
        right = daft.from_pydict({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort("ts")
        assert result.to_pydict() == {
            "entity": ["A", "A", "A"],
            "ts": [3, 7, 10],
            "v": [1, 2, 3],
            "w": [20, 20, 80],
        }

    def test_forward_fill_does_not_cross_group_boundaries(self):
        """Forward fill within one group must not propagate into a different group."""
        left = daft.from_pydict(
            {
                "entity": ["A", "A", "A", "B", "B", "B"],
                "ts": [3, 7, 10, 15, 20, 25],
                "v": [1, 2, 3, 4, 5, 6],
            }
        )
        right = daft.from_pydict({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [3, 7, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5, 6],
            "w": [20, 20, 80, None, None, None],
        }


# ---------------------------------------------------------------------------
# 4. Empty Table Edge Cases
# ---------------------------------------------------------------------------


class TestAsofJoinEmptyTables:
    def test_empty_left_table(self):
        """Empty left table produces an empty result."""
        left = daft.from_pydict({"ts": [], "v": []})
        right = daft.from_pydict({"ts": [1, 2, 3], "w": [10, 20, 30]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}

    def test_empty_right_table(self):
        """Empty right table produces nulls for all right columns."""
        left = daft.from_pydict({"ts": [1, 2, 3], "v": [10, 20, 30]})
        right = daft.from_pydict({"ts": [], "w": []})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [None, None, None]}

    def test_empty_left_and_right_tables(self):
        """Both tables empty produces an empty result."""
        left = daft.from_pydict({"ts": [], "v": []})
        right = daft.from_pydict({"ts": [], "w": []})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}


# ---------------------------------------------------------------------------
# 5. Column Handling
# ---------------------------------------------------------------------------


class TestAsofJoinColumnHandling:
    def test_on_coalesces_asof_key(self):
        """Using on= should not duplicate the asof key column."""
        left = daft.from_pydict({"ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"ts": [1, 2], "w": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts")
        ts_cols = [c for c in result.column_names if "ts" in c]
        assert ts_cols == ["ts"]

    def test_by_coalesces_by_key(self):
        """Using by= should not duplicate the by key column."""
        left = daft.from_pydict({"g": ["A", "B"], "ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"g": ["A", "B"], "ts": [1, 2], "w": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g")
        g_cols = [c for c in result.column_names if "g" in c]
        assert g_cols == ["g"]

    def test_payload_clash_gets_right_prefix(self):
        """Right non-key column that clashes with left gets default right. prefix."""
        left = daft.from_pydict({"ts": [1, 2, 3], "price": [10, 20, 30]})
        right = daft.from_pydict({"ts": [1, 2, 3], "price": [100, 200, 300], "bid": [1, 2, 3]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts")
        assert result.column_names == ["ts", "price", "right.price", "bid"]
        assert result.to_pydict()["price"] == [10, 20, 30]
        assert result.to_pydict()["right.price"] == [100, 200, 300]

    def test_multiple_payload_clashes(self):
        """Multiple right payload columns that clash with left all get renamed."""
        left = daft.from_pydict({"ts": [1, 2], "a": [1, 2], "b": [3, 4]})
        right = daft.from_pydict({"ts": [1, 2], "a": [10, 20], "b": [30, 40], "c": [50, 60]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.column_names == ["ts", "a", "b", "right.a", "right.b", "c"]
        assert result.to_pydict()["a"] == [1, 2]
        assert result.to_pydict()["right.a"] == [10, 20]
        assert result.to_pydict()["b"] == [3, 4]
        assert result.to_pydict()["right.b"] == [30, 40]
        assert result.to_pydict()["c"] == [50, 60]

    def test_custom_suffix(self):
        left = daft.from_pydict({"ts": [1, 2], "val": [10, 20]})
        right = daft.from_pydict({"ts": [1, 2], "val": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", suffix="_right")
        assert "val_right" in result.column_names

    def test_custom_prefix(self):
        left = daft.from_pydict({"ts": [1, 2], "val": [10, 20]})
        right = daft.from_pydict({"ts": [1, 2], "val": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", prefix="r_")
        assert "r_val" in result.column_names

    def test_right_payload_clashes_with_left_on_key(self):
        """Right payload column with same name as left on-key gets renamed."""
        left = daft.from_pydict({"ts_l": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"ts_r": [1, 2], "ts_l": [99, 98], "w": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, left_on="ts_l", right_on="ts_r")
        assert result.column_names == ["ts_l", "v", "ts_r", "right.ts_l", "w"]
        assert result.to_pydict()["ts_l"] == [1, 2]
        assert result.to_pydict()["right.ts_l"] == [99, 98]

    def test_right_by_dropped_when_names_differ(self):
        """When left_by/right_by differ, the right by-key is dropped from output."""
        left = daft.from_pydict({"grp_l": ["A", "B"], "ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"grp_r": ["A", "B"], "ts": [1, 2], "w": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", left_by="grp_l", right_by="grp_r")
        assert "grp_r" not in result.column_names
        assert result.column_names == ["grp_l", "ts", "v", "w"]

    def test_right_on_key_kept_when_names_differ(self):
        """Using left_on/right_on with different names keeps both key columns."""
        left = daft.from_pydict({"ts_l": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"ts_r": [1, 2], "w": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, left_on="ts_l", right_on="ts_r")
        assert "ts_l" in result.column_names
        assert "ts_r" in result.column_names
        assert result.column_names == ["ts_l", "v", "ts_r", "w"]


# ---------------------------------------------------------------------------
# 6. Complex Expressions
# ---------------------------------------------------------------------------


class TestAsofJoinComplexExpressions:
    def test_arithmetic_right_on_no_clash(self):
        """right_on is an arithmetic expression; no column name clashes."""
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right = daft.from_pydict({"raw_ts": [11, 21, 31], "adj": [1, 1, 1], "w": [11, 22, 33]})
        # right_on = raw_ts - adj = [10, 20, 30], matches left ts exactly
        result = left.join_asof(
            right, _assume_sorted_and_aligned=True, left_on="ts", right_on=col("raw_ts") - col("adj")
        ).sort("ts")
        assert result.column_names == ["ts", "v", "raw_ts", "adj", "w"]
        assert result.to_pydict()["w"] == [11, 22, 33]

    def test_arithmetic_both_on_with_payload_clash(self):
        """Both left_on and right_on are complex exprs; payload col clashes and gets renamed."""
        left = daft.from_pydict({"ts": [10, 20, 30], "offset": [1, 1, 1], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [11, 21, 31], "offset": [1, 1, 1], "w": [11, 22, 33]})
        # left_on = ts - offset = [9, 19, 29], right_on = ts - offset = [10, 20, 30]
        # backward: right key must be <= left key
        # left 9: no right <= 9 -> None; left 19: right 10 <= 19 -> w=11; left 29: right 20 <= 29 -> w=22
        result = left.join_asof(
            right,
            _assume_sorted_and_aligned=True,
            left_on=col("ts") - col("offset"),
            right_on=col("ts") - col("offset"),
        ).sort("ts")
        assert result.to_pydict()["w"] == [None, 11, 22]

    def test_right_on_expr_refs_renamed_payload_col(self):
        """right_on references a payload col that gets renamed during deduplication."""
        left = daft.from_pydict({"ts": [10, 20, 30], "offset": [1, 1, 1], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [11, 21, 31], "offset": [1, 1, 1], "w": [11, 22, 33]})
        # effective right key = ts - offset = [10, 20, 30], exact match with left ts
        result = left.join_asof(
            right, _assume_sorted_and_aligned=True, left_on="ts", right_on=col("ts") - col("offset")
        ).sort("ts")
        assert result.to_pydict()["w"] == [11, 22, 33]

    def test_on_alias_stripped(self):
        """Alias on on= is stripped; underlying column name drives coalescing."""
        left = daft.from_pydict({"ts": [1, 2, 3], "v": [10, 20, 30]})
        right = daft.from_pydict({"ts": [1, 2, 3], "w": [100, 200, 300]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on=col("ts").alias("my_ts")).sort("ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [100, 200, 300]}

    def test_left_on_right_on_aliases_stripped(self):
        """Aliases on left_on/right_on are stripped; underlying col names drive schema."""
        left = daft.from_pydict({"ts_l": [5, 10, 15], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts_r": [3, 8, 12], "w": [30, 80, 120]})
        result = left.join_asof(
            right,
            left_on=col("ts_l").alias("x"),
            right_on=col("ts_r").alias("y"),
        ).sort("ts_l")
        assert result.column_names == ["ts_l", "v", "ts_r", "w"]
        assert result.to_pydict()["w"] == [30, 80, 120]

    def test_by_alias_stripped(self):
        """Alias on by= is stripped; underlying column name drives coalescing."""
        left = daft.from_pydict({"group": ["A", "B"], "ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"group": ["A", "B"], "ts": [1, 2], "w": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by=col("group").alias("g")).sort(
            "group"
        )
        assert result.column_names == ["group", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [100, 200]

    def test_left_by_right_by_aliases_stripped(self):
        """Aliases on left_by/right_by are stripped; right by-key dropped by underlying name."""
        left = daft.from_pydict({"grp_l": ["A", "B"], "ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"grp_r": ["A", "B"], "ts": [1, 2], "w": [100, 200]})
        result = left.join_asof(
            right,
            on="ts",
            left_by=col("grp_l").alias("grp"),
            right_by=col("grp_r").alias("grp"),
        ).sort("grp_l")
        assert result.column_names == ["grp_l", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [100, 200]


# ---------------------------------------------------------------------------
# 7. Integration with Other Operations
# ---------------------------------------------------------------------------


class TestAsofJoinIntegration:
    def test_asof_join_then_select(self):
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 20, 30], "w": [11, 22, 33]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").select("ts", "w").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "w": [11, 22, 33]}

    def test_asof_join_then_filter(self):
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 20, 30], "w": [11, 22, 33]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").where(col("w") > 11).sort("ts")
        assert result.to_pydict() == {"ts": [20, 30], "v": [2, 3], "w": [22, 33]}

    def test_chained_asof_joins(self):
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right1 = daft.from_pydict({"ts": [10, 20, 30], "w1": [11, 22, 33]})
        right2 = daft.from_pydict({"ts": [10, 20, 30], "w2": [110, 220, 330]})
        result = (
            left.join_asof(right1, _assume_sorted_and_aligned=True, on="ts")
            .join_asof(right2, _assume_sorted_and_aligned=True, on="ts")
            .sort("ts")
        )
        assert result.to_pydict() == {
            "ts": [10, 20, 30],
            "v": [1, 2, 3],
            "w1": [11, 22, 33],
            "w2": [110, 220, 330],
        }


# ---------------------------------------------------------------------------
# 8. Type Mismatches on Join Keys
# ---------------------------------------------------------------------------


class TestAsofJoinTypeMismatches:
    def test_int_float_asof_key_coercion(self):
        """Int left asof key and float right asof key coerce to a common type."""
        left = daft.from_pydict({"ts": [1, 2, 3], "v": [10, 20, 30]})
        right = daft.from_pydict({"ts": [1.0, 2.0, 3.0], "w": [100, 200, 300]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [100, 200, 300]}

    def test_int_float_by_key_coercion(self):
        """Int left by key and float right by key coerce to a common type and group correctly."""
        left = daft.from_pydict({"g": [1, 2], "ts": [10, 20], "v": [1, 2]})
        right = daft.from_pydict({"g": [1.0, 2.0], "ts": [5, 4], "w": [50, 60]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g").sort("g")
        assert result.column_names == ["g", "ts", "v", "w"]
        assert result.to_pydict() == {"g": [1, 2], "ts": [10, 20], "v": [1, 2], "w": [50, 60]}

    def test_partial_int_str_by_key_coercion(self):
        """Two by keys: first is int/int (no cast), second is int/str (cast to Utf8). Both match."""
        left = daft.from_pydict({"g1": [1, 2], "g2": [10, 20], "ts": [1, 2], "v": [10, 20]})
        right = daft.from_pydict({"g1": [1, 2], "g2": ["10", "20"], "ts": [1, 2], "w": [100, 200]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by=["g1", "g2"]).sort("g1")
        assert result.column_names == ["g1", "g2", "ts", "v", "w"]
        assert result.to_pydict() == {"g1": [1, 2], "g2": [10, 20], "ts": [1, 2], "v": [10, 20], "w": [100, 200]}

    def test_int_float_multiple_by_keys_coercion(self):
        """Two by keys both with int/float type differences coerce and group correctly."""
        left = daft.from_pydict({"g1": [1, 2], "g2": [10, 20], "ts": [10, 20], "v": [1, 2]})
        right = daft.from_pydict({"g1": [1.0, 2.0], "g2": [10.0, 20.0], "ts": [5, 4], "w": [50, 60]})
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by=["g1", "g2"]).sort("g1")
        assert result.column_names == ["g1", "g2", "ts", "v", "w"]
        assert result.to_pydict() == {"g1": [1, 2], "g2": [10, 20], "ts": [10, 20], "v": [1, 2], "w": [50, 60]}

    def test_time_date_asof_key_raises(self):
        """Time left asof key vs Date right asof key: no supertype exists, should raise."""
        left = daft.from_arrow(pa.table({"ts": pa.array([3_600_000_000_000], type=pa.time64("ns")), "v": [1]}))
        right = daft.from_arrow(pa.table({"ts": pa.array([datetime.date(2021, 1, 1)]), "w": [10]}))
        with pytest.raises(
            (daft.exceptions.DaftCoreException, RuntimeError),
            match="could not determine supertype of Time\\(Nanoseconds\\) and Date",
        ):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").collect()

    def test_time_date_by_key_raises(self):
        """Time left by key vs Date right by key: no supertype exists, should raise."""
        left = daft.from_arrow(
            pa.table({"g": pa.array([3_600_000_000_000], type=pa.time64("ns")), "ts": [1], "v": [1]})
        )
        right = daft.from_arrow(pa.table({"g": pa.array([datetime.date(2021, 1, 1)]), "ts": [1], "w": [10]}))
        with pytest.raises(
            (daft.exceptions.DaftCoreException, RuntimeError),
            match="could not determine supertype of Time\\(Nanoseconds\\) and Date",
        ):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g").collect()

    def test_multiple_by_keys_first_coerces_second_raises(self):
        """Two by keys: first is int/float (coerces), second is Time/Date (no supertype, raises)."""
        left = daft.from_arrow(
            pa.table({"g1": [1], "g2": pa.array([3_600_000_000_000], type=pa.time64("ns")), "ts": [10], "v": [1]})
        )
        right = daft.from_arrow(
            pa.table({"g1": [1.0], "g2": pa.array([datetime.date(2021, 1, 1)]), "ts": [5], "w": [50]})
        )
        with pytest.raises(
            (daft.exceptions.DaftCoreException, RuntimeError),
            match="could not determine supertype of Time\\(Nanoseconds\\) and Date",
        ):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by=["g1", "g2"]).collect()


# ---------------------------------------------------------------------------
# 9. Distributed Execution
# ---------------------------------------------------------------------------


class TestAsofJoinDistributed:
    """Tests that exercise multi-partition execution paths."""

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_no_by_keys_coalesces(self, make_df, n_partitions, with_default_morsel_size):
        """ASOF join without by-keys on multi-partition data coalesces correctly."""
        left = make_df(
            {"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]},
            repartition=n_partitions,
        )
        right = make_df(
            {"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]},
            repartition=n_partitions,
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            # 5: right 3<=5 -> 30; 10: right 8<=10 -> 80; 15: right 8<=15 -> 80
            # 20: right 18<=20 -> 180; 25: right 18<=25 -> 180
            "w": [30, 80, 80, 180, 180],
        }
