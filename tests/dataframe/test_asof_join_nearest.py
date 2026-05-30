from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

import daft
from daft import col


def get_n_partitions():
    return [1, 2, 4, 8]


# ---------------------------------------------------------------------------
# 1. Match Correctness
# ---------------------------------------------------------------------------


class TestNearestAsofJoinMatchCorrectness:
    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_exact_match(self, make_df, n_partitions, with_default_morsel_size):
        """Exact timestamp match wins over any equidistant candidates."""
        left = make_df({"ts": [10, 20, 30], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [10, 20, 30], "w": [11, 22, 33]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_picks_backward_when_closer(self, make_df, n_partitions, with_default_morsel_size):
        """Picks backward right row when it is strictly closer than any forward candidate."""
        left = make_df({"ts": [10], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [7, 14], "w": [70, 140]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [70]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_picks_forward_when_closer(self, make_df, n_partitions, with_default_morsel_size):
        """Picks forward right row when it is strictly closer than any backward candidate."""
        left = make_df({"ts": [10], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [6, 13], "w": [60, 130]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [130]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_direction_switches_within_sequence(self, make_df, n_partitions, with_default_morsel_size):
        """Direction of nearest match changes row-by-row across the same right array."""
        left = make_df({"ts": [3, 8, 13], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [5, 11], "w": [50, 110]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [3, 8, 13], "v": [1, 2, 3], "w": [50, 110, 110]}

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_all_left_before_all_right_no_nulls(self, make_df, n_partitions, with_default_morsel_size):
        """When all left rows precede all right rows, nearest picks the closest right rather than returning null."""
        left = make_df({"ts": [1, 2, 3], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [100, 200], "w": [1000, 2000]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [1, 2, 3], "w": [1000, 1000, 1000]}

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_all_left_after_all_right_no_nulls(self, make_df, n_partitions, with_default_morsel_size):
        """When all left rows follow all right rows, nearest picks the closest right rather than returning null."""
        left = make_df({"ts": [100, 200, 300], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [10, 50], "w": [100, 500]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [100, 200, 300], "v": [1, 2, 3], "w": [500, 500, 500]}

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_single_right_row_always_matches(self, make_df, n_partitions, with_default_morsel_size):
        """With only one right row, every left row matches it regardless of relative position."""
        left = make_df({"ts": [1, 50, 99], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [50], "w": [500]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [1, 50, 99], "v": [1, 2, 3], "w": [500, 500, 500]}

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_direction_mix_across_longer_sequence(self, make_df, n_partitions, with_default_morsel_size):
        """Each left row independently picks its nearest right; the winning direction varies throughout."""
        left = make_df({"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]}, repartition=n_partitions)
        right = make_df({"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            "w": [30, 80, 180, 180, 300],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_left_on_right_on_different_names(self, make_df, n_partitions, with_default_morsel_size):
        """Nearest join works when left and right use different names for the on column."""
        left = make_df({"event_ts": [10, 20], "v": [1, 2]}, repartition=n_partitions)
        right = make_df({"obs_ts": [8, 22], "w": [80, 220]}, repartition=n_partitions)
        result = left.join_asof(right, left_on="event_ts", right_on="obs_ts", strategy="nearest").sort("event_ts")
        assert result.column_names == ["event_ts", "v", "obs_ts", "w"]
        pydict = result.to_pydict()
        assert pydict["event_ts"] == [10, 20]
        assert pydict["w"] == [80, 220]


# ---------------------------------------------------------------------------
# 2. Tie-Breaking
# ---------------------------------------------------------------------------


class TestNearestAsofJoinTieBreaking:
    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_equidistant_forward_preferred(self, make_df, n_partitions, with_default_morsel_size):
        """When backward and forward candidates are equidistant, forward is picked."""
        left = make_df({"ts": [10], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [8, 12], "w": [80, 120]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [120]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_exact_match_beats_equidistant_candidates(self, make_df, n_partitions, with_default_morsel_size):
        """Exact match (dist=0) always wins over equidistant backward/forward candidates."""
        left = make_df({"ts": [10], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [8, 10, 12], "w": [80, 100, 120]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [100]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_one_right_row_matches_multiple_left_rows(self, make_df, n_partitions, with_default_morsel_size):
        """Multiple left rows can independently match the same nearest right row."""
        left = make_df({"ts": [10, 15], "v": [1, 2]}, repartition=n_partitions)
        right = make_df({"ts": [7, 13], "w": [70, 130]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [10, 15], "v": [1, 2], "w": [130, 130]}

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_duplicate_left_timestamps(self, make_df, n_partitions, with_default_morsel_size):
        """Duplicate left timestamps both independently match the same nearest right row."""
        left = make_df({"ts": [10, 10, 20], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [8, 15], "w": [80, 150]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort(["ts", "v"])
        # @10: dist(8)=2 < dist(15)=5 → backward@8 → 80 (both duplicates)
        # @20: dist(8)=12 > dist(15)=5 → forward@15 → 150
        assert result.to_pydict() == {"ts": [10, 10, 20], "v": [1, 2, 3], "w": [80, 80, 150]}


# ---------------------------------------------------------------------------
# 3. Null Handling
# ---------------------------------------------------------------------------


class TestNearestAsofJoinNullHandling:
    def test_null_in_asof_key_left_produces_null(self):
        """Null asof keys on left produce null matches on the right side."""
        left = daft.from_pydict({"ts": [None, 5, None], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [3, 5, 7], "w": [30, 50, 70]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("v")
        pydict = result.to_pydict()
        assert pydict["v"] == [1, 2, 3]
        assert pydict["w"][0] is None  # ts=null
        assert pydict["w"][1] == 50  # ts=5, exact match
        assert pydict["w"][2] is None  # ts=null

    def test_null_in_by_key_no_cross_match(self):
        """Null by-keys on left do not match null by-keys on right."""
        left = daft.from_pydict({"g": ["X", None], "ts": [5, 5], "v": [1, 2]})
        right = daft.from_pydict({"g": ["X", None], "ts": [3, 3], "w": [10, 20]})
        result = left.join_asof(right, on="ts", by="g", strategy="nearest").sort("v")
        assert result.column_names == ["g", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [10, None]


# ---------------------------------------------------------------------------
# 4. Group-By
# ---------------------------------------------------------------------------


class TestNearestAsofJoinWithBy:
    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_nearest_per_group_different_directions(self, make_df, n_partitions, with_default_morsel_size):
        """Each group independently finds its nearest; the winning direction can differ across groups."""
        left = make_df({"entity": ["A", "B"], "ts": [10, 10], "v": [1, 2]}, repartition=n_partitions)
        right = make_df(
            {"entity": ["A", "A", "B", "B"], "ts": [7, 14, 6, 13], "w": [70, 140, 60, 130]},
            repartition=n_partitions,
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort("entity")
        assert result.to_pydict() == {
            "entity": ["A", "B"],
            "ts": [10, 10],
            "v": [1, 2],
            "w": [70, 130],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_only_right_after_all_left_in_group_no_nulls(self, make_df, n_partitions, with_default_morsel_size):
        """Within a group where all right rows follow all left rows, nearest still matches rather than returning null."""
        left = make_df({"entity": ["A", "A", "A"], "ts": [1, 2, 3], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"entity": ["A"], "ts": [100], "w": [1000]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "entity": ["A", "A", "A"],
            "ts": [1, 2, 3],
            "v": [1, 2, 3],
            "w": [1000, 1000, 1000],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_only_right_before_all_left_in_group_no_nulls(self, make_df, n_partitions, with_default_morsel_size):
        """Within a group where all right rows precede all left rows, nearest still matches rather than returning null."""
        left = make_df({"entity": ["A", "A"], "ts": [100, 200], "v": [1, 2]}, repartition=n_partitions)
        right = make_df({"entity": ["A"], "ts": [5], "w": [50]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "entity": ["A", "A"],
            "ts": [100, 200],
            "v": [1, 2],
            "w": [50, 50],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_empty_right_group_gives_null(self, make_df, n_partitions, with_default_morsel_size):
        """A group with no right rows produces null; other groups still match correctly."""
        left = make_df(
            {"entity": ["A", "A", "B", "B"], "ts": [5, 10, 5, 10], "v": [1, 2, 3, 4]},
            repartition=n_partitions,
        )
        right = make_df({"entity": ["B", "B"], "ts": [4, 9], "w": [40, 90]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B"],
            "ts": [5, 10, 5, 10],
            "v": [1, 2, 3, 4],
            "w": [None, None, 40, 90],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_group_boundary_not_crossed(self, make_df, n_partitions, with_default_morsel_size):
        """A right row from one group cannot match a left row from a different group."""
        left = make_df(
            {
                "entity": ["A", "A", "A", "B", "B", "B"],
                "ts": [3, 7, 10, 15, 20, 25],
                "v": [1, 2, 3, 4, 5, 6],
            },
            repartition=n_partitions,
        )
        right = make_df({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [3, 7, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5, 6],
            # A@3: dist(2)=1, dist(8)=5 → backward@2 → 20
            # A@7: dist(2)=5, dist(8)=1 → forward@8 → 80
            # A@10: dist(2)=8, dist(8)=2 → backward@8 → 80
            # B@*: no right rows for B → null
            "w": [20, 80, 80, None, None, None],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_nearest_with_multiple_by_columns(self, make_df, n_partitions, with_default_morsel_size):
        """Composite by-key (entity, region) groups rows independently; each group picks its own nearest."""
        left = make_df(
            {"entity": ["A", "B"], "region": ["US", "EU"], "ts": [10, 10], "v": [1, 2]},
            repartition=n_partitions,
        )
        right = make_df(
            {
                "entity": ["A", "A", "B", "B"],
                "region": ["US", "US", "EU", "EU"],
                "ts": [7, 14, 6, 13],
                "w": [70, 140, 60, 130],
            },
            repartition=n_partitions,
        )
        result = left.join_asof(right, on="ts", by=["entity", "region"], strategy="nearest").sort("entity")
        assert result.to_pydict() == {
            "entity": ["A", "B"],
            "region": ["US", "EU"],
            "ts": [10, 10],
            "v": [1, 2],
            # (A, US)@10: dist(7)=3 < dist(14)=4 → backward@7 → w=70
            # (B, EU)@10: dist(6)=4 > dist(13)=3 → forward@13 → w=130
            "w": [70, 130],
        }

    def test_cross_group_isolation_multiple_by(self):
        """Right rows from a different (entity, region) pair are not used even if on-key is close."""
        left = daft.from_pydict({"entity": ["A"], "region": ["US"], "ts": [10], "v": [1]})
        right = daft.from_pydict({"entity": ["A"], "region": ["EU"], "ts": [9], "w": [90]})
        result = left.join_asof(right, on="ts", by=["entity", "region"], strategy="nearest")
        assert result.to_pydict()["w"] == [None]


# ---------------------------------------------------------------------------
# 5. Empty Tables
# ---------------------------------------------------------------------------


class TestNearestAsofJoinEmptyTables:
    def test_empty_left_table(self):
        """Empty left table produces an empty result."""
        left = daft.from_pydict({"ts": [], "v": []})
        right = daft.from_pydict({"ts": [1, 2, 3], "w": [10, 20, 30]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}

    def test_empty_right_table(self):
        """Empty right table produces nulls for all right columns."""
        left = daft.from_pydict({"ts": [1, 2, 3], "v": [10, 20, 30]})
        right = daft.from_pydict({"ts": [], "w": []})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [None, None, None]}

    def test_empty_left_and_right_tables(self):
        """Both tables empty produces an empty result."""
        left = daft.from_pydict({"ts": [], "v": []})
        right = daft.from_pydict({"ts": [], "w": []})
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}


# ---------------------------------------------------------------------------
# 6. Float on Column
# ---------------------------------------------------------------------------


class TestNearestAsofJoinFloatOnKey:
    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_basic_float_on_key(self, make_df, n_partitions, with_default_morsel_size):
        """Float on-key exercises the NaN-aware float comparator path."""
        left = make_df({"ts": [1.2, 3.7], "v": [1, 2]}, repartition=n_partitions)
        right = make_df({"ts": [1.0, 2.0, 4.0], "w": [10, 20, 40]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        # @1.2: dist(1.0)=0.2 < dist(2.0)=0.8 → backward@1.0 → w=10
        # @3.7: dist(2.0)=1.7 > dist(4.0)=0.3 → forward@4.0 → w=40
        assert result.to_pydict() == {"ts": [1.2, 3.7], "v": [1, 2], "w": [10, 40]}

    def test_float_equidistant_forward_preferred(self):
        """Float tie-breaking: equidistant candidates use the same forward-wins convention as integers."""
        left = daft.from_pydict({"ts": [2.0], "v": [1]})
        right = daft.from_pydict({"ts": [1.0, 3.0], "w": [10, 30]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        # dist(1.0)=1.0 == dist(3.0)=1.0 → tie → forward wins → w=30
        assert result.to_pydict()["w"] == [30]

    def test_nan_in_right_on_not_picked_over_finite(self):
        """NaN in right on-column is sorted to the end and never beats a finite candidate."""
        left = daft.from_pydict({"ts": [5.0], "v": [1]})
        right = daft.from_pydict({"ts": [3.0, float("nan")], "w": [30, 999]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        # NaN treated as +inf: dist(3.0)=2.0 < dist(NaN)=inf → w=30
        assert result.to_pydict()["w"] == [30]


# ---------------------------------------------------------------------------
# 7. Distributed Execution
# ---------------------------------------------------------------------------


class TestNearestAsofJoinDistributed:
    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_multi_group_correctness(self, make_df, n_partitions, with_default_morsel_size):
        """Multiple groups spread across partitions all nearest-match correctly."""
        left = make_df(
            {
                "entity": ["A", "B", "C", "D", "A", "B", "C", "D"],
                "ts": [10, 10, 10, 10, 20, 20, 20, 20],
                "v": [1, 2, 3, 4, 5, 6, 7, 8],
            },
            repartition=n_partitions,
        )
        right = make_df(
            {
                "entity": ["A", "B", "C", "D", "A", "B", "C", "D"],
                "ts": [5, 8, 12, 15, 18, 22, 25, 28],
                "w": [100, 200, 300, 400, 500, 600, 700, 800],
            },
            repartition=n_partitions,
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "ts": [10, 20, 10, 20, 10, 20, 10, 20],
            "v": [1, 5, 2, 6, 3, 7, 4, 8],
            "w": [100, 500, 200, 600, 300, 700, 400, 400],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_forward_carryover_beats_local_backward_match(self, make_df, n_partitions, with_default_morsel_size):
        """A closer right row in a later partition beats a local backward match."""
        left = make_df({"ts": [5], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [3, 6], "w": [30, 60]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [60]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_backward_match_beats_far_forward_carryover(self, make_df, n_partitions, with_default_morsel_size):
        """A nearby backward match is not overwritten by a distant forward carryover."""
        left = make_df({"ts": [50], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [48, 200], "w": [480, 2000]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [480]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_equidistant_across_partition_boundary(self, make_df, n_partitions, with_default_morsel_size):
        """Equidistant candidates spanning partitions: forward wins consistently."""
        left = make_df({"ts": [10], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [8, 12], "w": [80, 120]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [120]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_nearest_requires_both_carryovers_simultaneously(self, make_df, n_partitions, with_default_morsel_size):
        """Left rows at opposite ends of the range each need carryover from a different direction simultaneously."""
        left = make_df({"ts": [10, 90], "v": [1, 2]}, repartition=n_partitions)
        right = make_df({"ts": [5, 50, 95], "w": [50, 500, 950]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [10, 90],
            "v": [1, 2],
            "w": [50, 950],
        }

    @pytest.mark.parametrize(
        "left_partitions,right_partitions",
        [(4, 2), (2, 4), (8, 2), (2, 8)],
    )
    def test_asymmetric_partition_counts(self, make_df, left_partitions, right_partitions, with_default_morsel_size):
        """Left and right are hash-partitioned on the by-key with different partition counts."""
        left = make_df(
            {"entity": ["A", "B", "A", "B"], "ts": [10, 10, 20, 20], "v": [1, 2, 3, 4]},
            repartition=left_partitions,
            repartition_columns=["entity"],
        )
        right = make_df(
            {"entity": ["A", "A", "B", "B"], "ts": [5, 18, 8, 22], "w": [100, 500, 200, 600]},
            repartition=right_partitions,
            repartition_columns=["entity"],
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B"],
            "ts": [10, 20, 10, 20],
            "v": [1, 3, 2, 4],
            # A@10: dist(5)=5 < dist(18)=8 → backward@5 → w=100
            # A@20: dist(5)=15 > dist(18)=2 → forward@18 → w=500
            # B@10: dist(8)=2 < dist(22)=12 → backward@8 → w=200
            # B@20: dist(8)=12 > dist(22)=2 → forward@22 → w=600
            "w": [100, 500, 200, 600],
        }

    @pytest.mark.parametrize(
        "left_partitions,right_partitions,hash_partition_left",
        [(4, 2, True), (2, 4, True), (4, 2, False), (2, 4, False)],
    )
    def test_one_side_not_hash_partitioned(
        self, make_df, left_partitions, right_partitions, hash_partition_left, with_default_morsel_size
    ):
        """One side is hash-partitioned on the by-key; the other has a random clustering spec."""
        left = make_df(
            {"entity": ["A", "B", "A", "B"], "ts": [10, 10, 20, 20], "v": [1, 2, 3, 4]},
            repartition=left_partitions,
            repartition_columns=["entity"] if hash_partition_left else [],
        )
        right = make_df(
            {"entity": ["A", "A", "B", "B"], "ts": [5, 18, 8, 22], "w": [100, 500, 200, 600]},
            repartition=right_partitions,
            repartition_columns=[] if hash_partition_left else ["entity"],
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B"],
            "ts": [10, 20, 10, 20],
            "v": [1, 3, 2, 4],
            "w": [100, 500, 200, 600],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_child_join_same_by_keys(self, make_df, n_partitions, with_default_morsel_size):
        """Nearest asof where left input is already a join hashed on the same by-keys."""
        base_left = make_df(
            {"ticker": ["AAPL", "GOOG", "AAPL", "GOOG"], "id": [1, 2, 3, 4], "ts": [10, 10, 20, 20]},
            repartition=n_partitions,
        )
        base_right = make_df(
            {"ticker": ["AAPL", "GOOG", "AAPL", "GOOG"], "id": [1, 2, 3, 4], "label": ["a", "b", "c", "d"]},
            repartition=n_partitions,
        )
        joined_left = base_left.join(base_right, on=["ticker", "id"])

        asof_right = make_df(
            {"ticker": ["AAPL", "GOOG", "AAPL", "GOOG"], "ts": [5, 8, 22, 25], "w": [50, 80, 220, 250]},
            repartition=n_partitions,
        )
        result = joined_left.join_asof(asof_right, on="ts", by="ticker", strategy="nearest").sort(["ticker", "ts"])
        assert result.to_pydict() == {
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "id": [1, 3, 2, 4],
            "ts": [10, 20, 10, 20],
            "label": ["a", "c", "b", "d"],
            # AAPL@10: dist(5)=5 < dist(22)=12 → backward@5  → w=50
            # AAPL@20: dist(5)=15 > dist(22)=2 → forward@22  → w=220
            # GOOG@10: dist(8)=2 < dist(25)=15 → backward@8  → w=80
            # GOOG@20: dist(8)=12 > dist(25)=5 → forward@25  → w=250
            "w": [50, 220, 80, 250],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_child_join_different_by_keys(self, make_df, n_partitions, with_default_morsel_size):
        """Nearest asof where left input is a join hashed on different keys than the asof by-keys."""
        base_left = make_df(
            {"id": [1, 2, 3, 4], "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"], "ts": [10, 10, 20, 20]},
            repartition=n_partitions,
        )
        base_right = make_df(
            {"id": [1, 2, 3, 4], "label": ["a", "b", "c", "d"]},
            repartition=n_partitions,
        )
        joined_left = base_left.join(base_right, on="id")

        asof_right = make_df(
            {"ticker": ["AAPL", "GOOG", "AAPL", "GOOG"], "ts": [5, 8, 22, 25], "w": [50, 80, 220, 250]},
            repartition=n_partitions,
        )
        result = joined_left.join_asof(asof_right, on="ts", by="ticker", strategy="nearest").sort(["ticker", "ts"])
        assert result.to_pydict() == {
            "id": [1, 3, 2, 4],
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "ts": [10, 20, 10, 20],
            "label": ["a", "c", "b", "d"],
            "w": [50, 220, 80, 250],
        }


# ---------------------------------------------------------------------------
# 8. On-Key Data Types
# ---------------------------------------------------------------------------


class TestNearestAsofJoinDataTypes:
    """One correctness probe per supported on-key data type."""

    def test_int8_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.int8()))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.int8()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_int16_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.int16()))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.int16()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_int32_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.int32()))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.int32()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_int64_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_uint8_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.uint8()))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.uint8()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_uint16_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.uint16()))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.uint16()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_uint32_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.uint32()))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.uint32()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_uint64_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.uint64()))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.uint64()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_decimal128_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.decimal128(10, 0)))
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.decimal128(10, 0)))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_float16_key(self):
        left = daft.from_pydict({"ts": [10.0, 20.0], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.float16()))
        right = daft.from_pydict({"ts": [8.0, 22.0], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.float16()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_float32_key(self):
        left = daft.from_pydict({"ts": [10.0, 20.0], "v": [1, 2]})
        left = left.with_column("ts", col("ts").cast(daft.DataType.float32()))
        right = daft.from_pydict({"ts": [8.0, 22.0], "w": [80, 220]})
        right = right.with_column("ts", col("ts").cast(daft.DataType.float32()))
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_float64_key(self):
        left = daft.from_pydict({"ts": [10.0, 20.0], "v": [1, 2]})
        right = daft.from_pydict({"ts": [8.0, 22.0], "w": [80, 220]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]


# ---------------------------------------------------------------------------
# Temporal key types (distributed)
# ---------------------------------------------------------------------------


class TestNearestAsofJoinTemporalKeys:
    """Nearest join over various temporal and numeric on-key types.

    Each test uses the same shape:
      left  ts=[A, B],  right ts=[C, D]   where A is closer to C and B is closer to D.
    Expected w=[80, 220] for all type variants.

    These tests run with DAFT_RUNNER=ray to exercise the distributed code path,
    where Arrow IPC serialization preserves the true Arrow type rather than
    Daft's internal int-backed representation.
    """

    def test_date32_key(self):
        """Date32: distance in days; 2 days < 12 days selects the correct direction."""
        left = daft.from_pydict({"ts": [date(2020, 1, 11), date(2020, 1, 21)], "v": [1, 2]})
        right = daft.from_pydict({"ts": [date(2020, 1, 9), date(2020, 1, 23)], "w": [80, 220]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_int64_key(self):
        left = daft.from_pydict({"ts": [10, 20], "v": [1, 2]})
        right = daft.from_pydict({"ts": [8, 22], "w": [80, 220]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_date64_key(self):
        # daft promotes pa.date64 to Timestamp[ms] internally.
        import pyarrow as pa

        left = daft.from_pydict({"ts": pa.array([date(2020, 1, 11), date(2020, 1, 21)], type=pa.date64()), "v": [1, 2]})
        right = daft.from_pydict(
            {"ts": pa.array([date(2020, 1, 9), date(2020, 1, 23)], type=pa.date64()), "w": [80, 220]}
        )
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_timestamp_us_key(self):
        """Timestamp[us]: distance in microseconds; 2 days < 12 days selects the correct direction."""
        left = daft.from_pydict({"ts": [datetime(2020, 1, 11), datetime(2020, 1, 21)], "v": [1, 2]})
        right = daft.from_pydict({"ts": [datetime(2020, 1, 9), datetime(2020, 1, 23)], "w": [80, 220]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_time64_us_key(self):
        """Time64[us]: distance in microseconds; 2 hours < 12 hours selects the correct direction."""
        left = daft.from_pydict({"ts": [time(10), time(20)], "v": [1, 2]})
        right = daft.from_pydict({"ts": [time(8), time(22)], "w": [80, 220]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]

    def test_duration_us_key(self):
        """Duration[us]: distance in microseconds; 2s < 12s selects the correct direction."""
        left = daft.from_pydict({"ts": [timedelta(seconds=10), timedelta(seconds=20)], "v": [1, 2]})
        right = daft.from_pydict({"ts": [timedelta(seconds=8), timedelta(seconds=22)], "w": [80, 220]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict()["w"] == [80, 220]
