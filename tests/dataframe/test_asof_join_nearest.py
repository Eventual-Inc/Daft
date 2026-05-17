from __future__ import annotations

import pytest

import daft
from daft import col


def get_n_partitions():
    return [1, 2, 4, 8]


# ---------------------------------------------------------------------------
# 1. Match Correctness
# ---------------------------------------------------------------------------


class TestNearestAsofJoinMatchCorrectness:
    def test_exact_match(self):
        """Exact timestamp match wins over any equidistant candidates."""
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 20, 30], "w": [11, 22, 33]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    def test_picks_backward_when_closer(self):
        """Picks backward right row when it is strictly closer than any forward candidate."""
        # dist(7)=3 < dist(14)=4 → backward wins
        left = daft.from_pydict({"ts": [10], "v": [1]})
        right = daft.from_pydict({"ts": [7, 14], "w": [70, 140]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [70]

    def test_picks_forward_when_closer(self):
        """Picks forward right row when it is strictly closer than any backward candidate."""
        # dist(6)=4 > dist(13)=3 → forward wins
        left = daft.from_pydict({"ts": [10], "v": [1]})
        right = daft.from_pydict({"ts": [6, 13], "w": [60, 130]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [130]

    def test_direction_switches_within_sequence(self):
        """Most important correctness test: direction of match changes row-by-row.

        Impossible to cover with backward or forward alone.
          @3:  only right@5 available (forward, dist=2)  → w=50
          @8:  right@5 dist=3, right@11 dist=3 → tie → forward wins → w=110
          @13: only right@11 reachable more closely (backward, dist=2) → w=110
        """
        left = daft.from_pydict({"ts": [3, 8, 13], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [5, 11], "w": [50, 110]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [3, 8, 13], "v": [1, 2, 3], "w": [50, 110, 110]}

    def test_all_left_before_all_right_no_nulls(self):
        """Key distinction from backward: nearest never nulls due to wrong direction.

        When all left rows precede all right rows, nearest picks the first (closest) right row.
        Backward would return all nulls here.
        """
        left = daft.from_pydict({"ts": [1, 2, 3], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [100, 200], "w": [1000, 2000]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [1, 2, 3], "w": [1000, 1000, 1000]}

    def test_all_left_after_all_right_no_nulls(self):
        """Key distinction from forward: nearest never nulls due to wrong direction.

        When all left rows follow all right rows, nearest picks the last (closest) right row.
        Forward would return all nulls here.
        """
        left = daft.from_pydict({"ts": [100, 200, 300], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 50], "w": [100, 500]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [100, 200, 300], "v": [1, 2, 3], "w": [500, 500, 500]}

    def test_single_right_row_always_matches(self):
        """With only one right row, every left row matches it regardless of relative position."""
        left = daft.from_pydict({"ts": [1, 50, 99], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [50], "w": [500]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [1, 50, 99], "v": [1, 2, 3], "w": [500, 500, 500]}

    def test_direction_mix_across_longer_sequence(self):
        """Each left row independently picks its nearest right; direction varies throughout.

        left@[5,10,15,20,25], right@[3,8,18,30]:
          @5:  dist(3)=2 < dist(8)=3  → backward@3  → w=30
          @10: dist(8)=2 < dist(18)=8 → backward@8  → w=80
          @15: dist(8)=7 > dist(18)=3 → forward@18  → w=180
          @20: dist(18)=2 < dist(30)=10 → backward@18 → w=180
          @25: dist(18)=7 > dist(30)=5  → forward@30  → w=300
        """
        left = daft.from_pydict({"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]})
        right = daft.from_pydict({"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            "w": [30, 80, 180, 180, 300],
        }


# ---------------------------------------------------------------------------
# 2. Tie-Breaking
# ---------------------------------------------------------------------------


class TestNearestAsofJoinTieBreaking:
    def test_equidistant_forward_preferred(self):
        """When backward and forward candidates are equidistant, forward is picked.

        This pins down Daft's tie-break convention for nearest.
        left@10, right@[8,12]: dist=2 on both sides → forward (12) wins.
        """
        left = daft.from_pydict({"ts": [10], "v": [1]})
        right = daft.from_pydict({"ts": [8, 12], "w": [80, 120]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [120]

    def test_equidistant_in_sequence(self):
        """Tie-breaking applied mid-sequence; non-tie rows match straightforwardly.

        left@[5,10,15], right@[7,13]:
          @5:  only forward@7 available (dist=2)        → w=70
          @10: dist(7)=3 == dist(13)=3, tie → forward@13 → w=130
          @15: only backward@13 closer (dist=2)          → w=130
        """
        left = daft.from_pydict({"ts": [5, 10, 15], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [7, 13], "w": [70, 130]})
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [5, 10, 15], "v": [1, 2, 3], "w": [70, 130, 130]}

    def test_exact_match_beats_equidistant_candidates(self):
        """Exact match (dist=0) always wins over equidistant backward/forward candidates."""
        left = daft.from_pydict({"ts": [10], "v": [1]})
        right = daft.from_pydict({"ts": [8, 10, 12], "w": [80, 100, 120]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [100]

    def test_duplicate_right_same_side(self):
        """Duplicate right rows on the winning side: any one of them is a valid pick."""
        # dist(8)=2 wins over dist(14)=4; two right rows at ts=8
        left = daft.from_pydict({"ts": [10], "v": [1]})
        right = daft.from_pydict({"ts": [8, 8, 14], "w": [80, 81, 140]})
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["ts"] == [10]
        assert result.to_pydict()["w"][0] in [80, 81]

    def test_duplicate_left_timestamps(self):
        """Duplicate left timestamps both independently match the same nearest right row."""
        left = daft.from_pydict({"ts": [10, 10, 20], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [8, 15], "w": [80, 150]})
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
    def test_nearest_per_group_different_directions(self):
        """Each group independently finds its nearest; direction can differ across groups.

        Entity A: left@10, right@[7,14] → dist(7)=3 < dist(14)=4 → backward@7 → w=70
        Entity B: left@10, right@[6,13] → dist(6)=4 > dist(13)=3 → forward@13 → w=130
        """
        left = daft.from_pydict({"entity": ["A", "B"], "ts": [10, 10], "v": [1, 2]})
        right = daft.from_pydict(
            {"entity": ["A", "A", "B", "B"], "ts": [7, 14, 6, 13], "w": [70, 140, 60, 130]}
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort("entity")
        assert result.to_pydict() == {
            "entity": ["A", "B"],
            "ts": [10, 10],
            "v": [1, 2],
            "w": [70, 130],
        }

    def test_only_right_after_all_left_in_group_no_nulls(self):
        """Within a group, if all right rows follow all left rows, nearest still matches (no nulls).

        Backward would give null here. Nearest gives right@100 (the only and thus nearest right).
        """
        left = daft.from_pydict({"entity": ["A", "A", "A"], "ts": [1, 2, 3], "v": [1, 2, 3]})
        right = daft.from_pydict({"entity": ["A"], "ts": [100], "w": [1000]})
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "entity": ["A", "A", "A"],
            "ts": [1, 2, 3],
            "v": [1, 2, 3],
            "w": [1000, 1000, 1000],
        }

    def test_only_right_before_all_left_in_group_no_nulls(self):
        """Within a group, if all right rows precede all left rows, nearest still matches (no nulls).

        Forward would give null here. Nearest gives right@5 (the only and thus nearest right).
        """
        left = daft.from_pydict({"entity": ["A", "A"], "ts": [100, 200], "v": [1, 2]})
        right = daft.from_pydict({"entity": ["A"], "ts": [5], "w": [50]})
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "entity": ["A", "A"],
            "ts": [100, 200],
            "v": [1, 2],
            "w": [50, 50],
        }

    def test_empty_right_group_gives_null(self):
        """A group with no right rows produces null; other groups still match correctly."""
        left = daft.from_pydict(
            {"entity": ["A", "A", "B", "B"], "ts": [5, 10, 5, 10], "v": [1, 2, 3, 4]}
        )
        right = daft.from_pydict({"entity": ["B", "B"], "ts": [4, 9], "w": [40, 90]})
        result = left.join_asof(right, on="ts", by="entity", strategy="nearest").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B"],
            "ts": [5, 10, 5, 10],
            "v": [1, 2, 3, 4],
            "w": [None, None, 40, 90],
        }

    def test_group_boundary_not_crossed(self):
        """A right row from one group cannot match a left row from a different group."""
        left = daft.from_pydict(
            {"entity": ["A", "A", "A", "B", "B", "B"], "ts": [3, 7, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5, 6]}
        )
        # Only entity A has right rows; entity B has none
        right = daft.from_pydict({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]})
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
# 6. Distributed Execution
# ---------------------------------------------------------------------------


class TestNearestAsofJoinDistributed:
    """Tests that exercise multi-partition nearest join paths.

    Unlike backward (forward-only carryover) and forward (backward-only carryover),
    nearest requires comparing candidates from BOTH directions across partition boundaries.
    """

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_no_by_keys_multi_partition(self, make_df, n_partitions, with_default_morsel_size):
        """Nearest without by-keys across multiple partitions picks the true nearest right row."""
        left = make_df({"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]}, repartition=n_partitions)
        right = make_df({"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            # @5:  dist(3)=2 < dist(8)=3  → backward@3  → 30
            # @10: dist(8)=2 < dist(18)=8 → backward@8  → 80
            # @15: dist(8)=7 > dist(18)=3 → forward@18  → 180
            # @20: dist(18)=2 < dist(30)=10 → backward@18 → 180
            # @25: dist(18)=7 > dist(30)=5  → forward@30  → 300
            "w": [30, 80, 180, 180, 300],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_multi_group_correctness(self, make_df, n_partitions, with_default_morsel_size):
        """Multiple entities spread across partitions all nearest-match correctly."""
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
            # A@10: dist(A@5)=5 < dist(A@18)=8  → backward@5  → 100
            # A@20: dist(A@5)=15 > dist(A@18)=2 → forward@18  → 500
            # B@10: dist(B@8)=2 < dist(B@22)=12 → backward@8  → 200
            # B@20: dist(B@8)=12 > dist(B@22)=2 → forward@22  → 600
            # C@10: dist(C@12)=2 < dist(C@25)=15 → forward@12 → 300
            # C@20: dist(C@12)=8 > dist(C@25)=5 → forward@25  → 700
            # D@10: dist(D@15)=5 < dist(D@28)=18 → forward@15 → 400
            # D@20: dist(D@15)=5 < dist(D@28)=8 → backward@15 → 400
            "w": [100, 500, 200, 600, 300, 700, 400, 400],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_forward_carryover_beats_local_backward_match(
        self, make_df, n_partitions, with_default_morsel_size
    ):
        """A right row in a later partition wins because it is closer than the local backward match.

        left@5: right@3 (dist=2) lives in an earlier partition, right@6 (dist=1) in a later one.
        The forward carryover must propagate right@6 back so it beats right@3.
        """
        left = make_df({"ts": [5], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [3, 6], "w": [30, 60]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [60]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_backward_match_beats_far_forward_carryover(
        self, make_df, n_partitions, with_default_morsel_size
    ):
        """A nearby backward match must not be overwritten by a distant forward carryover.

        left@50: right@48 (dist=2) in an earlier bucket, right@200 (dist=150) in a later one.
        The forward carryover must NOT replace the closer backward match.
        """
        left = make_df({"ts": [50], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [48, 200], "w": [480, 2000]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [480]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_equidistant_across_partition_boundary(
        self, make_df, n_partitions, with_default_morsel_size
    ):
        """Tie-breaking is consistent even when the two equidistant candidates span partitions.

        left@10: right@8 (dist=2) and right@12 (dist=2) may land in different partitions.
        Forward should win regardless of partitioning.
        """
        left = make_df({"ts": [10], "v": [1]}, repartition=n_partitions)
        right = make_df({"ts": [8, 12], "w": [80, 120]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest")
        assert result.to_pydict()["w"] == [120]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_all_right_before_all_left_distributed(
        self, make_df, n_partitions, with_default_morsel_size
    ):
        """All right rows precede all left rows across partitions: nearest gives the last right (no nulls).

        This would produce all nulls with forward strategy. Nearest must propagate the
        backward match (right@50) across all partitions to reach every left row.
        """
        left = make_df({"ts": [100, 200, 300], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [10, 50], "w": [100, 500]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [100, 200, 300],
            "v": [1, 2, 3],
            "w": [500, 500, 500],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_all_right_after_all_left_distributed(
        self, make_df, n_partitions, with_default_morsel_size
    ):
        """All right rows follow all left rows across partitions: nearest gives the first right (no nulls).

        This would produce all nulls with backward strategy. Nearest must propagate the
        forward carryover (right@100) backwards through all partitions to reach every left row.
        """
        left = make_df({"ts": [1, 2, 3], "v": [1, 2, 3]}, repartition=n_partitions)
        right = make_df({"ts": [100, 200], "w": [1000, 2000]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [1, 2, 3],
            "v": [1, 2, 3],
            "w": [1000, 1000, 1000],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_nearest_requires_both_carryovers_simultaneously(
        self, make_df, n_partitions, with_default_morsel_size
    ):
        """The critical distributed test: two left rows need opposite carryover directions.

        left@[10, 90], right@[5, 50, 95]:
          @10: dist(5)=5 < dist(50)=40 → backward@5  → w=50
          @90: dist(50)=40 > dist(95)=5 → forward@95 → w=950

        Both a backward pass (for @10) and a forward pass (for @90) must coexist.
        An implementation that only carries in one direction will fail one of these rows.
        """
        left = make_df({"ts": [10, 90], "v": [1, 2]}, repartition=n_partitions)
        right = make_df({"ts": [5, 50, 95], "w": [50, 500, 950]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [10, 90],
            "v": [1, 2],
            "w": [50, 950],
        }
