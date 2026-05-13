from __future__ import annotations

import pytest

import daft


def get_n_partitions():
    return [1, 2, 4, 8]


class TestAsofJoinForwardMatchCorrectness:
    def test_exact_match(self):
        """Exact timestamp match is always picked in forward direction."""
        left = daft.from_pydict({"ts": [10, 20, 30], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 20, 30], "w": [11, 22, 33]})
        result = left.join_asof(right, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    def test_closest_later_when_no_exact(self):
        """Picks the smallest right value >= left value when there is no exact match."""
        left = daft.from_pydict({"a": [1.0, 2.0, 3.0], "b": ["p", "q", "r"]})
        right = daft.from_pydict({"a": [1.3, 2.4, 3.5], "c": ["s1", "s2", "s3"]})
        result = left.join_asof(right, on="a", strategy="forward").sort("a")
        assert result.to_pydict() == {
            "a": [1.0, 2.0, 3.0],
            "b": ["p", "q", "r"],
            "c": ["s1", "s2", "s3"],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_no_right_after_left_returns_null(self, make_df, n_partitions, with_default_morsel_size):
        """Left rows that come after all right rows get null (no future right event exists)."""
        left = make_df({"ts": [2, 6, 12], "v": [10, 20, 30]}, repartition=n_partitions)
        right = make_df({"ts": [4, 9], "w": [40, 90]}, repartition=n_partitions)
        result = left.join_asof(right, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [2, 6, 12], "v": [10, 20, 30], "w": [40, 90, None]}

    def test_all_left_after_all_right_all_nulls(self):
        """Every left row is after all right rows -- all nulls (no forward match possible)."""
        left = daft.from_pydict({"ts": [100, 200, 300], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [10, 50], "w": [11, 55]})
        result = left.join_asof(right, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [100, 200, 300], "v": [1, 2, 3], "w": [None, None, None]}

    def test_all_right_after_all_left_match_first(self):
        """Every right row is after all left rows -- each left matches the first (minimum) right row."""
        left = daft.from_pydict({"ts": [1, 2, 3], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [100, 200], "w": [11, 22]})
        result = left.join_asof(right, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [1, 2, 3], "w": [11, 11, 11]}

    def test_duplicate_left_timestamps(self):
        """Duplicate left timestamps each independently match the same nearest future right row."""
        left = daft.from_pydict({"ts": [6, 6, 11], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [8, 15], "w": [80, 150]})
        result = left.join_asof(right, on="ts", strategy="forward").sort(["ts", "v"])
        assert result.to_pydict() == {"ts": [6, 6, 11], "v": [1, 2, 3], "w": [80, 80, 150]}

    def test_duplicate_right_timestamps(self):
        """Multiple right rows at the same timestamp -- one is picked (either is valid)."""
        left = daft.from_pydict({"ts": [7], "v": [1]})
        right = daft.from_pydict({"ts": [7, 7], "w": [70, 77]})
        result = left.join_asof(right, on="ts", strategy="forward")
        assert result.to_pydict()["ts"] == [7]
        assert result.to_pydict()["v"] == [1]
        assert result.to_pydict()["w"][0] in [70, 77]

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_trades_quotes_with_by(self, make_df, n_partitions, with_default_morsel_size):
        """Forward asof: each trade matches the next quote at or after it, grouped by ticker."""
        trades = make_df(
            {
                "time": [2, 5, 5, 8],
                "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"],
                "price": [150, 2800, 155, 2850],
            },
            repartition=n_partitions,
        )
        quotes = make_df(
            {
                "time": [1, 3, 6, 9],
                "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"],
                "bid": [149, 2790, 153, 2860],
            },
            repartition=n_partitions,
        )
        result = trades.join_asof(quotes, on="time", by="ticker", strategy="forward").sort(["ticker", "time"])
        assert result.column_names == ["time", "ticker", "price", "bid"]
        assert result.to_pydict() == {
            "time": [2, 5, 5, 8],
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "price": [150, 155, 2800, 2850],
            # AAPL@2: next AAPL quote >= 2 is quote@6=153
            # AAPL@5: next AAPL quote >= 5 is quote@6=153
            # GOOG@5: next GOOG quote >= 5 is quote@9=2860
            # GOOG@8: next GOOG quote >= 8 is quote@9=2860
            "bid": [153, 153, 2860, 2860],
        }

    def test_float_asof_key_with_by(self):
        """Float asof key with group-by: picks the smallest right value >= left within each group."""
        left = daft.from_pydict(
            {
                "b": [0.0, 1.25, 2.5, 3.75, 5.0, 6.25, 7.5],
                "c": ["x", "x", "x", "x", "y", "y", "y"],
            }
        )
        right = daft.from_pydict(
            {
                "b": [1.0, 3.0, 5.0, 5.5, 7.0, 9.0],
                "c": ["x", "x", "x", "y", "y", "y"],
                "val": [10.0, 30.0, 50.0, 55.0, 70.0, 90.0],
            }
        )
        result = left.join_asof(right, on="b", by="c", strategy="forward").sort(["c", "b"])
        assert result.column_names == ["b", "c", "val"]
        assert result.to_pydict() == {
            "b": [0.0, 1.25, 2.5, 3.75, 5.0, 6.25, 7.5],
            "c": ["x", "x", "x", "x", "y", "y", "y"],
            # x@0.0: right x@1.0=10   (1.0 >= 0.0)
            # x@1.25: right x@3.0=30  (3.0 >= 1.25; 1.0 < 1.25 so skipped)
            # x@2.5:  right x@3.0=30  (3.0 >= 2.5)
            # x@3.75: right x@5.0=50  (5.0 >= 3.75)
            # y@5.0:  right y@5.5=55  (5.5 >= 5.0)
            # y@6.25: right y@7.0=70  (7.0 >= 6.25)
            # y@7.5:  right y@9.0=90  (9.0 >= 7.5)
            "val": [10.0, 30.0, 30.0, 50.0, 55.0, 70.0, 90.0],
        }

    def test_null_in_asof_key_left_produces_null(self):
        """Null asof keys on left produce null matches on the right side."""
        left = daft.from_pydict({"ts": [None, 2, None], "v": [1, 2, 3]})
        right = daft.from_pydict({"ts": [1, 2, 3], "w": [10, 20, 30]})
        result = left.join_asof(right, on="ts", strategy="forward").sort("v")
        pydict = result.to_pydict()
        assert pydict["v"] == [1, 2, 3]
        assert pydict["w"][0] is None  # ts=null
        assert pydict["w"][1] == 20  # ts=2, forward match is right@2=20
        assert pydict["w"][2] is None  # ts=null

    def test_null_in_by_key_no_cross_match(self):
        """Null by-keys do not match each other."""
        left = daft.from_pydict({"g": ["X", None], "ts": [5, 5], "v": [1, 2]})
        right = daft.from_pydict({"g": ["X", None], "ts": [7, 7], "w": [10, 20]})
        result = left.join_asof(right, on="ts", by="g", strategy="forward").sort("v")
        assert result.column_names == ["g", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [10, None]

    def test_sandwiched_left_row_backward_filled_with_by(self):
        """A left row with no direct match is backward-filled from its later group neighbour.

        left@7 has no right event in [7, 11), so it inherits the match of left@10 (right@11).
        """
        left = daft.from_pydict({"entity": ["A", "A", "A"], "ts": [3, 7, 10], "v": [1, 2, 3]})
        right = daft.from_pydict({"entity": ["A", "A"], "ts": [4, 11], "w": [40, 110]})
        result = left.join_asof(right, on="ts", by="entity", strategy="forward").sort("ts")
        assert result.to_pydict() == {
            "entity": ["A", "A", "A"],
            "ts": [3, 7, 10],
            "v": [1, 2, 3],
            # left@3:  right@4=40   (direct: 4 >= 3)
            # left@7:  right@11=110 (backward-filled from left@10; no right in [7, 11))
            # left@10: right@11=110 (direct: 11 >= 10)
            "w": [40, 110, 110],
        }

    def test_backward_fill_does_not_cross_group_boundaries(self):
        """Backward-fill within one group must not propagate into a different group."""
        left = daft.from_pydict(
            {
                "entity": ["A", "A", "A", "B", "B", "B"],
                "ts": [3, 7, 10, 15, 20, 25],
                "v": [1, 2, 3, 4, 5, 6],
            }
        )
        right = daft.from_pydict({"entity": ["A", "A"], "ts": [4, 11], "w": [40, 110]})
        result = left.join_asof(right, on="ts", by="entity", strategy="forward").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [3, 7, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5, 6],
            "w": [40, 110, 110, None, None, None],
        }


# ---------------------------------------------------------------------------
# Distributed Execution
# ---------------------------------------------------------------------------


class TestAsofJoinForwardDistributed:
    """Tests that exercise multi-partition forward carryover paths."""

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_multi_group_correctness(self, make_df, n_partitions, with_default_morsel_size):
        """Multiple entities spread across partitions all forward-match correctly."""
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
        result = left.join_asof(right, on="ts", by="entity", strategy="forward").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "ts": [10, 20, 10, 20, 10, 20, 10, 20],
            "v": [1, 5, 2, 6, 3, 7, 4, 8],
            # A@10: smallest right A >= 10 -> A@18=500; A@20: no right A >= 20 -> None
            # B@10: smallest right B >= 10 -> B@22=600; B@20: smallest right B >= 20 -> B@22=600
            # C@10: smallest right C >= 10 -> C@12=300; C@20: smallest right C >= 20 -> C@25=700
            # D@10: smallest right D >= 10 -> D@15=400; D@20: smallest right D >= 20 -> D@28=800
            "w": [500, None, 600, 600, 300, 700, 400, 800],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_no_by_keys_coalesces(self, make_df, n_partitions, with_default_morsel_size):
        """Forward asof join without by-keys on multi-partition data coalesces correctly."""
        left = make_df(
            {"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]},
            repartition=n_partitions,
        )
        right = make_df(
            {"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]},
            repartition=n_partitions,
        )
        result = left.join_asof(right, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            # 5:  smallest right >= 5  -> right@8=80
            # 10: smallest right >= 10 -> right@18=180
            # 15: smallest right >= 15 -> right@18=180
            # 20: smallest right >= 20 -> right@30=300
            # 25: smallest right >= 25 -> right@30=300
            "w": [80, 180, 180, 300, 300],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_interleaved_timestamps_across_entities(self, make_df, n_partitions, with_default_morsel_size):
        """Entities with interleaved timestamps where best match requires correct colocation."""
        left = make_df(
            {
                "entity": ["A", "B", "A", "B", "A", "B"],
                "ts": [1, 2, 3, 4, 5, 6],
                "v": [10, 20, 30, 40, 50, 60],
            },
            repartition=n_partitions,
        )
        right = make_df(
            {
                "entity": ["A", "B", "A", "B"],
                "ts": [2, 3, 4, 5],
                "w": [200, 300, 400, 500],
            },
            repartition=n_partitions,
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="forward").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [1, 3, 5, 2, 4, 6],
            "v": [10, 30, 50, 20, 40, 60],
            # A@1: smallest right A >= 1 -> A@2=200; A@3: smallest right A >= 3 -> A@4=400
            # A@5: no right A >= 5 -> None
            # B@2: smallest right B >= 2 -> B@3=300; B@4: smallest right B >= 4 -> B@5=500
            # B@6: no right B >= 6 -> None
            "w": [200, 400, None, 300, 500, None],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_right_carryover_crosses_bucket_boundary(self, make_df, n_partitions, with_default_morsel_size):
        """Left rows in an early bucket match right rows in a later bucket via forward carryover."""
        left = make_df(
            {"ts": [1, 2, 3], "v": [10, 20, 30]},
            repartition=n_partitions,
        )
        right = make_df(
            {"ts": [100, 200], "w": [1000, 2000]},
            repartition=n_partitions,
        )
        result = left.join_asof(right, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {
            "ts": [1, 2, 3],
            "v": [10, 20, 30],
            # all left rows are before all right rows; each should match right@100=1000
            "w": [1000, 1000, 1000],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_last_bucket_gets_no_carryover(self, make_df, n_partitions, with_default_morsel_size):
        """Left rows in the last bucket with no right row after them get null."""
        left = make_df(
            {"ts": [10, 20, 100], "v": [1, 2, 3]},
            repartition=n_partitions,
        )
        right = make_df(
            {"ts": [15, 25], "w": [150, 250]},
            repartition=n_partitions,
        )
        result = left.join_asof(right, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {
            "ts": [10, 20, 100],
            "v": [1, 2, 3],
            # ts=10: right@15=150; ts=20: right@25=250; ts=100: no right >= 100 -> None
            "w": [150, 250, None],
        }

    @pytest.mark.parametrize(
        "left_partitions,right_partitions",
        [(4, 2), (2, 4), (8, 2), (2, 8)],
    )
    def test_asymmetric_partition_counts(self, make_df, left_partitions, right_partitions, with_default_morsel_size):
        """Left and right are both hash-partitioned on the by-key with different partition counts."""
        left = make_df(
            {
                "entity": ["A", "B", "C", "D", "A", "B", "C", "D"],
                "ts": [10, 10, 10, 10, 20, 20, 20, 20],
                "v": [1, 2, 3, 4, 5, 6, 7, 8],
            },
            repartition=left_partitions,
            repartition_columns=["entity"],
        )
        right = make_df(
            {
                "entity": ["A", "B", "C", "D", "A", "B", "C", "D"],
                "ts": [5, 8, 12, 15, 18, 22, 25, 28],
                "w": [100, 200, 300, 400, 500, 600, 700, 800],
            },
            repartition=right_partitions,
            repartition_columns=["entity"],
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="forward").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "ts": [10, 20, 10, 20, 10, 20, 10, 20],
            "v": [1, 5, 2, 6, 3, 7, 4, 8],
            # A@10: right A@18=500; A@20: no right A >= 20 -> None
            # B@10: right B@22=600; B@20: right B@22=600
            # C@10: right C@12=300; C@20: right C@25=700
            # D@10: right D@15=400; D@20: right D@28=800
            "w": [500, None, 600, 600, 300, 700, 400, 800],
        }

    @pytest.mark.parametrize(
        "left_partitions,right_partitions,hash_partition_left",
        [(4, 2, True), (2, 4, True), (4, 2, False), (2, 4, False)],
    )
    def test_one_side_not_hash_partitioned(
        self, make_df, left_partitions, right_partitions, hash_partition_left, with_default_morsel_size
    ):
        """One side is hash-partitioned on the by-key; the other has a Random clustering spec."""
        left = make_df(
            {
                "entity": ["A", "B", "C", "D", "A", "B", "C", "D"],
                "ts": [10, 10, 10, 10, 20, 20, 20, 20],
                "v": [1, 2, 3, 4, 5, 6, 7, 8],
            },
            repartition=left_partitions,
            repartition_columns=["entity"] if hash_partition_left else [],
        )
        right = make_df(
            {
                "entity": ["A", "B", "C", "D", "A", "B", "C", "D"],
                "ts": [5, 8, 12, 15, 18, 22, 25, 28],
                "w": [100, 200, 300, 400, 500, 600, 700, 800],
            },
            repartition=right_partitions,
            repartition_columns=[] if hash_partition_left else ["entity"],
        )
        result = left.join_asof(right, on="ts", by="entity", strategy="forward").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "ts": [10, 20, 10, 20, 10, 20, 10, 20],
            "v": [1, 5, 2, 6, 3, 7, 4, 8],
            "w": [500, None, 600, 600, 300, 700, 400, 800],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_child_join_same_by_keys(self, make_df, n_partitions, with_default_morsel_size):
        """Forward asof where left input is a join already hashed on the same by-keys."""
        base_left = make_df(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"],
                "id": [1, 2, 3, 4],
                "ts": [10, 10, 20, 20],
            },
            repartition=n_partitions,
        )
        base_right = make_df(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"],
                "id": [1, 2, 3, 4],
                "label": ["a", "b", "c", "d"],
            },
            repartition=n_partitions,
        )
        joined_left = base_left.join(base_right, on=["ticker", "id"])

        asof_right = make_df(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"],
                "ts": [5, 8, 15, 18],
                "w": [50, 80, 150, 180],
            },
            repartition=n_partitions,
        )
        result = joined_left.join_asof(asof_right, on="ts", by="ticker", strategy="forward").sort(["ticker", "ts"])
        assert result.to_pydict() == {
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "id": [1, 3, 2, 4],
            "ts": [10, 20, 10, 20],
            "label": ["a", "c", "b", "d"],
            # AAPL@10: smallest right AAPL >= 10 -> AAPL@15=150; AAPL@20: no right AAPL >= 20 -> None
            # GOOG@10: smallest right GOOG >= 10 -> GOOG@18=180; GOOG@20: no right GOOG >= 20 -> None
            "w": [150, None, 180, None],
        }

    @pytest.mark.parametrize("n_partitions", get_n_partitions())
    def test_child_join_different_by_keys(self, make_df, n_partitions, with_default_morsel_size):
        """Forward asof where left input is a join hashed on different keys than the asof by-keys."""
        base_left = make_df(
            {
                "id": [1, 2, 3, 4],
                "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"],
                "ts": [10, 10, 20, 20],
            },
            repartition=n_partitions,
        )
        base_right = make_df(
            {
                "id": [1, 2, 3, 4],
                "label": ["a", "b", "c", "d"],
            },
            repartition=n_partitions,
        )
        joined_left = base_left.join(base_right, on="id")

        asof_right = make_df(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "GOOG"],
                "ts": [8, 7, 15, 18],
                "w": [80, 70, 150, 180],
            },
            repartition=n_partitions,
        )
        result = joined_left.join_asof(asof_right, on="ts", by="ticker", strategy="forward").sort(["ticker", "ts"])
        assert result.to_pydict() == {
            "id": [1, 3, 2, 4],
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "ts": [10, 20, 10, 20],
            "label": ["a", "c", "b", "d"],
            # AAPL@10: smallest right AAPL >= 10 -> AAPL@15=150; AAPL@20: no right AAPL >= 20 -> None
            # GOOG@10: smallest right GOOG >= 10 -> GOOG@18=180; GOOG@20: no right GOOG >= 20 -> None
            "w": [150, None, 180, None],
        }
