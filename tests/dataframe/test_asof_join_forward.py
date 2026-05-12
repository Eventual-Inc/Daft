from __future__ import annotations

import pytest

import daft
from daft import col


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
        assert pydict["w"][1] == 20   # ts=2, forward match is right@2=20
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
