"""Correctness tests for asof join with _assume_sorted_and_aligned=True.

These tests mirror the correctness checks in
test_asof_join.py / test_asof_join_forward.py / test_asof_join_nearest.py
but exercise the aligned code path so that algorithm changes in either the
distributed or local execution layer are caught here too.

Only runs under the Ray runner because _assume_sorted_and_aligned is a
distributed-planner feature.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import daft
from daft.context import execution_config_ctx
from daft.exceptions import DaftCoreException
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="_assume_sorted_and_aligned is a distributed-planner feature (ray runner only)",
)


@pytest.fixture(autouse=True)
def disable_scan_task_split_and_merge():
    with execution_config_ctx(enable_scan_task_split_and_merge=False):
        yield


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------


class _InMemoryTask(DataSourceTask):
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        yield RecordBatch.from_arrow_table(self._table)


class AlignedSource(DataSource):
    """Emits pre-split partitions as individual scan tasks.

    No clustering hint is declared — _assume_sorted_and_aligned=True on the
    join call is the only mechanism needed.
    """

    def __init__(self, name: str, partitions: list[pa.Table]) -> None:
        self._name = name
        self._partitions = partitions

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._partitions[0].schema)

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        for part in self._partitions:
            yield _InMemoryTask(part)


def aligned(*partitions: pa.Table, name: str = "source") -> daft.DataFrame:
    """Shorthand: wrap explicit partition tables in an AlignedSource DataFrame."""
    return AlignedSource(name, list(partitions)).read()


def split(table: pa.Table, key: str, boundary) -> tuple[pa.Table, pa.Table]:
    """Split table into rows where key < boundary and key >= boundary."""
    mask = pc.less(table[key], boundary)
    return table.filter(mask), table.filter(pc.invert(mask))


# ---------------------------------------------------------------------------
# Backward strategy
# ---------------------------------------------------------------------------


class TestAlignedAsofJoinBackwardCorrectness:
    def test_exact_match(self):
        tbl_l = pa.table({"ts": [10, 20, 30], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [10, 20, 30], "w": [11, 22, 33]})
        lp0, lp1 = split(tbl_l, "ts", 15)
        rp0, rp1 = split(tbl_r, "ts", 15)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="backward").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    def test_closest_earlier_when_no_exact(self):
        tbl_l = pa.table({"a": [1.0, 2.0, 3.0], "b": ["p", "q", "r"]})
        tbl_r = pa.table({"a": [0.4, 1.6, 2.7], "c": ["s1", "s2", "s3"]})
        lp0, lp1 = split(tbl_l, "a", 2.0)
        rp0, rp1 = split(tbl_r, "a", 2.0)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="a").sort("a")
        assert result.to_pydict() == {"a": [1.0, 2.0, 3.0], "b": ["p", "q", "r"], "c": ["s1", "s2", "s3"]}

    def test_no_right_before_left_returns_null(self):
        tbl_l = pa.table({"ts": [2, 6, 12], "v": [10, 20, 30]})
        tbl_r = pa.table({"ts": [4, 9], "w": [40, 90]})
        lp0, lp1 = split(tbl_l, "ts", 8)
        rp0, rp1 = split(tbl_r, "ts", 8)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {"ts": [2, 6, 12], "v": [10, 20, 30], "w": [None, 40, 90]}

    def test_all_left_before_all_right_all_nulls(self):
        tbl_l = pa.table({"ts": [1, 2, 3], "v": [10, 20, 30]})
        tbl_r = pa.table({"ts": [100, 200], "w": [11, 22]})
        lp0, lp1 = split(tbl_l, "ts", 2)
        rp0, rp1 = split(tbl_r, "ts", 2)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [None, None, None]}

    def test_all_right_before_all_left_match_last(self):
        tbl_l = pa.table({"ts": [100, 200, 300], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [10, 50], "w": [11, 55]})
        lp0, lp1 = split(tbl_l, "ts", 150)
        rp0, rp1 = split(tbl_r, "ts", 150)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {"ts": [100, 200, 300], "v": [1, 2, 3], "w": [55, 55, 55]}

    def test_duplicate_left_timestamps(self):
        tbl_l = pa.table({"ts": [6, 6, 11], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [4, 8], "w": [40, 80]})
        lp0, lp1 = split(tbl_l, "ts", 8)
        rp0, rp1 = split(tbl_r, "ts", 8)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort(["ts", "v"])
        assert result.to_pydict() == {"ts": [6, 6, 11], "v": [1, 2, 3], "w": [40, 40, 80]}

    def test_duplicate_right_timestamps(self):
        tbl_l = pa.table({"ts": [3, 7], "v": [1, 2]})
        tbl_r = pa.table({"ts": [4, 7, 7], "w": [40, 70, 77]})
        lp0, lp1 = split(tbl_l, "ts", 5)
        rp0, rp1 = split(tbl_r, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [3, 7]
        assert pydict["w"][0] is None  # ts=3 backward: right=4>3 → None
        assert pydict["w"][1] in [70, 77]  # ts=7 exact match duplicate

    def test_trades_quotes_with_by(self):
        # Partitioned by ticker (composite key): AAPL in partition 0, GOOG in partition 1.
        tp0 = pa.table({"time": [2, 5], "ticker": ["AAPL", "AAPL"], "price": [150, 155]})
        tp1 = pa.table({"time": [5, 8], "ticker": ["GOOG", "GOOG"], "price": [2800, 2850]})
        qp0 = pa.table({"time": [1, 6], "ticker": ["AAPL", "AAPL"], "bid": [149, 153]})
        qp1 = pa.table({"time": [3, 9], "ticker": ["GOOG", "GOOG"], "bid": [2790, 2860]})
        left = aligned(tp0, tp1, name="trades")
        right = aligned(qp0, qp1, name="quotes")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="time", by="ticker").sort(["ticker", "time"])
        assert result.column_names == ["time", "ticker", "price", "bid"]
        assert result.to_pydict() == {
            "time": [2, 5, 5, 8],
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "price": [150, 155, 2800, 2850],
            "bid": [149, 149, 2790, 2790],
        }

    def test_null_in_asof_key_left_produces_null(self):
        lp0 = pa.table({"ts": pa.array([None], type=pa.int64()), "v": [1]})
        lp1 = pa.table({"ts": pa.array([2, None], type=pa.int64()), "v": [2, 3]})
        rp0 = pa.table({"ts": [1], "w": [10]})
        rp1 = pa.table({"ts": [2, 3], "w": [20, 30]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("v")
        pydict = result.to_pydict()
        assert pydict["v"] == [1, 2, 3]
        assert pydict["w"][0] is None
        assert pydict["w"][1] == 20
        assert pydict["w"][2] is None

    def test_null_in_by_key_no_cross_match(self):
        # Partition by composite key (g, ts): g=None in p0 (empty right), g="X" in p1.
        lp0 = pa.table({"g": pa.array([None, None], type=pa.string()), "ts": [3, 5], "v": [2, 4]})
        lp1 = pa.table({"g": ["X", "X"], "ts": [3, 5], "v": [1, 3]})
        rp0 = pa.table(
            {
                "g": pa.array([], type=pa.string()),
                "ts": pa.array([], type=pa.int64()),
                "w": pa.array([], type=pa.int64()),
            }
        )
        rp1 = pa.table({"g": ["X", "X"], "ts": [2, 4], "w": [20, 40]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g").sort("v")
        assert result.column_names == ["g", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [20, None, 40, None]

    def test_sandwiched_left_row_forward_filled_with_by(self):
        tbl_l = pa.table({"entity": ["A", "A", "A"], "ts": [3, 7, 10], "v": [1, 2, 3]})
        tbl_r = pa.table({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]})
        lp0, lp1 = split(tbl_l, "ts", 6)
        rp0, rp1 = split(tbl_r, "ts", 6)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort("ts")
        assert result.to_pydict() == {
            "entity": ["A", "A", "A"],
            "ts": [3, 7, 10],
            "v": [1, 2, 3],
            "w": [20, 20, 80],
        }

    def test_forward_fill_does_not_cross_group_boundaries(self):
        # Partition by composite key (entity, ts): entity A in p0, entity B in p1.
        lp0 = pa.table({"entity": ["A", "A", "A"], "ts": [3, 7, 10], "v": [1, 2, 3]})
        lp1 = pa.table({"entity": ["B", "B", "B"], "ts": [15, 20, 25], "v": [4, 5, 6]})
        rp0 = pa.table({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]})
        rp1 = pa.table(
            {
                "entity": pa.array([], type=pa.string()),
                "ts": pa.array([], type=pa.int64()),
                "w": pa.array([], type=pa.int64()),
            }
        )
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [3, 7, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5, 6],
            "w": [20, 20, 80, None, None, None],
        }


class TestAlignedAsofJoinBackwardEmptyTables:
    def test_empty_left_table(self):
        left = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            name="left",
        )
        right = aligned(
            pa.table({"ts": [1, 2], "w": [10, 20]}),
            pa.table({"ts": [3], "w": [30]}),
            name="right",
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}

    def test_empty_right_table(self):
        left = aligned(
            pa.table({"ts": [1, 2], "v": [10, 20]}),
            pa.table({"ts": [3], "v": [30]}),
            name="left",
        )
        right = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            name="right",
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [None, None, None]}

    def test_empty_left_and_right_tables(self):
        left = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            name="left",
        )
        right = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            name="right",
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}


class TestAlignedAsofJoinBackwardDistributed:
    def test_no_by_keys_coalesces(self):
        tbl_l = pa.table({"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]})
        tbl_r = pa.table({"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]})
        lp0, lp1 = split(tbl_l, "ts", 15)
        rp0, rp1 = split(tbl_r, "ts", 15)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            "w": [30, 80, 80, 180, 180],
        }

    def test_multi_group_correctness(self):
        # Partitioned by entity (A+B in partition 0, C+D in partition 1), sorted by ts within each.
        lp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [10, 20, 10, 20], "v": [1, 5, 2, 6]})
        lp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [10, 20, 10, 20], "v": [3, 7, 4, 8]})
        rp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [5, 18, 8, 22], "w": [100, 500, 200, 600]})
        rp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [12, 25, 15, 28], "w": [300, 700, 400, 800]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "ts": [10, 20, 10, 20, 10, 20, 10, 20],
            "v": [1, 5, 2, 6, 3, 7, 4, 8],
            "w": [100, 500, 200, 200, None, 300, None, 400],
        }

    def test_interleaved_timestamps_across_entities(self):
        # Partition by composite key (entity, ts): all A rows in p0, all B rows in p1.
        lp0 = pa.table({"entity": ["A", "A", "A"], "ts": [1, 3, 5], "v": [10, 30, 50]})
        lp1 = pa.table({"entity": ["B", "B", "B"], "ts": [2, 4, 6], "v": [20, 40, 60]})
        rp0 = pa.table({"entity": ["A", "A"], "ts": [2, 4], "w": [200, 400]})
        rp1 = pa.table({"entity": ["B", "B"], "ts": [3, 5], "w": [300, 500]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity").sort(["entity", "ts"])
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [1, 3, 5, 2, 4, 6],
            "v": [10, 30, 50, 20, 40, 60],
            "w": [None, 200, 400, None, 300, 500],
        }


# ---------------------------------------------------------------------------
# Forward strategy
# ---------------------------------------------------------------------------


class TestAlignedAsofJoinForwardCorrectness:
    def test_exact_match(self):
        tbl_l = pa.table({"ts": [10, 20, 30], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [10, 20, 30], "w": [11, 22, 33]})
        lp0, lp1 = split(tbl_l, "ts", 15)
        rp0, rp1 = split(tbl_r, "ts", 15)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    def test_closest_later_when_no_exact(self):
        tbl_l = pa.table({"a": [1.0, 2.0, 3.0], "b": ["p", "q", "r"]})
        tbl_r = pa.table({"a": [1.3, 2.4, 3.5], "c": ["s1", "s2", "s3"]})
        lp0, lp1 = split(tbl_l, "a", 2.0)
        rp0, rp1 = split(tbl_r, "a", 2.0)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="a", strategy="forward").sort("a")
        assert result.to_pydict() == {"a": [1.0, 2.0, 3.0], "b": ["p", "q", "r"], "c": ["s1", "s2", "s3"]}

    def test_no_right_after_left_returns_null(self):
        tbl_l = pa.table({"ts": [2, 6, 12], "v": [10, 20, 30]})
        tbl_r = pa.table({"ts": [4, 9], "w": [40, 90]})
        lp0, lp1 = split(tbl_l, "ts", 8)
        rp0, rp1 = split(tbl_r, "ts", 8)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [2, 6, 12], "v": [10, 20, 30], "w": [40, 90, None]}

    def test_all_left_after_all_right_all_nulls(self):
        tbl_l = pa.table({"ts": [100, 200, 300], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [10, 50], "w": [11, 55]})
        lp0, lp1 = split(tbl_l, "ts", 150)
        rp0, rp1 = split(tbl_r, "ts", 150)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [100, 200, 300], "v": [1, 2, 3], "w": [None, None, None]}

    def test_all_right_after_all_left_match_first(self):
        tbl_l = pa.table({"ts": [1, 2, 3], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [100, 200], "w": [11, 22]})
        lp0, lp1 = split(tbl_l, "ts", 2)
        rp0, rp1 = split(tbl_r, "ts", 2)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [1, 2, 3], "w": [11, 11, 11]}

    def test_duplicate_left_timestamps(self):
        tbl_l = pa.table({"ts": [6, 6, 11], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [8, 15], "w": [80, 150]})
        lp0, lp1 = split(tbl_l, "ts", 8)
        rp0, rp1 = split(tbl_r, "ts", 8)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort(["ts", "v"])
        assert result.to_pydict() == {"ts": [6, 6, 11], "v": [1, 2, 3], "w": [80, 80, 150]}

    def test_duplicate_right_timestamps(self):
        tbl_l = pa.table({"ts": [3, 7], "v": [1, 2]})
        tbl_r = pa.table({"ts": [4, 7, 7], "w": [40, 70, 77]})
        lp0, lp1 = split(tbl_l, "ts", 5)
        rp0, rp1 = split(tbl_r, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [3, 7]
        assert pydict["w"][0] == 40  # ts=3 forward: min(ts≥3) from {4} = 4→w=40
        assert pydict["w"][1] in [70, 77]  # ts=7 exact match duplicate

    def test_trades_quotes_with_by(self):
        # Partitioned by ticker: AAPL in partition 0, GOOG in partition 1.
        tp0 = pa.table({"time": [2, 5], "ticker": ["AAPL", "AAPL"], "price": [150, 155]})
        tp1 = pa.table({"time": [5, 8], "ticker": ["GOOG", "GOOG"], "price": [2800, 2850]})
        qp0 = pa.table({"time": [1, 6], "ticker": ["AAPL", "AAPL"], "bid": [149, 153]})
        qp1 = pa.table({"time": [3, 9], "ticker": ["GOOG", "GOOG"], "bid": [2790, 2860]})
        left = aligned(tp0, tp1, name="trades")
        right = aligned(qp0, qp1, name="quotes")
        result = left.join_asof(
            right, _assume_sorted_and_aligned=True, on="time", by="ticker", strategy="forward"
        ).sort(["ticker", "time"])
        assert result.column_names == ["time", "ticker", "price", "bid"]
        assert result.to_pydict() == {
            "time": [2, 5, 5, 8],
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "price": [150, 155, 2800, 2850],
            "bid": [153, 153, 2860, 2860],
        }

    def test_null_in_asof_key_left_produces_null(self):
        lp0 = pa.table({"ts": pa.array([None], type=pa.int64()), "v": [1]})
        lp1 = pa.table({"ts": pa.array([2, None], type=pa.int64()), "v": [2, 3]})
        rp0 = pa.table({"ts": [1], "w": [10]})
        rp1 = pa.table({"ts": [2, 3], "w": [20, 30]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("v")
        pydict = result.to_pydict()
        assert pydict["v"] == [1, 2, 3]
        assert pydict["w"][0] is None
        assert pydict["w"][1] == 20
        assert pydict["w"][2] is None

    def test_null_in_by_key_no_cross_match(self):
        # Partition by composite key (g, ts): g=None in p0 (empty right), g="X" in p1.
        lp0 = pa.table({"g": pa.array([None, None], type=pa.string()), "ts": [3, 5], "v": [2, 4]})
        lp1 = pa.table({"g": ["X", "X"], "ts": [3, 5], "v": [1, 3]})
        rp0 = pa.table(
            {
                "g": pa.array([], type=pa.string()),
                "ts": pa.array([], type=pa.int64()),
                "w": pa.array([], type=pa.int64()),
            }
        )
        rp1 = pa.table({"g": ["X", "X"], "ts": [4, 6], "w": [40, 60]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g", strategy="forward").sort("v")
        assert result.column_names == ["g", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [40, None, 60, None]

    def test_sandwiched_left_row_backward_filled_with_by(self):
        tbl_l = pa.table({"entity": ["A", "A", "A"], "ts": [3, 7, 10], "v": [1, 2, 3]})
        tbl_r = pa.table({"entity": ["A", "A"], "ts": [4, 11], "w": [40, 110]})
        lp0, lp1 = split(tbl_l, "ts", 6)
        rp0, rp1 = split(tbl_r, "ts", 6)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="forward").sort(
            "ts"
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "A"],
            "ts": [3, 7, 10],
            "v": [1, 2, 3],
            "w": [40, 110, 110],
        }

    def test_backward_fill_does_not_cross_group_boundaries(self):
        tbl_l = pa.table(
            {"entity": ["A", "A", "A", "B", "B", "B"], "ts": [3, 7, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5, 6]}
        )
        tbl_r = pa.table({"entity": ["A", "A"], "ts": [4, 11], "w": [40, 110]})
        lp0, lp1 = split(tbl_l, "ts", 12)
        rp0, rp1 = split(tbl_r, "ts", 12)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="forward").sort(
            ["entity", "ts"]
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [3, 7, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5, 6],
            "w": [40, 110, 110, None, None, None],
        }


class TestAlignedAsofJoinForwardDistributed:
    def test_no_by_keys_coalesces(self):
        tbl_l = pa.table({"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]})
        tbl_r = pa.table({"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]})
        lp0, lp1 = split(tbl_l, "ts", 15)
        rp0, rp1 = split(tbl_r, "ts", 15)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            "w": [80, 180, 180, 300, 300],
        }

    def test_multi_group_correctness(self):
        # Partitioned by entity (A+B in partition 0, C+D in partition 1), sorted by ts within each.
        lp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [10, 20, 10, 20], "v": [1, 5, 2, 6]})
        lp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [10, 20, 10, 20], "v": [3, 7, 4, 8]})
        rp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [5, 18, 8, 22], "w": [100, 500, 200, 600]})
        rp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [12, 25, 15, 28], "w": [300, 700, 400, 800]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="forward").sort(
            ["entity", "ts"]
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "ts": [10, 20, 10, 20, 10, 20, 10, 20],
            "v": [1, 5, 2, 6, 3, 7, 4, 8],
            "w": [500, None, 600, 600, 300, 700, 400, 800],
        }

    def test_interleaved_timestamps_across_entities(self):
        # Partition by composite key (entity, ts): all A rows in p0, all B rows in p1.
        lp0 = pa.table({"entity": ["A", "A", "A"], "ts": [1, 3, 5], "v": [10, 30, 50]})
        lp1 = pa.table({"entity": ["B", "B", "B"], "ts": [2, 4, 6], "v": [20, 40, 60]})
        rp0 = pa.table({"entity": ["A", "A"], "ts": [2, 4], "w": [200, 400]})
        rp1 = pa.table({"entity": ["B", "B"], "ts": [3, 5], "w": [300, 500]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="forward").sort(
            ["entity", "ts"]
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [1, 3, 5, 2, 4, 6],
            "v": [10, 30, 50, 20, 40, 60],
            "w": [200, 400, None, 300, 500, None],
        }

    def test_right_carryover_crosses_bucket_boundary(self):
        tbl_l = pa.table({"ts": [1, 2, 3], "v": [10, 20, 30]})
        tbl_r = pa.table({"ts": [100, 200], "w": [1000, 2000]})
        lp0, lp1 = split(tbl_l, "ts", 2)
        rp0, rp1 = split(tbl_r, "ts", 2)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [1000, 1000, 1000]}

    def test_last_bucket_gets_no_carryover(self):
        tbl_l = pa.table({"ts": [10, 20, 100], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [15, 25], "w": [150, 250]})
        lp0, lp1 = split(tbl_l, "ts", 50)
        rp0, rp1 = split(tbl_r, "ts", 50)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="forward").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 100], "v": [1, 2, 3], "w": [150, 250, None]}


# ---------------------------------------------------------------------------
# Nearest strategy
# ---------------------------------------------------------------------------


class TestAlignedAsofJoinNearestCorrectness:
    def test_exact_match(self):
        tbl_l = pa.table({"ts": [10, 20, 30], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [10, 20, 30], "w": [11, 22, 33]})
        lp0, lp1 = split(tbl_l, "ts", 15)
        rp0, rp1 = split(tbl_r, "ts", 15)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [10, 20, 30], "v": [1, 2, 3], "w": [11, 22, 33]}

    def test_picks_backward_when_closer(self):
        tbl_l = pa.table({"ts": [3, 10], "v": [1, 2]})
        tbl_r = pa.table({"ts": [2, 7, 14], "w": [20, 70, 140]})
        lp0, lp1 = split(tbl_l, "ts", 5)
        rp0, rp1 = split(tbl_r, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [3, 10]
        assert pydict["w"][0] == 20  # ts=3 nearest: backward 2(|1|) vs forward 7(|4|) → 2
        assert pydict["w"][1] == 70  # ts=10 nearest: 7(|3|) vs 14(|4|) vs carryover 2(|8|) → 7 (backward closer)

    def test_picks_forward_when_closer(self):
        tbl_l = pa.table({"ts": [3, 10], "v": [1, 2]})
        tbl_r = pa.table({"ts": [2, 6, 13], "w": [20, 60, 130]})
        lp0, lp1 = split(tbl_l, "ts", 5)
        rp0, rp1 = split(tbl_r, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [3, 10]
        assert pydict["w"][0] == 20  # ts=3: backward 2(|1|) vs forward 6(|3|) → 2
        assert pydict["w"][1] == 130  # ts=10: 6(|4|) vs 13(|3|) → 13 (forward closer)

    def test_direction_switches_within_sequence(self):
        tbl_l = pa.table({"ts": [3, 8, 13], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [5, 11], "w": [50, 110]})
        lp0, lp1 = split(tbl_l, "ts", 8)
        rp0, rp1 = split(tbl_r, "ts", 8)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [3, 8, 13], "v": [1, 2, 3], "w": [50, 110, 110]}

    def test_all_left_before_all_right_no_nulls(self):
        tbl_l = pa.table({"ts": [1, 2, 3], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [100, 200], "w": [1000, 2000]})
        lp0, lp1 = split(tbl_l, "ts", 2)
        rp0, rp1 = split(tbl_r, "ts", 2)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [1, 2, 3], "w": [1000, 1000, 1000]}

    def test_all_left_after_all_right_no_nulls(self):
        tbl_l = pa.table({"ts": [100, 200, 300], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [10, 50], "w": [100, 500]})
        lp0, lp1 = split(tbl_l, "ts", 150)
        rp0, rp1 = split(tbl_r, "ts", 150)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [100, 200, 300], "v": [1, 2, 3], "w": [500, 500, 500]}

    def test_single_right_row_always_matches(self):
        tbl_l = pa.table({"ts": [1, 50, 99], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [25, 50], "w": [250, 500]})
        lp0, lp1 = split(tbl_l, "ts", 50)
        rp0, rp1 = split(tbl_r, "ts", 50)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [1, 50, 99], "v": [1, 2, 3], "w": [250, 500, 500]}

    def test_direction_mix_across_longer_sequence(self):
        tbl_l = pa.table({"ts": [5, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5]})
        tbl_r = pa.table({"ts": [3, 8, 18, 30], "w": [30, 80, 180, 300]})
        lp0, lp1 = split(tbl_l, "ts", 15)
        rp0, rp1 = split(tbl_r, "ts", 15)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {
            "ts": [5, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5],
            "w": [30, 80, 180, 180, 300],
        }


class TestAlignedAsofJoinNearestTieBreaking:
    def test_equidistant_forward_preferred(self):
        tbl_l = pa.table({"ts": [3, 10], "v": [1, 2]})
        tbl_r = pa.table({"ts": [2, 8, 12], "w": [20, 80, 120]})
        lp0, lp1 = split(tbl_l, "ts", 5)
        rp0, rp1 = split(tbl_r, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [3, 10]
        assert pydict["w"][0] == 20  # ts=3: backward 2(|1|) → 2
        assert pydict["w"][1] == 120  # ts=10: 8(|2|) vs 12(|2|) equidistant, prefer forward → 12

    def test_exact_match_beats_equidistant_candidates(self):
        tbl_l = pa.table({"ts": [3, 10], "v": [1, 2]})
        tbl_r = pa.table({"ts": [2, 8, 10, 12], "w": [20, 80, 100, 120]})
        lp0, lp1 = split(tbl_l, "ts", 5)
        rp0, rp1 = split(tbl_r, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [3, 10]
        assert pydict["w"][0] == 20  # ts=3: backward 2(|1|) → 2
        assert pydict["w"][1] == 100  # ts=10: exact match beats equidistant 8 and 12

    def test_one_right_row_matches_multiple_left_rows(self):
        tbl_l = pa.table({"ts": [10, 15], "v": [1, 2]})
        tbl_r = pa.table({"ts": [7, 13], "w": [70, 130]})
        lp0, lp1 = split(tbl_l, "ts", 12)
        rp0, rp1 = split(tbl_r, "ts", 12)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [10, 15], "v": [1, 2], "w": [130, 130]}

    def test_duplicate_left_timestamps(self):
        tbl_l = pa.table({"ts": [10, 10, 20], "v": [1, 2, 3]})
        tbl_r = pa.table({"ts": [8, 15], "w": [80, 150]})
        lp0, lp1 = split(tbl_l, "ts", 15)
        rp0, rp1 = split(tbl_r, "ts", 15)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort(["ts", "v"])
        assert result.to_pydict() == {"ts": [10, 10, 20], "v": [1, 2, 3], "w": [80, 80, 150]}


class TestAlignedAsofJoinNearestNullHandling:
    def test_null_in_asof_key_left_produces_null(self):
        lp0 = pa.table({"ts": pa.array([None], type=pa.int64()), "v": [1]})
        lp1 = pa.table({"ts": pa.array([5, None], type=pa.int64()), "v": [2, 3]})
        rp0 = pa.table({"ts": [3], "w": [30]})
        rp1 = pa.table({"ts": [5, 7], "w": [50, 70]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("v")
        pydict = result.to_pydict()
        assert pydict["v"] == [1, 2, 3]
        assert pydict["w"][0] is None
        assert pydict["w"][1] == 50
        assert pydict["w"][2] is None

    def test_null_in_by_key_no_cross_match(self):
        # Partition by composite key (g, ts): g=None in p0 (empty right), g="X" in p1.
        lp0 = pa.table({"g": pa.array([None, None], type=pa.string()), "ts": [3, 5], "v": [2, 4]})
        lp1 = pa.table({"g": ["X", "X"], "ts": [3, 5], "v": [1, 3]})
        rp0 = pa.table(
            {
                "g": pa.array([], type=pa.string()),
                "ts": pa.array([], type=pa.int64()),
                "w": pa.array([], type=pa.int64()),
            }
        )
        rp1 = pa.table({"g": ["X", "X"], "ts": [2, 4], "w": [20, 40]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g", strategy="nearest").sort("v")
        assert result.column_names == ["g", "ts", "v", "w"]
        assert result.to_pydict()["w"] == [40, None, 40, None]


class TestAlignedAsofJoinNearestWithBy:
    def test_nearest_per_group_different_directions(self):
        lp0 = pa.table({"entity": ["A"], "ts": [10], "v": [1]})
        lp1 = pa.table({"entity": ["B"], "ts": [10], "v": [2]})
        rp0 = pa.table({"entity": ["A", "A"], "ts": [7, 14], "w": [70, 140]})
        rp1 = pa.table({"entity": ["B", "B"], "ts": [6, 13], "w": [60, 130]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="nearest").sort(
            "entity"
        )
        assert result.to_pydict() == {"entity": ["A", "B"], "ts": [10, 10], "v": [1, 2], "w": [70, 130]}

    def test_only_right_after_all_left_in_group_no_nulls(self):
        tbl_l = pa.table({"entity": ["A", "A", "A"], "ts": [1, 2, 3], "v": [1, 2, 3]})
        tbl_r = pa.table({"entity": ["A", "A"], "ts": [5, 100], "w": [50, 1000]})
        lp0, lp1 = split(tbl_l, "ts", 2)
        rp0, rp1 = split(tbl_r, "ts", 2)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="nearest").sort(
            "ts"
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "A"],
            "ts": [1, 2, 3],
            "v": [1, 2, 3],
            "w": [50, 50, 50],
        }

    def test_only_right_before_all_left_in_group_no_nulls(self):
        tbl_l = pa.table({"entity": ["A", "A"], "ts": [100, 200], "v": [1, 2]})
        tbl_r = pa.table({"entity": ["A", "A"], "ts": [5, 150], "w": [50, 1500]})
        lp0, lp1 = split(tbl_l, "ts", 150)
        rp0, rp1 = split(tbl_r, "ts", 150)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="nearest").sort(
            "ts"
        )
        assert result.to_pydict() == {"entity": ["A", "A"], "ts": [100, 200], "v": [1, 2], "w": [1500, 1500]}

    def test_empty_right_group_gives_null(self):
        # Partition by composite key (entity, ts): entity A in p0 (no right data), entity B in p1.
        lp0 = pa.table({"entity": ["A", "A"], "ts": [5, 10], "v": [1, 2]})
        lp1 = pa.table({"entity": ["B", "B"], "ts": [5, 10], "v": [3, 4]})
        rp0 = pa.table(
            {
                "entity": pa.array([], type=pa.string()),
                "ts": pa.array([], type=pa.int64()),
                "w": pa.array([], type=pa.int64()),
            }
        )
        rp1 = pa.table({"entity": ["B", "B"], "ts": [4, 9], "w": [40, 90]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="nearest").sort(
            ["entity", "ts"]
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B"],
            "ts": [5, 10, 5, 10],
            "v": [1, 2, 3, 4],
            "w": [None, None, 40, 90],
        }

    def test_group_boundary_not_crossed(self):
        tbl_l = pa.table(
            {"entity": ["A", "A", "A", "B", "B", "B"], "ts": [3, 7, 10, 15, 20, 25], "v": [1, 2, 3, 4, 5, 6]}
        )
        tbl_r = pa.table({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]})
        lp0, lp1 = split(tbl_l, "ts", 12)
        rp0, rp1 = split(tbl_r, "ts", 12)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="nearest").sort(
            ["entity", "ts"]
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "A", "B", "B", "B"],
            "ts": [3, 7, 10, 15, 20, 25],
            "v": [1, 2, 3, 4, 5, 6],
            "w": [20, 80, 80, None, None, None],
        }

    def test_nearest_with_multiple_by_columns(self):
        lp0 = pa.table({"entity": ["A"], "region": ["US"], "ts": [10], "v": [1]})
        lp1 = pa.table({"entity": ["B"], "region": ["EU"], "ts": [10], "v": [2]})
        rp0 = pa.table({"entity": ["A", "A"], "region": ["US", "US"], "ts": [7, 14], "w": [70, 140]})
        rp1 = pa.table({"entity": ["B", "B"], "region": ["EU", "EU"], "ts": [6, 13], "w": [60, 130]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(
            right, _assume_sorted_and_aligned=True, on="ts", by=["entity", "region"], strategy="nearest"
        ).sort("entity")
        assert result.to_pydict() == {
            "entity": ["A", "B"],
            "region": ["US", "EU"],
            "ts": [10, 10],
            "v": [1, 2],
            "w": [70, 130],
        }


class TestAlignedAsofJoinNearestEmptyTables:
    def test_empty_left_table(self):
        left = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            name="left",
        )
        right = aligned(
            pa.table({"ts": [1, 2], "w": [10, 20]}),
            pa.table({"ts": [3], "w": [30]}),
            name="right",
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}

    def test_empty_right_table(self):
        left = aligned(
            pa.table({"ts": [1, 2], "v": [10, 20]}),
            pa.table({"ts": [3], "v": [30]}),
            name="left",
        )
        right = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            name="right",
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [10, 20, 30], "w": [None, None, None]}

    def test_empty_left_and_right_tables(self):
        left = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "v": pa.array([], type=pa.int64())}),
            name="left",
        )
        right = aligned(
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            pa.table({"ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}),
            name="right",
        )
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest")
        assert result.column_names == ["ts", "v", "w"]
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}


class TestAlignedAsofJoinNearestDistributed:
    def test_multi_group_correctness(self):
        # Partitioned by entity (A+B in partition 0, C+D in partition 1), sorted by ts within each.
        lp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [10, 20, 10, 20], "v": [1, 5, 2, 6]})
        lp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [10, 20, 10, 20], "v": [3, 7, 4, 8]})
        rp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [5, 18, 8, 22], "w": [100, 500, 200, 600]})
        rp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [12, 25, 15, 28], "w": [300, 700, 400, 800]})
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="nearest").sort(
            ["entity", "ts"]
        )
        assert result.to_pydict() == {
            "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "ts": [10, 20, 10, 20, 10, 20, 10, 20],
            "v": [1, 5, 2, 6, 3, 7, 4, 8],
            "w": [100, 500, 200, 600, 300, 700, 400, 400],
        }

    def test_forward_carryover_beats_local_backward_match(self):
        tbl_l = pa.table({"ts": [2, 5], "v": [1, 2]})
        tbl_r = pa.table({"ts": [3, 6], "w": [30, 60]})
        lp0, lp1 = split(tbl_l, "ts", 4)
        rp0, rp1 = split(tbl_r, "ts", 4)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [2, 5]
        assert pydict["w"][0] == 30  # ts=2: nearest 3(|1|) → 30
        assert pydict["w"][1] == 60  # ts=5: backward carryover 3(|2|) vs forward 6(|1|) → forward wins

    def test_backward_match_beats_far_forward_carryover(self):
        tbl_l = pa.table({"ts": [20, 50], "v": [1, 2]})
        tbl_r = pa.table({"ts": [48, 200], "w": [480, 2000]})
        lp0, lp1 = split(tbl_l, "ts", 25)
        rp0, rp1 = split(tbl_r, "ts", 25)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [20, 50]
        assert pydict["w"][0] == 480  # ts=20: 48(|28|) vs 200(|180|) → 48
        assert pydict["w"][1] == 480  # ts=50: backward carryover 48(|2|) vs forward 200(|150|) → backward wins

    def test_equidistant_across_partition_boundary(self):
        tbl_l = pa.table({"ts": [3, 10], "v": [1, 2]})
        tbl_r = pa.table({"ts": [2, 8, 12], "w": [20, 80, 120]})
        lp0, lp1 = split(tbl_l, "ts", 5)
        rp0, rp1 = split(tbl_r, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        pydict = result.to_pydict()
        assert pydict["ts"] == [3, 10]
        assert pydict["w"][0] == 20  # ts=3: backward 2(|1|) → 2
        assert pydict["w"][1] == 120  # ts=10: equidistant 8(|2|) vs 12(|2|), prefer forward → 12

    def test_nearest_requires_both_carryovers_simultaneously(self):
        tbl_l = pa.table({"ts": [10, 90], "v": [1, 2]})
        tbl_r = pa.table({"ts": [5, 50, 95], "w": [50, 500, 950]})
        lp0, lp1 = split(tbl_l, "ts", 50)
        rp0, rp1 = split(tbl_r, "ts", 50)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy="nearest").sort("ts")
        assert result.to_pydict() == {"ts": [10, 90], "v": [1, 2], "w": [50, 950]}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestAlignedAsofJoinValidation:
    @pytest.mark.skip(
        reason="range-shuffle in the copied node silently equalises partition counts; re-enable once zip_and_join lands"
    )
    def test_mismatched_partition_counts_raises(self):
        """Left and right with different partition counts raise at execution time."""
        tbl_l = pa.table({"ts": [1, 2, 3, 4], "v": [1, 2, 3, 4]})
        tbl_r = pa.table({"ts": [1, 2, 3, 4, 5, 6], "w": [10, 20, 30, 40, 50, 60]})
        # left: 2 partitions, right: 3 partitions
        lp0, lp1 = split(tbl_l, "ts", 3)
        rp0, rp1 = split(tbl_r, "ts", 3)
        rp1a, rp1b = split(rp1, "ts", 5)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1a, rp1b, name="right")
        with pytest.raises(DaftCoreException, match="partition count mismatch at execution time"):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").collect()

    def test_scan_task_split_and_merge_enabled_raises(self):
        """_assume_sorted_and_aligned=True is incompatible with enable_scan_task_split_and_merge=True."""
        tbl_l = pa.table({"ts": [1, 2], "v": [1, 2]})
        tbl_r = pa.table({"ts": [1, 2], "w": [10, 20]})
        lp0, lp1 = split(tbl_l, "ts", 2)
        rp0, rp1 = split(tbl_r, "ts", 2)
        left = aligned(lp0, lp1, name="left")
        right = aligned(rp0, rp1, name="right")
        with execution_config_ctx(enable_scan_task_split_and_merge=True):
            with pytest.raises(Exception, match="enable_scan_task_split_and_merge"):
                left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").collect()
