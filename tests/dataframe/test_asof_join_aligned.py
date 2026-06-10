"""Correctness tests for asof join with _assume_sorted_and_aligned=True.

These tests pin down the behavioral contract of the aligned asof join so that
any execution strategy (shuffle-based or shuffle-skipping) must satisfy them.
They deliberately include >=3-partition layouts with empty / group-sparse
middle partitions: with only 2 partitions, "look at the adjacent partition"
and "propagate a carryover across all preceding partitions" are
indistinguishable, so 2-partition tests cannot catch a broken carryover.

Conventions for the scenario table:
- left tables are {"ts": ..., "v": 0..n} (v makes duplicate-ts rows distinguishable)
- right tables are {"ts": ..., "w": ts * 10} so expected matches are readable
- `boundaries` value-splits both sides into the same aligned partitions

Only runs under the Ray runner because _assume_sorted_and_aligned is a
distributed-planner feature.
"""

from __future__ import annotations

import random
from collections.abc import AsyncIterator

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import daft
from daft.context import execution_config_ctx
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="_assume_sorted_and_aligned is a distributed-planner feature (ray runner only)",
)

STRATEGIES = ["backward", "forward", "nearest"]


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
    """Emits pre-split partitions as individual scan tasks."""

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
    """Wrap explicit partition tables in an AlignedSource DataFrame."""
    return AlignedSource(name, list(partitions)).read()


def value_aligned(table: pa.Table, key: str, boundaries: list, name: str = "source") -> daft.DataFrame:
    """Value-split `table` on `key` at `boundaries` into len(boundaries)+1 aligned partitions."""
    parts = []
    for b in boundaries:
        mask = pc.less(table[key], b)
        parts.append(table.filter(mask))
        table = table.filter(pc.invert(mask))
    parts.append(table)
    return aligned(*parts, name=name)


# ---------------------------------------------------------------------------
# Correctness without by-keys
#
# Each scenario gives left ts, right ts, partition boundaries, and the expected
# matched `w` (= matched right ts * 10) per strategy, in (ts, v) order.
# ---------------------------------------------------------------------------

NO_BY_CASES = [
    # (id, left_ts, right_ts, boundaries, {strategy: expected_w})
    (
        "exact_match",
        [10, 20, 30],
        [10, 20, 30],
        [15],
        {"backward": [100, 200, 300], "forward": [100, 200, 300], "nearest": [100, 200, 300]},
    ),
    (
        "inexact_match",
        [10, 20, 30],
        [4, 16, 27],
        [20],
        {"backward": [40, 160, 270], "forward": [160, 270, None], "nearest": [160, 160, 270]},
    ),
    (
        "unmatched_edges",
        [2, 6, 12],
        [4, 9],
        [8],
        {"backward": [None, 40, 90], "forward": [40, 90, None], "nearest": [40, 40, 90]},
    ),
    (
        "all_left_before_all_right",
        [1, 2, 3],
        [100, 200],
        [2],
        {"backward": [None, None, None], "forward": [1000, 1000, 1000], "nearest": [1000, 1000, 1000]},
    ),
    (
        "all_right_before_all_left",
        [100, 200, 300],
        [10, 50],
        [150],
        {"backward": [500, 500, 500], "forward": [None, None, None], "nearest": [500, 500, 500]},
    ),
    (
        "duplicate_left_ts",
        [6, 6, 11],
        [4, 8],
        [8],
        {"backward": [40, 40, 80], "forward": [80, 80, None], "nearest": [80, 80, 80]},
    ),
    (
        "duplicate_right_ts",
        [3, 7],
        [4, 7, 7],
        [5],
        {"backward": [None, 70], "forward": [40, 70], "nearest": [40, 70]},
    ),
    (
        "direction_mix",
        [5, 10, 15, 20, 25],
        [3, 8, 18, 30],
        [15],
        {
            "backward": [30, 80, 80, 180, 180],
            "forward": [80, 180, 180, 300, 300],
            "nearest": [30, 80, 180, 180, 300],
        },
    ),
    (
        "no_carryover_into_last_partition",
        [10, 20, 100],
        [15, 25],
        [50],
        {"backward": [None, 150, 250], "forward": [150, 250, None], "nearest": [150, 250, 250]},
    ),
    # Carryovers must cross *empty* right partitions: the only right matches for
    # the last left partition live 2+ partitions away.
    (
        "empty_middle_right_partition",
        [1, 15, 25],
        [5, 30],
        [10, 20],
        {"backward": [None, 50, 50], "forward": [50, 300, 300], "nearest": [50, 50, 300]},
    ),
    (
        "three_consecutive_empty_right_partitions",
        [1, 15, 35, 48],
        [5, 45],
        [10, 20, 30, 40],
        {
            "backward": [None, 50, 50, 450],
            "forward": [50, 450, 450, None],
            "nearest": [50, 50, 450, 450],
        },
    ),
    # Nearest-specific tie-breaking and direction selection.
    ("nearest_picks_backward_when_closer", [3, 10], [2, 7, 14], [5], {"nearest": [20, 70]}),
    ("nearest_picks_forward_when_closer", [3, 10], [2, 6, 13], [5], {"nearest": [20, 130]}),
    ("nearest_direction_switches", [3, 8, 13], [5, 11], [8], {"nearest": [50, 110, 110]}),
    ("nearest_single_right_carryover", [1, 50, 99], [25, 50], [50], {"nearest": [250, 500, 500]}),
    ("nearest_equidistant_prefers_forward", [3, 10], [2, 8, 12], [5], {"nearest": [20, 120]}),
    ("nearest_exact_beats_equidistant", [3, 10], [2, 8, 10, 12], [5], {"nearest": [20, 100]}),
    ("nearest_one_right_matches_many", [10, 15], [7, 13], [12], {"nearest": [130, 130]}),
]


@pytest.mark.parametrize(
    "left_ts,right_ts,boundaries,strategy,expected_w",
    [
        pytest.param(left, right, bounds, strategy, expected, id=f"{name}-{strategy}")
        for name, left, right, bounds, per_strategy in NO_BY_CASES
        for strategy, expected in per_strategy.items()
    ],
)
def test_correctness_no_by(left_ts, right_ts, boundaries, strategy, expected_w):
    left = value_aligned(pa.table({"ts": left_ts, "v": list(range(len(left_ts)))}), "ts", boundaries, name="left")
    right = value_aligned(pa.table({"ts": right_ts, "w": [t * 10 for t in right_ts]}), "ts", boundaries, name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy=strategy).sort(["ts", "v"])
    pydict = result.to_pydict()
    assert pydict["ts"] == sorted(left_ts)
    assert pydict["w"] == expected_w


# ---------------------------------------------------------------------------
# Correctness with by-keys
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "strategy,expected_w",
    [
        ("backward", [100, 500, 200, 200, None, 300, None, 400]),
        ("forward", [500, None, 600, 600, 300, 700, 400, 800]),
        ("nearest", [100, 500, 200, 600, 300, 700, 400, 400]),
    ],
)
def test_multi_group(strategy, expected_w):
    """Partitioned by entity (A+B in partition 0, C+D in partition 1), sorted by ts within each."""
    lp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [10, 20, 10, 20], "v": [1, 5, 2, 6]})
    lp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [10, 20, 10, 20], "v": [3, 7, 4, 8]})
    rp0 = pa.table({"entity": ["A", "A", "B", "B"], "ts": [5, 18, 8, 22], "w": [100, 500, 200, 600]})
    rp1 = pa.table({"entity": ["C", "C", "D", "D"], "ts": [12, 25, 15, 28], "w": [300, 700, 400, 800]})
    left = aligned(lp0, lp1, name="left")
    right = aligned(rp0, rp1, name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy=strategy).sort(
        ["entity", "ts"]
    )
    assert result.column_names == ["entity", "ts", "v", "w"]
    assert result.to_pydict() == {
        "entity": ["A", "A", "B", "B", "C", "C", "D", "D"],
        "ts": [10, 20, 10, 20, 10, 20, 10, 20],
        "v": [1, 5, 2, 6, 3, 7, 4, 8],
        "w": expected_w,
    }


@pytest.mark.parametrize(
    "strategy,expected_w",
    [
        ("backward", [20, 20, 80, None, None, None]),
        ("forward", [80, 80, None, None, None, None]),
        ("nearest", [20, 80, 80, None, None, None]),
    ],
)
def test_matches_never_cross_group_boundaries(strategy, expected_w):
    """Entity B has no right rows at all; carryover from entity A must not leak into it."""
    tbl_l = pa.table({"entity": ["A", "A", "A", "B", "B", "B"], "ts": [3, 7, 10, 15, 20, 25], "v": [0, 1, 2, 3, 4, 5]})
    tbl_r = pa.table({"entity": ["A", "A"], "ts": [2, 8], "w": [20, 80]})
    left = value_aligned(tbl_l, "ts", [12], name="left")
    right = value_aligned(tbl_r, "ts", [12], name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy=strategy).sort(
        ["entity", "ts"]
    )
    assert result.to_pydict()["w"] == expected_w


@pytest.mark.parametrize(
    "strategy,expected_w",
    [
        ("backward", [None, 200, 400, None, 300, 500]),
        ("forward", [200, 400, None, 300, 500, None]),
    ],
)
def test_interleaved_timestamps_across_entities(strategy, expected_w):
    """All A rows in partition 0, all B rows in partition 1, with overlapping ts ranges."""
    lp0 = pa.table({"entity": ["A", "A", "A"], "ts": [1, 3, 5], "v": [10, 30, 50]})
    lp1 = pa.table({"entity": ["B", "B", "B"], "ts": [2, 4, 6], "v": [20, 40, 60]})
    rp0 = pa.table({"entity": ["A", "A"], "ts": [2, 4], "w": [200, 400]})
    rp1 = pa.table({"entity": ["B", "B"], "ts": [3, 5], "w": [300, 500]})
    left = aligned(lp0, lp1, name="left")
    right = aligned(rp0, rp1, name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy=strategy).sort(
        ["entity", "ts"]
    )
    assert result.to_pydict()["w"] == expected_w


@pytest.mark.parametrize(
    "strategy,expected_w",
    [("backward", [50, None]), ("forward", [None, 150]), ("nearest", [50, 150])],
)
def test_group_skips_partition(strategy, expected_w):
    """Entity A's only right match is 2 partitions away from its left row.

    The partition in between is non-empty but contains only entity B, so
    per-group carryovers must skip it.
    """
    tbl_l = pa.table({"entity": ["B", "A"], "ts": [12, 25], "v": [0, 1]})
    tbl_r = pa.table({"entity": ["A", "B"], "ts": [5, 15], "w": [50, 150]})
    left = value_aligned(tbl_l, "ts", [10, 20], name="left")
    right = value_aligned(tbl_r, "ts", [10, 20], name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy=strategy).sort(
        ["entity", "ts"]
    )
    assert result.to_pydict()["w"] == expected_w


@pytest.mark.parametrize(
    "right_ts,expected_w",
    [([5, 100], [50, 50, 50]), ([-100, -5], [-50, -50, -50])],
    ids=["all_right_after_group", "all_right_before_group"],
)
def test_nearest_group_matches_all_cross_boundary(right_ts, expected_w):
    tbl_l = pa.table({"entity": ["A", "A", "A"], "ts": [1, 2, 3], "v": [0, 1, 2]})
    tbl_r = pa.table({"entity": ["A", "A"], "ts": right_ts, "w": [t * 10 for t in right_ts]})
    left = value_aligned(tbl_l, "ts", [2], name="left")
    right = value_aligned(tbl_r, "ts", [2], name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="entity", strategy="nearest").sort("ts")
    assert result.to_pydict()["w"] == expected_w


def test_nearest_with_multiple_by_columns():
    """Per-group nearest picks different directions; by-key is composite."""
    lp0 = pa.table({"entity": ["A"], "region": ["US"], "ts": [10], "v": [1]})
    lp1 = pa.table({"entity": ["B"], "region": ["EU"], "ts": [10], "v": [2]})
    rp0 = pa.table({"entity": ["A", "A"], "region": ["US", "US"], "ts": [7, 14], "w": [70, 140]})
    rp1 = pa.table({"entity": ["B", "B"], "region": ["EU", "EU"], "ts": [6, 13], "w": [60, 130]})
    left = aligned(lp0, lp1, name="left")
    right = aligned(rp0, rp1, name="right")
    result = left.join_asof(
        right, _assume_sorted_and_aligned=True, on="ts", by=["entity", "region"], strategy="nearest"
    ).sort("entity")
    assert result.to_pydict()["w"] == [70, 130]


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_null_in_asof_key_produces_null(strategy):
    lp0 = pa.table({"ts": pa.array([None], type=pa.int64()), "v": [1]})
    lp1 = pa.table({"ts": pa.array([2, None], type=pa.int64()), "v": [2, 3]})
    rp0 = pa.table({"ts": [1], "w": [10]})
    rp1 = pa.table({"ts": [2, 3], "w": [20, 30]})
    left = aligned(lp0, lp1, name="left")
    right = aligned(rp0, rp1, name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy=strategy).sort("v")
    assert result.to_pydict()["w"] == [None, 20, None]


@pytest.mark.parametrize(
    "strategy,expected_w",
    [
        ("backward", [20, None, 40, None]),
        ("forward", [40, None, None, None]),
        ("nearest", [40, None, 40, None]),
    ],
)
def test_null_in_by_key_no_cross_match(strategy, expected_w):
    """Null by-keys never match each other; expected_w is in `v` order."""
    lp0 = pa.table({"g": pa.array([None, None], type=pa.string()), "ts": [3, 5], "v": [2, 4]})
    lp1 = pa.table({"g": ["X", "X"], "ts": [3, 5], "v": [1, 3]})
    rp0 = pa.table(
        {"g": pa.array([], type=pa.string()), "ts": pa.array([], type=pa.int64()), "w": pa.array([], type=pa.int64())}
    )
    rp1 = pa.table({"g": ["X", "X"], "ts": [2, 4], "w": [20, 40]})
    left = aligned(lp0, lp1, name="left")
    right = aligned(rp0, rp1, name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by="g", strategy=strategy).sort("v")
    assert result.column_names == ["g", "ts", "v", "w"]
    assert result.to_pydict()["w"] == expected_w


# ---------------------------------------------------------------------------
# Empty tables
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize(
    "empty_left,empty_right", [(True, False), (False, True), (True, True)], ids=["left", "right", "both"]
)
def test_empty_tables(strategy, empty_left, empty_right):
    def table(payload: str, empty: bool) -> pa.Table:
        ts = [] if empty else [1, 2, 3]
        return pa.table({"ts": pa.array(ts, type=pa.int64()), payload: pa.array(ts, type=pa.int64())})

    left = value_aligned(table("v", empty_left), "ts", [2], name="left")
    right = value_aligned(table("w", empty_right), "ts", [2], name="right")
    result = left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", strategy=strategy).sort("ts")
    assert result.column_names == ["ts", "v", "w"]
    if empty_left:
        assert result.to_pydict() == {"ts": [], "v": [], "w": []}
    else:
        assert result.to_pydict() == {"ts": [1, 2, 3], "v": [1, 2, 3], "w": [None, None, None]}


# ---------------------------------------------------------------------------
# Differential test against a pure-Python reference implementation
#
# The reference is deliberately independent of any Daft execution path (the
# distributed shuffle-based asof join has its own carryover machinery and
# cannot serve as an oracle for itself). Right-side ts values are unique per
# group so every scenario has a deterministic match.
# ---------------------------------------------------------------------------


def reference_asof_match(left_ts: int, right_ts: list[int], strategy: str) -> int | None:
    backward = max((t for t in right_ts if t <= left_ts), default=None)
    forward = min((t for t in right_ts if t >= left_ts), default=None)
    if strategy == "backward":
        return backward
    if strategy == "forward":
        return forward
    if backward is None or forward is None:
        return backward if forward is None else forward
    return forward if forward - left_ts <= left_ts - backward else backward


@pytest.mark.parametrize(
    "strategy",
    [
        "backward",
        "forward",
        pytest.param(
            "nearest",
            marks=pytest.mark.xfail(
                reason="local nearest asof join: a left row that gets a one-sided direct offer "
                "(search_nearest floor/ceil) is skipped by nearest_fill and never sees its "
                "other-side candidate. Repro: left=[593, 597], right=[577, 608] -> 593 matches "
                "577 (dist 16) instead of 608 (dist 15).",
                strict=False,
            ),
        ),
    ],
)
@pytest.mark.parametrize("by", [None, "entity"], ids=["no_by", "by"])
@pytest.mark.parametrize("num_partitions", [1, 3, 5])
def test_matches_reference_implementation(strategy, by, num_partitions):
    rng = random.Random(f"{strategy}-{by}-{num_partitions}")
    hi = 1000
    boundaries = [hi * i // num_partitions for i in range(1, num_partitions)]
    groups = ["A", "B", "C"] if by else [None]

    left_rows, right_rows = [], []
    for group in groups:
        left_ts = rng.choices(range(hi), k=40)
        right_ts = rng.sample(range(hi), k=20)
        # Make the right side sparse: drop one middle partition's range entirely so
        # matches must carry across it.
        if num_partitions >= 3:
            dead = rng.randrange(1, num_partitions - 1)
            right_ts = [t for t in right_ts if not (boundaries[dead - 1] <= t < boundaries[dead])]
        left_rows += [(group, t) for t in left_ts]
        right_rows += [(group, t) for t in right_ts]

    def build(rows: list, payload: str) -> pa.Table:
        rows = sorted(rows, key=lambda r: r[1])
        columns = {"ts": [t for _, t in rows], payload: list(range(len(rows)))}
        if by:
            columns = {"entity": [g for g, _ in rows], **columns}
        return pa.table(columns)

    left_tbl, right_tbl = build(left_rows, "v"), build(right_rows, "w")
    left = value_aligned(left_tbl, "ts", boundaries, name="left")
    right = value_aligned(right_tbl, "ts", boundaries, name="right")

    sort_cols = (["entity"] if by else []) + ["ts", "v"]
    actual = (
        left.join_asof(right, _assume_sorted_and_aligned=True, on="ts", by=by, strategy=strategy)
        .sort(sort_cols)
        .to_pydict()
    )

    right_groups = right_tbl["entity"].to_pylist() if by else [None] * right_tbl.num_rows
    w_by_match = dict(zip(zip(right_groups, right_tbl["ts"].to_pylist()), right_tbl["w"].to_pylist()))
    right_ts_by_group = {}
    for g, t in right_rows:
        right_ts_by_group.setdefault(g, []).append(t)

    left_groups = left_tbl["entity"].to_pylist() if by else [None] * left_tbl.num_rows
    expected_rows = sorted(
        zip(left_groups, left_tbl["ts"].to_pylist(), left_tbl["v"].to_pylist()),
        key=lambda r: (r[0] or "", r[1], r[2]),
    )
    matches = [reference_asof_match(t, right_ts_by_group.get(g, []), strategy) for g, t, _ in expected_rows]
    expected = {
        **({"entity": [g for g, _, _ in expected_rows]} if by else {}),
        "ts": [t for _, t, _ in expected_rows],
        "v": [v for _, _, v in expected_rows],
        "w": [None if m is None else w_by_match[(g, m)] for m, (g, _, _) in zip(matches, expected_rows)],
    }
    assert actual == expected


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestAlignedAsofJoinValidation:
    @pytest.mark.skip(
        reason="the shuffle-based fallback silently equalises partition counts; enable once the aligned execution path lands"
    )
    def test_mismatched_partition_counts_raises(self):
        """Left and right with different partition counts raise at execution time."""
        left = value_aligned(pa.table({"ts": [1, 2, 3, 4], "v": [1, 2, 3, 4]}), "ts", [3], name="left")
        right = value_aligned(
            pa.table({"ts": [1, 2, 3, 4, 5, 6], "w": [10, 20, 30, 40, 50, 60]}), "ts", [3, 5], name="right"
        )
        with pytest.raises(Exception, match="partition count mismatch at execution time"):
            left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").collect()

    def test_scan_task_split_and_merge_enabled_raises(self):
        """_assume_sorted_and_aligned=True is incompatible with enable_scan_task_split_and_merge=True."""
        left = value_aligned(pa.table({"ts": [1, 2], "v": [1, 2]}), "ts", [2], name="left")
        right = value_aligned(pa.table({"ts": [1, 2], "w": [10, 20]}), "ts", [2], name="right")
        with execution_config_ctx(enable_scan_task_split_and_merge=True):
            with pytest.raises(Exception, match="enable_scan_task_split_and_merge"):
                left.join_asof(right, _assume_sorted_and_aligned=True, on="ts").collect()
