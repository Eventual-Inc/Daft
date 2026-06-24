"""End-to-end correctness tests for spill-to-disk in the Sort (external merge sort) and
grouped Aggregation (grace aggregation) blocking sinks.

Each test runs the same query twice — once with spilling forced on (threshold = 1 byte, so the
engine spills aggressively) and once with spilling disabled (threshold = 0) — and asserts the
results match. This exercises the spill/merge code paths against the in-memory baseline.
"""

from __future__ import annotations

import random

import pytest

import daft
import daft.runners
from daft import Window, col

pytestmark = pytest.mark.skipif(
    daft.runners.get_or_infer_runner_type() == "ray",
    reason="Spill-to-disk is a property of the native execution engine; force-spill comparison is run on the native runner.",
)


def _spill_off():
    return daft.context.execution_config_ctx(sort_spill_threshold_bytes=0, agg_spill_threshold_bytes=0)


def _spill_on(tmp_dir: str | None = None):
    kwargs = dict(
        sort_spill_threshold_bytes=1,
        agg_spill_threshold_bytes=1,
        window_spill_threshold_bytes=1,
    )
    if tmp_dir is not None:
        kwargs["flight_shuffle_dirs"] = [tmp_dir]
    return daft.context.execution_config_ctx(**kwargs)


def _window_spill_off():
    return daft.context.execution_config_ctx(window_spill_threshold_bytes=0)


def _window_spill_on():
    return daft.context.execution_config_ctx(window_spill_threshold_bytes=1)


def _rows(pydict: dict) -> list[tuple]:
    """Normalize a pydict into a sorted list of row tuples for order-insensitive comparison."""
    cols = list(pydict.keys())
    rows = list(zip(*[pydict[c] for c in cols])) if cols else []
    # Sort by a stringified key so mixed/None types compare deterministically.
    return sorted(rows, key=lambda r: tuple(str(x) for x in r))


def _make_data(n: int, num_groups: int, seed: int = 0) -> dict:
    rng = random.Random(seed)
    return {
        "k": [rng.randint(0, num_groups - 1) for _ in range(n)],
        "k2": [rng.choice(["x", "y", "z", None]) for _ in range(n)],
        "v": [rng.randint(-1000, 1000) for _ in range(n)],
        "f": [rng.random() for _ in range(n)],
    }


# ──────────────────────────── Sort ────────────────────────────


@pytest.mark.parametrize(
    "sort_cols,desc,nulls_first",
    [
        (["v"], [False], None),
        (["v"], [True], None),
        (["k", "v"], [False, True], None),
        (["k2"], [False], [True]),
        (["k2"], [False], [False]),
        (["k2", "v"], [True, False], [True, False]),
    ],
)
def test_sort_spill_matches_in_memory(sort_cols, desc, nulls_first):
    data = _make_data(20000, num_groups=500, seed=1)

    def run():
        df = daft.from_pydict(data)
        if nulls_first is None:
            df = df.sort(sort_cols, desc=desc)
        else:
            df = df.sort(sort_cols, desc=desc, nulls_first=nulls_first)
        return df.to_pydict()

    with _spill_off():
        expected = run()
    with _spill_on():
        actual = run()

    # Sort imposes a total key order; compare full output as ordered rows on the sort keys, but
    # since ties among non-key columns are unspecified, compare as multisets of rows.
    assert _rows(expected) == _rows(actual)


def test_sort_many_runs_multi_batch():
    # Larger input to force many spilled runs and a real multi-way merge.
    data = _make_data(200000, num_groups=50000, seed=2)
    with _spill_off():
        expected = daft.from_pydict(data).sort(["k", "v"]).to_pydict()
    with _spill_on():
        actual = daft.from_pydict(data).sort(["k", "v"]).to_pydict()
    assert _rows(expected) == _rows(actual)


def test_sort_empty_input_spill():
    df = daft.from_pydict({"k": [1], "v": [2]}).where(col("k") != 1)
    with _spill_on():
        out = df.sort("v").to_pydict()
    assert out == {"k": [], "v": []}


# ──────────────────────── Grouped aggregation ────────────────────────


def test_agg_decomposable_spill_matches():
    data = _make_data(50000, num_groups=2000, seed=3)

    def run():
        return (
            daft.from_pydict(data)
            .groupby("k")
            .agg(
                col("v").sum().alias("sum"),
                col("v").mean().alias("mean"),
                col("v").min().alias("min"),
                col("v").max().alias("max"),
                col("v").count().alias("count"),
            )
            .to_pydict()
        )

    with _spill_off():
        expected = run()
    with _spill_on():
        actual = run()
    assert _rows(expected) == _rows(actual)


def test_agg_high_cardinality_spill_matches():
    # num_groups ≈ n forces the PartitionThenAgg (high-cardinality) strategy.
    data = _make_data(40000, num_groups=40000, seed=4)
    with _spill_off():
        expected = daft.from_pydict(data).groupby("k").agg(col("v").sum().alias("s")).to_pydict()
    with _spill_on():
        actual = daft.from_pydict(data).groupby("k").agg(col("v").sum().alias("s")).to_pydict()
    assert _rows(expected) == _rows(actual)


def test_agg_multi_key_spill_matches():
    data = _make_data(30000, num_groups=300, seed=5)

    def run():
        return (
            daft.from_pydict(data).groupby("k", "k2").agg(col("v").sum().alias("s"), col("f").mean().alias("mf")).to_pydict()
        )

    with _spill_off():
        expected = run()
    with _spill_on():
        actual = run()
    assert _rows(expected) == _rows(actual)


def test_agg_non_decomposable_list_spill_matches():
    # list_agg / list_agg_distinct cannot be decomposed; the sink spills raw partitioned rows.
    data = _make_data(20000, num_groups=400, seed=6)

    def run():
        out = (
            daft.from_pydict(data)
            .groupby("k")
            .agg(col("v").list_agg().alias("vs"), col("v").sum().alias("s"))
            .to_pydict()
        )
        # Lists are unordered across spill boundaries; compare as sorted multisets per group.
        return {"k": out["k"], "vs": [sorted(x) for x in out["vs"]], "s": out["s"]}

    with _spill_off():
        expected = run()
    with _spill_on():
        actual = run()
    assert _rows(expected) == _rows(actual)


def test_agg_empty_input_spill():
    df = daft.from_pydict({"k": [1], "v": [2]}).where(col("k") != 1)
    with _spill_on():
        out = df.groupby("k").agg(col("v").sum().alias("s")).to_pydict()
    assert out == {"k": [], "s": []}


def test_spill_files_cleaned_up(tmp_path):
    data = _make_data(30000, num_groups=1000, seed=7)
    with _spill_on(str(tmp_path)):
        _ = daft.from_pydict(data).sort("v").to_pydict()
        _ = daft.from_pydict(data).groupby("k").agg(col("v").sum()).to_pydict()
    leftover = list(tmp_path.rglob("daft_*"))
    assert leftover == [], f"spill files were not cleaned up: {leftover}"


# ─────────────────────── Partitioned window functions ───────────────────────


def test_window_partition_only_spill_matches():
    data = _make_data(40000, num_groups=800, seed=10)
    wspec = Window().partition_by("k")

    def run():
        return daft.from_pydict(data).with_column("ws", col("v").sum().over(wspec)).to_pydict()

    with _window_spill_off():
        expected = run()
    with _window_spill_on():
        actual = run()
    assert _rows(expected) == _rows(actual)


def test_window_partition_order_by_agg_spill_matches():
    data = _make_data(40000, num_groups=800, seed=11)
    wspec = Window().partition_by("k").order_by("v")

    def run():
        return daft.from_pydict(data).with_column("running", col("v").sum().over(wspec)).to_pydict()

    with _window_spill_off():
        expected = run()
    with _window_spill_on():
        actual = run()
    assert _rows(expected) == _rows(actual)


def test_window_partition_order_by_rank_spill_matches():
    from daft.functions import rank, row_number

    data = _make_data(40000, num_groups=800, seed=12)
    wspec = Window().partition_by("k").order_by("v", desc=True)

    def run():
        return (
            daft.from_pydict(data)
            .with_column("rn", row_number().over(wspec))
            .with_column("rk", rank().over(wspec))
            .to_pydict()
        )

    with _window_spill_off():
        expected = run()
    with _window_spill_on():
        actual = run()
    assert _rows(expected) == _rows(actual)


def test_window_multi_partition_key_spill_matches():
    data = _make_data(30000, num_groups=300, seed=13)
    wspec = Window().partition_by("k", "k2").order_by("v")

    def run():
        return daft.from_pydict(data).with_column("ws", col("v").mean().over(wspec)).to_pydict()

    with _window_spill_off():
        expected = run()
    with _window_spill_on():
        actual = run()
    assert _rows(expected) == _rows(actual)


def test_window_spill_files_cleaned_up(tmp_path):
    data = _make_data(40000, num_groups=1000, seed=14)
    wspec = Window().partition_by("k").order_by("v")
    with daft.context.execution_config_ctx(window_spill_threshold_bytes=1, flight_shuffle_dirs=[str(tmp_path)]):
        _ = daft.from_pydict(data).with_column("ws", col("v").sum().over(wspec)).to_pydict()
    leftover = list(tmp_path.rglob("daft_window_spill_*"))
    assert leftover == [], f"window spill files were not cleaned up: {leftover}"


# ─────────────────── Shared spill pool / new operator coverage ───────────────


def test_hash_join_spills_by_default():
    # No explicit hash_join_spill_threshold_bytes — must spill via the shared pool.
    # 1 MiB pool is far too small for 20 000-row tables, so spilling is forced.
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):  # 1 MiB pool
        left = daft.from_pydict({"k": list(range(20000)), "v": list(range(20000))})
        right = daft.from_pydict({"k": list(range(20000)), "w": list(range(20000))})
        out = left.join(right, on="k").sort("k").to_pydict()
        assert out["k"] == list(range(20000))


def test_dedup_spills_and_matches_in_memory():
    data = {"a": [i % 5000 for i in range(50000)]}
    expected = sorted(set(data["a"]))
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        got = daft.from_pydict(data).distinct().sort("a").to_pydict()["a"]
    assert got == expected


def test_large_pipeline_completes_under_tiny_pool():
    # NOTE: repartition's Flight-shuffle spill (streaming finalize, Task 8/9) is a
    # distributed/Flotilla-runner feature and is a no-op on the native runner, so it
    # cannot be exercised here. That path is covered by Rust unit tests:
    #   - src/daft-shuffles/src/oneshot_writer.rs (streaming writer row counts)
    #   - src/daft-local-execution/src/sinks/repartition.rs (spill/flatten round-trip)
    # This test verifies a large pipeline completes and stays correct under a tiny pool.
    n = 200000
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        df = daft.from_pydict({"k": list(range(n)), "v": list(range(n))})
        out = df.sort("k").to_pydict()
    assert out["k"] == list(range(n))


def test_two_spilling_operators_share_one_pool():
    # An aggregation feeding a sort, both under one small pool — must stay correct.
    n = 100000
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        df = daft.from_pydict({"g": [i % 1000 for i in range(n)], "v": list(range(n))})
        out = df.groupby("g").sum("v").sort("g").to_pydict()
    assert out["g"] == list(range(1000))
    # sum of v for group g = sum of i where i % 1000 == g
    expected = [sum(i for i in range(n) if i % 1000 == g) for g in range(1000)]
    assert out["v"] == expected


@pytest.mark.parametrize("op", ["sort", "agg", "dedup"])
def test_spill_matches_no_spill(op):
    n = 30000
    base = daft.from_pydict({"g": [i % 777 for i in range(n)], "v": list(range(n))})

    def run(df):
        if op == "sort":
            return df.sort("v").to_pydict()
        if op == "agg":
            return df.groupby("g").sum("v").sort("g").to_pydict()
        return df.select("g").distinct().sort("g").to_pydict()

    with daft.context.execution_config_ctx(
        sort_spill_threshold_bytes=0,
        agg_spill_threshold_bytes=0,
    ):
        no_spill = run(base)
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        spilled = run(base)
    assert no_spill == spilled


def test_outer_join_spills_correct():
    # LEFT OUTER join under a tiny pool (exercises the bitmap-tracked spill path).
    # Expected result is computed with spilling disabled for comparison.
    n = 10000
    left = daft.from_pydict({"k": list(range(n)), "lv": list(range(n))})
    # right has keys 0..n/2 — so the upper half of left produces NULL right-side rows.
    right = daft.from_pydict({"k": list(range(n // 2)), "rv": list(range(n // 2))})

    def run_join(disable_hash_join_spill: bool):
        ctx_kwargs = {}
        if disable_hash_join_spill:
            ctx_kwargs["hash_join_spill_threshold_bytes"] = 0
        else:
            ctx_kwargs["spill_pool_bytes"] = 1 << 20  # 1 MiB — forces spilling
        with daft.context.execution_config_ctx(**ctx_kwargs):
            return left.join(right, on="k", how="left").sort("k").to_pydict()

    no_spill = run_join(disable_hash_join_spill=True)
    spilled = run_join(disable_hash_join_spill=False)
    assert _rows(no_spill) == _rows(spilled)


def test_join_multi_partition_spill():
    # Enough distinct keys and a tiny pool so multiple build partitions spill.
    # Result must equal the no-spill result.
    n = 20000
    left = daft.from_pydict({"k": list(range(n)), "lv": list(range(n))})
    right = daft.from_pydict({"k": list(range(n)), "rv": list(range(n))})

    with daft.context.execution_config_ctx(hash_join_spill_threshold_bytes=0):
        no_spill = left.join(right, on="k").sort("k").to_pydict()

    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        spilled = left.join(right, on="k").sort("k").to_pydict()

    assert _rows(no_spill) == _rows(spilled)
