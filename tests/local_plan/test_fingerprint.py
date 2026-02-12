"""Tests for LocalPhysicalPlan fingerprinting and plan reuse in NativeExecutor."""

from __future__ import annotations

import pytest

import daft
from daft import col
from daft.daft import LocalPhysicalPlan
from daft.daft import NativeExecutor as _NativeExecutor
from daft.event_loop import get_or_init_event_loop


def build_plan(df):
    """Translate a DataFrame to (LocalPhysicalPlan, inputs dict)."""
    ctx = daft.context.get_context()
    builder = df._builder.optimize(ctx.daft_execution_config)
    return LocalPhysicalPlan.from_logical_plan_builder(builder._builder, {})


def _assert_plan_count(dfs, expected_count, query_ids=None):
    """Launch each df as a separate input_id, assert expected active plan count.

    If query_ids is given, each df gets the corresponding query_id;
    otherwise all share "test".

    All handles are drained concurrently (via asyncio.gather) to avoid
    backpressure deadlocks: when multiple input_ids share a pipeline,
    sequential draining can block the message router if an un-drained
    input_id's result channel fills up.
    """
    loop = get_or_init_event_loop()
    executor = _NativeExecutor()
    ctx = daft.context.get_context()._ctx

    async def run(plan, inputs, i, qid):
        return await executor.run(plan, ctx, dict(inputs), i, {"query_id": qid})

    async def drain(h):
        while await h.__anext__() is not None:
            pass
        await h.try_finish()

    async def drain_all(handles):
        import asyncio

        await asyncio.gather(*(drain(h) for h in handles))

    handles = []
    for i, df in enumerate(dfs):
        qid = query_ids[i] if query_ids else "test"
        plan, inputs = build_plan(df)
        handles.append(loop.run(run(plan, inputs, i, qid)))

    assert executor.active_plan_count() == expected_count

    loop.run(drain_all(handles))

    assert executor.active_plan_count() == 0


@daft.udf(return_dtype=daft.DataType.int64())
def _my_udf(x):
    return x


SAME_PLAN_CASES = [
    pytest.param(lambda: daft.range(0, 100), id="range"),
    pytest.param(lambda: daft.range(0, 100).limit(10), id="limit"),
    pytest.param(lambda: daft.range(0, 100).where(col("id") > 50), id="filter"),
    pytest.param(lambda: daft.range(0, 100).with_column("x", col("id") + 1), id="project"),
    pytest.param(lambda: daft.range(0, 100).agg(col("id").sum()), id="agg"),
    pytest.param(lambda: daft.range(0, 100).sort("id"), id="sort"),
    pytest.param(lambda: daft.range(0, 100).join(daft.range(0, 100), on="id"), id="join"),
    pytest.param(lambda: daft.range(0, 100).with_column("y", _my_udf(col("id"))), id="udf"),
]

DIFFERENT_PLAN_CASES = [
    pytest.param(
        lambda: daft.range(0, 100).limit(10),
        lambda: daft.range(0, 100).limit(20),
        id="limit",
    ),
    pytest.param(
        lambda: daft.range(0, 100).where(col("id") > 50),
        lambda: daft.range(0, 100).where(col("id") > 25),
        id="filter",
    ),
    pytest.param(
        lambda: daft.range(0, 100).with_column("x", col("id") + 1),
        lambda: daft.range(0, 100).with_column("x", col("id") + 2),
        id="project",
    ),
    pytest.param(
        lambda: daft.range(0, 100).agg(col("id").sum()),
        lambda: daft.range(0, 100).agg(col("id").mean()),
        id="agg",
    ),
]


@pytest.mark.parametrize("make_df", SAME_PLAN_CASES)
def test_same_plans_reused(make_df, with_morsel_size):
    _assert_plan_count([make_df() for _ in range(2)], 1)


@pytest.mark.parametrize("make_df_a, make_df_b", DIFFERENT_PLAN_CASES)
def test_different_plans_not_reused(make_df_a, make_df_b, with_morsel_size):
    _assert_plan_count([make_df_a(), make_df_b()], 2)


def test_different_query_ids_not_reused(with_morsel_size):
    """Same plan with different query_ids should not share a pipeline."""
    df = daft.range(0, 100)
    _assert_plan_count([df, df], 2, query_ids=["q1", "q2"])
