"""Unit tests for the distributed-Limit state machine.

Exercises `_LimitCounterImpl` directly (no Ray cluster). The interesting
invariants here are the retry-rewind path in `start_task` — failed
SwordfishTask attempts must release their claimed budget back so the retry
emits the right total. Dataframe-level limit tests don't fail tasks, so this
path is otherwise uncovered.
"""

from __future__ import annotations

import pytest

ray = pytest.importorskip("ray")

from daft.execution.ray_distributed_limit import _LimitCounterImpl
from tests.conftest import get_tests_daft_runner_name


def test_claim_basic():
    actor = _LimitCounterImpl(limit=10, offset=0)
    actor.start_task("t1")
    assert actor.claim("t1", 100) == (0, 10, True)
    assert actor.claim("t1", 100) == (0, 0, True)


def test_claim_with_offset():
    actor = _LimitCounterImpl(limit=10, offset=5)
    actor.start_task("t1")
    # 5 rows go to skip, 10 to take, 5 discarded.
    assert actor.claim("t1", 20) == (5, 10, True)


def test_claim_offset_spans_multiple_calls():
    actor = _LimitCounterImpl(limit=10, offset=15)
    actor.start_task("t1")
    # First batch fully consumed by offset.
    assert actor.claim("t1", 10) == (10, 0, False)
    # Second batch: 5 more to skip, then 10 to take.
    assert actor.claim("t1", 20) == (5, 10, True)


def test_start_task_rewinds_prior_claim():
    """A retried task gets its prior take/skip refunded to the global budget."""
    actor = _LimitCounterImpl(limit=100, offset=0)
    actor.start_task("t1")
    assert actor.claim("t1", 60) == (0, 60, False)
    assert actor.remaining_take == 40

    # Simulate retry: same input_id calls start_task again.
    actor.start_task("t1")
    assert actor.remaining_take == 100, "budget should be restored after rewind"
    # The retry can now claim up to the full limit again.
    assert actor.claim("t1", 80) == (0, 80, False)


def test_start_task_rewinds_offset_claim():
    """Rewind must restore offset progress too, not just take."""
    actor = _LimitCounterImpl(limit=10, offset=20)
    actor.start_task("t1")
    assert actor.claim("t1", 15) == (15, 0, False)
    assert actor.remaining_skip == 5

    actor.start_task("t1")
    assert actor.remaining_skip == 20, "offset progress should rewind"
    assert actor.remaining_take == 10


def test_start_task_rewind_isolated_per_task():
    """Rewinding t1 must not affect t2's claims."""
    actor = _LimitCounterImpl(limit=100, offset=0)
    actor.start_task("t1")
    actor.start_task("t2")
    actor.claim("t1", 30)  # t1 takes 30
    actor.claim("t2", 40)  # t2 takes 40
    assert actor.remaining_take == 30

    # Retry t1 only.
    actor.start_task("t1")
    # t1's 30 should be refunded; t2's 40 stays claimed.
    assert actor.remaining_take == 60
    # t2's bookkeeping should be intact.
    assert actor.input_claims["t2"] == (0, 40)


def test_double_start_task_is_idempotent():
    """Calling start_task twice with no intervening claim must not rewind twice."""
    actor = _LimitCounterImpl(limit=100, offset=0)
    actor.start_task("t1")
    actor.claim("t1", 30)
    assert actor.remaining_take == 70

    actor.start_task("t1")  # first rewind: refund 30
    actor.start_task("t1")  # second call: nothing to refund, must be a no-op
    assert actor.remaining_take == 100


def test_zero_claim_entries_dropped():
    """Tasks that never consume budget shouldn't accumulate in input_claims."""
    actor = _LimitCounterImpl(limit=5, offset=0)
    actor.start_task("t1")
    actor.claim("t1", 10)  # claims all 5
    assert actor.is_done()

    # Subsequent tasks past the limit get (0, 0, True) and shouldn't be retained.
    for i in range(100):
        tid = f"past_limit_{i}"
        actor.start_task(tid)
        assert actor.claim(tid, 50) == (0, 0, True)
        assert tid not in actor.input_claims, "past-limit task should not be retained"

    # Only the one boundary task remains.
    assert set(actor.input_claims.keys()) == {"t1"}


def test_is_done_transitions():
    actor = _LimitCounterImpl(limit=10, offset=0)
    assert not actor.is_done()
    actor.start_task("t1")
    actor.claim("t1", 5)
    assert not actor.is_done()
    actor.claim("t1", 5)
    assert actor.is_done()


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_distributed_limit_retries_after_worker_death(tmp_path):
    """`.limit(N)` must still produce N rows when a SwordfishTask crashes mid-claim.

    Without the rewind in `_LimitCounterImpl.start_task`, the failed attempt's
    claim stays charged against the global budget while its slice never reaches
    downstream — the retry then sees a smaller budget and the output undercounts.
    """
    import os

    import daft
    from daft import DataType, col, func

    marker = str(tmp_path / "crashed_once")

    @func(return_dtype=DataType.int64(), cpus=os.cpu_count())
    def crash_once_on_zero(v: int) -> int:
        import os

        if v == 0 and not os.path.exists(marker):
            with open(marker, "w") as f:
                f.write("crashed")
            # Hard-exit the swordfish actor process. Ray surfaces this as
            # ActorDiedError → RayTaskResult.worker_died() → dispatcher marks
            # WorkerDied. flotilla's RayWorkerManager.refresh_workers loop
            # then spawns a fresh actor on this node within ~5s, onto which
            # the failed task is re-dispatched. The retry's
            # DistributedLimitSink calls start_task(input_id), which refunds
            # the crashed attempt's prior claim before claiming again.
            os._exit(1)
        return v

    df = daft.range(0, 15, partitions=15).limit(3).select(crash_once_on_zero(col("id")))
    result = df.to_pydict()

    import os

    assert os.path.exists(marker), "UDF never crashed — retry path not exercised"
    # Single-CPU serialization makes contributor order deterministic: task 0
    # retries and finishes first, then tasks 1 and 2 run in sequence. Without
    # rewind in start_task, the crashed task's row would be missing
    # (e.g. [1, 2] instead of [0, 1, 2]).
    assert result["id"] == [0, 1, 2], (
        f"expected [0, 1, 2] after retry, got {result['id']}. "
        "If 0 is missing, the limit actor failed to rewind the crashed "
        "task's claim in start_task."
    )


def test_claim_signals_done_event():
    """`claim` must wake `await_limit_completion` rather than have it poll."""
    import asyncio

    async def run():
        actor = _LimitCounterImpl(limit=2, offset=0)
        waiter = asyncio.create_task(actor.await_limit_completion())
        # Let the waiter reach `Event.wait()` before the limit is satisfied, so
        # the test covers the wake-up rather than the already-done shortcut.
        await asyncio.sleep(0)
        assert not waiter.done()

        actor.start_task("t1")
        actor.claim("t1", 2)
        return await asyncio.wait_for(waiter, timeout=5)

    assert asyncio.run(run()) == ["t1"]


def test_done_event_set_before_any_waiter():
    """A limit reached before the first `await_limit_completion` still resolves.

    The event is created lazily — `__init__` runs before the actor has an event
    loop — so the first waiter has to notice a limit that was already satisfied
    instead of blocking forever.
    """
    import asyncio

    async def run():
        actor = _LimitCounterImpl(limit=1, offset=0)
        actor.start_task("t1")
        actor.claim("t1", 1)
        return await asyncio.wait_for(actor.await_limit_completion(), timeout=5)

    assert asyncio.run(run()) == ["t1"]


def test_retry_rewind_reopens_done_event():
    """A refund that lifts the budget back above zero must un-signal `done`.

    `start_task` rewinds a crashed attempt's claims. If the event stayed set,
    a waiter would conclude the limit was satisfied while the rewound rows had
    never reached downstream.
    """
    import asyncio

    async def run():
        actor = _LimitCounterImpl(limit=1, offset=0)
        actor.start_task("t1")
        actor.claim("t1", 1)
        assert actor.is_done()
        # Force the event into existence and into the set state.
        assert await asyncio.wait_for(actor.await_limit_completion(), timeout=5) == ["t1"]

        actor.start_task("t1")  # retry: refunds the claim
        assert not actor.is_done()
        waiter = asyncio.create_task(actor.await_limit_completion())
        await asyncio.sleep(0)
        assert not waiter.done(), "refund left the done event set"

        actor.claim("t2", 1)
        return await asyncio.wait_for(waiter, timeout=5)

    assert asyncio.run(run()) == ["t2"]


@pytest.mark.skip(
    reason="`limit` under a cross join hangs on main, unrelated to this test's subject. "
    "SwordfishTaskBuilder::combine_with resets notify_tokens/cancel_token to empty, so the "
    "oneshot sender LimitNode registered via add_notify_token is dropped when CrossJoinNode "
    "combines the builder. Its receiver then resolves Err, limit_execution_loop's "
    "`if let Ok(Ok(task_id))` swallows it, completed_ids never gains the task, and "
    "`contributors.is_subset(completed_ids)` is never satisfied. Re-enable once tokens survive "
    "combine_with."
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_cross_join_keeps_the_other_side_whole():
    """Satisfying a `LIMIT` must not stop the *other* side of a fused join.

    This is the strongest form of the invariant: `CrossJoinNode` is the one
    place that puts two *scan* sources into a single task with a
    `distributed_limit` over only one of them. `test_limit_under_broadcast_join_
    emits_every_limited_row` covers the same invariant on a path that works
    today, but its build side is an in-memory source.

    `CrossJoinNode` builds each task with `SwordfishTaskBuilder::combine_with`,
    which merges two plans — and their sources — into a single task. With a
    `limit` on one side, that task contains a `distributed_limit` plus a
    `ScanTaskSource` the limit does not own. If early-stop were signalled per
    `input_id` alone it would stop that scan too, and the join would silently
    lose rows. The signal is scoped to the limit's own subtree instead.

    Asserted on shape rather than on *which* rows survive: a distributed limit
    claims rows first-come-first-served across tasks, so the surviving set is
    not deterministic. The cross product's shape is: every row the limit let
    through must pair with every one of the right side's rows.
    """
    from collections import Counter

    import daft
    from daft import DataType, col, func

    limit = 100
    right_rows = 5

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        # Blocks limit pushdown into the scan, forcing the path where the
        # counter actor actually reports `done` mid-stream.
        return v

    left = daft.range(0, 400, partitions=4).select(identity(col("id")).alias("lid")).limit(limit)
    right = daft.range(0, right_rows, partitions=1).select(col("id").alias("rid"))

    result = left.join(right, how="cross").to_pydict()

    assert len(result["lid"]) == limit * right_rows, (
        f"cross join produced {len(result['lid'])} rows, expected {limit * right_rows}. "
        "A short count means the limit's early stop reached the join's other side."
    )
    assert Counter(result["rid"]) == {r: limit for r in range(right_rows)}, (
        f"right-side rows are not evenly paired: {Counter(result['rid'])}. "
        "The right scan was truncated by the limit's early stop."
    )


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_broadcast_join_emits_every_limited_row():
    """A satisfied `LIMIT` under a broadcast join must not drop probe rows.

    `PushDownLimit` leaves the `limit` on the join's receiver side, and
    `map_plan` fuses both into one task plan. The build side covers every
    possible key here, so each of the `limit` rows has exactly one match and the
    inner join has to emit exactly `limit` rows — no assumption about *which*
    rows the distributed limit let through, which is not deterministic.
    """
    import daft
    from daft import DataType, col, func

    limit = 200
    universe = 400

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        return v

    big = daft.range(0, universe, partitions=8).select(identity(col("id")).alias("id"))
    small = daft.from_pydict({"id": list(range(universe)), "tag": [f"t{i}" for i in range(universe)]})

    result = big.limit(limit).join(small, on="id").to_pydict()

    assert len(result["id"]) == limit, (
        f"join emitted {len(result['id'])} rows, expected {limit}. "
        "Rows the limit let through failed to find their match."
    )
    assert len(set(result["id"])) == limit, f"join duplicated rows: {result['id']}"
    # Each row must carry its own tag, not another row's — a partially built
    # hash table can match while pairing the wrong side.
    assert all(tag == f"t{i}" for i, tag in zip(result["id"], result["tag"]))


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_stops_upstream_work():
    """A satisfied `LIMIT` must stop upstream production, not just discard it.

    Regression test for the distributed limit doing no work-saving at all: the
    counter actor's `done` flag was dropped on the floor, so every task drained
    its whole partition through the (non-pushdown-able) UDF and the query cost
    the same as having no `LIMIT`.

    Asserts on rows actually fed to the UDF rather than on wall clock, so it
    does not depend on machine speed. The bound is deliberately loose — how much
    is saved depends on scheduling, and the counter increments are
    fire-and-forget so the total can undercount slightly — but the pre-fix
    behavior feeds the UDF *every* row, which is far outside it.
    """
    import daft
    from daft import DataType, col, func

    total_rows = 8 * 20_000

    @ray.remote(num_cpus=0)
    class RowCounter:
        def __init__(self) -> None:
            self.n = 0

        def add(self, n: int) -> None:
            self.n += n

        def get(self) -> int:
            return self.n

    counter = RowCounter.options(name="limit_row_counter", lifetime=None).remote()

    @func(return_dtype=DataType.bool())
    def keep_and_count(a: int) -> bool:
        import ray as _ray

        _ray.get_actor("limit_row_counter").add.remote(1)
        return a % 1_000 == 0

    try:
        # Small morsels so the early-stop signal has a chance to land partway
        # through a partition; the default morsel is larger than these
        # partitions, which would make the test measure nothing.
        daft.set_execution_config(default_morsel_size=2_000)
        df = daft.range(0, total_rows, partitions=8).where(keep_and_count(col("id"))).limit(4)
        result = df.to_pydict()
        assert len(result["id"]) == 4

        processed = ray.get(counter.get.remote())
        assert processed < total_rows * 0.9, (
            f"UDF saw {processed} of {total_rows} rows; the limit saved almost nothing. "
            "The DistributedLimitSink is not honoring the counter actor's `done` flag, "
            "or the cancellation is not reaching the source."
        )
    finally:
        daft.set_execution_config(default_morsel_size=None)
        ray.kill(counter)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_larger_than_input_reads_everything():
    """`limit > total rows` never satisfies the counter, so nothing is cancelled.

    The complement of `test_limit_stops_upstream_work`: this query legitimately
    has to read all of its input, and early stopping must not truncate it.
    """
    import daft
    from daft import DataType, col, func

    @func(return_dtype=DataType.bool())
    def keep(a: int) -> bool:
        return a % 3 == 0

    df = daft.range(0, 300, partitions=8).where(keep(col("id"))).limit(10_000)
    assert len(df.to_pydict()["id"]) == 100
