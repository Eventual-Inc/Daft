"""Unit tests for the distributed-Limit state machine.

Exercises `_LimitCounterImpl` directly (no Ray cluster). The interesting
invariants here are the retry-rewind path in `start_task` — failed
SwordfishTask attempts must release their claimed budget back so the retry
emits the right total. Dataframe-level limit tests don't fail tasks, so this
path is otherwise uncovered.
"""

from __future__ import annotations

import threading

import pytest

ray = pytest.importorskip("ray")

import daft
from daft import DataType, col, func
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


def _materialize_with_deadline(df, timeout_s=120):
    """Materialize `df` in a daemon thread, failing the test if it doesn't finish.

    `@pytest.mark.timeout` can't guard the deadlock these tests cover: the query
    blocks inside the Rust runtime holding the GIL, so a SIGALRM handler never
    gets a chance to run and pytest-timeout's signal method hangs right along
    with it. A watchdog in a separate thread is the only one that can report.
    The thread is a daemon so a regression fails this test and lets the rest of
    the session continue instead of wedging the run.
    """
    outcome: dict = {}

    def target():
        try:
            outcome["result"] = df.to_pydict()
        except BaseException as e:
            outcome["error"] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        pytest.fail(f"query did not finish within {timeout_s}s — the distributed limit deadlocked instead of returning")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.parametrize("num_partitions", [1, 4, 16])
def test_limit_under_into_partitions_does_not_hang(num_partitions):
    """`into_partitions` on top of a `Limit` must not deadlock the query.

    `PushDownLimit` commutes `Limit-IntoPartitions` into `IntoPartitions-Limit`,
    so `IntoPartitionsNode` ends up consuming `LimitNode`'s task-builder stream.
    It has to count its input tasks before it can decide whether to coalesce or
    split, so it drains that stream to exhaustion before submitting anything —
    while `LimitNode`'s loop only finishes once the tasks it emitted have run
    and claimed rows from the counter actor. If `LimitNode` holds its output
    channel open until its loop ends, neither side can move: no task is ever
    submitted, no `claim` ever arrives, and the query hangs forever with the
    actor spinning in `_LimitCounterImpl.await_limit_completion`.

    `num_partitions` covers all three `IntoPartitionsNode` shapes against the
    8-task input: coalesce to one task, coalesce to four, and split to sixteen.
    """
    df = daft.range(0, 10_000, partitions=8).into_partitions(num_partitions).limit(10)
    result = _materialize_with_deadline(df)

    assert len(result["id"]) == 10
    assert len(set(result["id"])) == 10, f"limit returned duplicate rows: {result['id']}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_into_partitions_offset_and_overshoot():
    """The same deadlock, on the paths that don't early-stop.

    With `limit > total rows` the counter never reaches zero, so the loop has to
    end by draining its input rather than by the actor reporting completion —
    the output channel has to be released in that case too. `limit(0)` is the
    other no-contributor path.
    """
    df = daft.range(0, 100, partitions=8).into_partitions(3).offset(10).limit(20)
    assert len(_materialize_with_deadline(df)["id"]) == 20

    df = daft.range(0, 100, partitions=8).into_partitions(3).limit(1000)
    assert len(_materialize_with_deadline(df)["id"]) == 100

    df = daft.range(0, 100, partitions=8).into_partitions(3).limit(0)
    assert len(_materialize_with_deadline(df)["id"]) == 0


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.parametrize("limit_on_left", [True, False])
def test_limit_under_cross_join_does_not_hang(limit_on_left):
    """A `LIMIT` on either side of a cross join must not deadlock the query.

    `CrossJoinNode` builds its tasks with `SwordfishTaskBuilder::combine_with`,
    fusing a builder from each side into one task and keeping both sides'
    builders as templates to cross with whatever arrives later. So a builder
    `LimitNode` forwarded turns into *n* tasks, none of which is the builder
    that was handed to the join.

    `combine_with` used to drop the notify token and the cancel token when it
    merged, which stranded `LimitNode`: the tasks that actually ran reported to
    nobody, `contributors.is_subset(completed_ids)` was never satisfied, and the
    query hung forever with no error and no timeout. Tokens now carry into every
    derived task, so the limit loop sees each one finish.

    Asserted on shape rather than on which rows survive: a distributed limit
    claims rows first-come-first-served across tasks, so the surviving set is
    not deterministic. The cross product's shape is: every row the limit let
    through pairs with every one of the other side's rows.
    """
    from collections import Counter

    limit = 100
    other_rows = 5

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        # Blocks limit pushdown into the scan, so the limit is a real
        # `distributed_limit` inside the fused cross-join task.
        return v

    limited = daft.range(0, 400, partitions=4).select(identity(col("id")).alias("lid")).limit(limit)
    whole = daft.range(0, other_rows, partitions=1).select(col("id").alias("rid"))

    joined = limited.join(whole, how="cross") if limit_on_left else whole.join(limited, how="cross")
    result = _materialize_with_deadline(joined)

    assert len(result["lid"]) == limit * other_rows, (
        f"cross join produced {len(result['lid'])} rows, expected {limit * other_rows}"
    )
    assert Counter(result["rid"]) == {r: limit for r in range(other_rows)}, (
        f"rows are not evenly paired: {Counter(result['rid'])}"
    )


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_cross_join_natural_drain():
    """The cross-join deadlock on the path where the limit is never hit.

    With `limit > total rows` the counter actor never reports completion, so the
    limit loop can only end by draining: input exhausted *and* nothing derived
    from a forwarded builder still running. Cross join's templates keep deriving
    tasks after the input is exhausted, so "nothing running right now" is not
    enough — tearing the counter actor down there kills an actor that in-flight
    tasks still talk to, and they retry against it forever.
    """
    limited = daft.range(0, 20, partitions=4).limit(1000)
    whole = daft.range(0, 3, partitions=1).select(col("id").alias("rid"))

    result = _materialize_with_deadline(limited.join(whole, how="cross"))
    assert len(result["id"]) == 20 * 3


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.xfail(
    strict=True,
    reason="Known defect, independent of the deadlock this file's other cross-join tests cover: "
    "CrossJoinNode fuses the limited side into one task per builder on the other side, and every "
    "one of those copies claims from the same counter actor. The budget is meant to be shared "
    "across distinct input partitions, not across duplicates of the same partition, so each row "
    "the limit lets through survives in only one of the tasks it should appear in. The native "
    "runner returns the full cross product for this query.",
)
def test_limit_under_cross_join_pairs_with_every_partition_of_the_other_side():
    """Every row a `LIMIT` lets through must pair with the *whole* other side.

    The other side has more than one partition here, so `CrossJoinNode` fans
    each limited builder out into several tasks — the case where the shared
    counter actor silently drops rows. With a single-partition other side (the
    other cross-join tests here) each builder becomes exactly one task and the
    count comes out right, which is why this needs its own case.
    """
    limit = 100
    other_rows, other_partitions = 50, 2

    limited = daft.range(0, 400, partitions=4).limit(limit)
    whole = daft.range(0, other_rows, partitions=other_partitions).select(col("id").alias("rid"))

    result = _materialize_with_deadline(limited.join(whole, how="cross"))
    assert len(result["rid"]) == limit * other_rows
