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

    # Simulate retry: same task_id calls start_task again.
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
    assert actor.task_claims["t2"] == (0, 40)


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
    """Tasks that never consume budget shouldn't accumulate in task_claims."""
    actor = _LimitCounterImpl(limit=5, offset=0)
    actor.start_task("t1")
    actor.claim("t1", 10)  # claims all 5
    assert actor.is_done()

    # Subsequent tasks past the limit get (0, 0, True) and shouldn't be retained.
    for i in range(100):
        tid = f"past_limit_{i}"
        actor.start_task(tid)
        assert actor.claim(tid, 50) == (0, 0, True)
        assert tid not in actor.task_claims, "past-limit task should not be retained"

    # Only the one boundary task remains.
    assert set(actor.task_claims.keys()) == {"t1"}


def test_is_done_transitions():
    actor = _LimitCounterImpl(limit=10, offset=0)
    assert not actor.is_done()
    actor.start_task("t1")
    actor.claim("t1", 5)
    assert not actor.is_done()
    actor.claim("t1", 5)
    assert actor.is_done()
