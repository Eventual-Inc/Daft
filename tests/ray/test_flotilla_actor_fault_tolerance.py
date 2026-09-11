"""Fault-tolerance tests for RaySwordfishActor.

Covers the fix for node-level compute loss on actor crash: swordfish actors
are created with ``max_restarts > 0`` so a transient crash (e.g. OOM kill) is
restarted in place by Ray instead of permanently evicting the node from the
worker pool.

Context (upstream Eventual-Inc/Daft):
- PR #4628 (feat(flotilla): Fault tolerance) introduced dispatcher-level
  rescheduling on WorkerDied/WorkerUnavailable.
- PR #7116 bumped the ray floor to >=2.11.0 because Daft relies on
  ``ActorUnavailableError`` to distinguish a restarting actor from a dead one.
- Without ``max_restarts``, ``ActorUnavailableError`` is never raised for a
  crashed actor -- only the terminal ``ActorDiedError`` path is reachable.
"""

from __future__ import annotations

import asyncio

import pytest

from daft.runners import flotilla

# ---------------------------------------------------------------------------
# _get_swordfish_actor_max_restarts env parsing
# ---------------------------------------------------------------------------


def test_max_restarts_default_when_env_unset(monkeypatch):
    monkeypatch.delenv(flotilla.SWORDFISH_ACTOR_MAX_RESTARTS_ENV, raising=False)
    assert flotilla._get_swordfish_actor_max_restarts() == flotilla.DEFAULT_SWORDFISH_ACTOR_MAX_RESTARTS
    # The whole point of the fix: the default must enable in-place restarts.
    assert flotilla.DEFAULT_SWORDFISH_ACTOR_MAX_RESTARTS > 0


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0", 0),  # explicit opt-out restores the old behavior
        ("7", 7),
        ("-1", -1),  # infinite restarts
    ],
)
def test_max_restarts_env_override(monkeypatch, raw, expected):
    monkeypatch.setenv(flotilla.SWORDFISH_ACTOR_MAX_RESTARTS_ENV, raw)
    assert flotilla._get_swordfish_actor_max_restarts() == expected


@pytest.mark.parametrize("raw", ["", "abc", "1.5"])
def test_max_restarts_invalid_env_falls_back_to_default(monkeypatch, raw):
    monkeypatch.setenv(flotilla.SWORDFISH_ACTOR_MAX_RESTARTS_ENV, raw)
    assert flotilla._get_swordfish_actor_max_restarts() == flotilla.DEFAULT_SWORDFISH_ACTOR_MAX_RESTARTS


# ---------------------------------------------------------------------------
# RaySwordfishTaskHandle actor-error -> RayTaskResult mapping
# ---------------------------------------------------------------------------
#
# The dispatcher (src/daft-distributed/src/scheduling/dispatcher.rs) treats:
# - WorkerDied        -> evict the worker + requeue the task (slow path)
# - WorkerUnavailable -> requeue the task, keep the worker   (fast path)
# With max_restarts > 0, Ray raises ActorUnavailableError while the actor is
# restarting, so a crashed-but-restartable actor takes the fast path and the
# node's compute is not lost. These tests pin that error-mapping contract.


def _actor_error(name: str) -> Exception:
    # Construct without invoking Ray's constructor (signature varies by version).
    cls = getattr(flotilla.ray.exceptions, name)
    return cls.__new__(cls)


class _FakeResultHandle:
    """Stand-in for the ObjectRefGenerator returned by ``run_plan.remote()``."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def completed(self) -> None:
        raise self._exc


class _FakeRayTaskResult:
    @staticmethod
    def worker_died() -> str:
        return "worker_died"

    @staticmethod
    def worker_unavailable() -> str:
        return "worker_unavailable"


@pytest.mark.parametrize(
    ("error_name", "expected"),
    [
        # Permanently dead (max_restarts exhausted, or restarts disabled).
        ("ActorDiedError", "worker_died"),
        # Node churned away before the actor could be scheduled.
        ("ActorUnschedulableError", "worker_died"),
        # Actor crashed but Ray is restarting it in place (max_restarts > 0):
        # the task must be requeued WITHOUT evicting the node.
        ("ActorUnavailableError", "worker_unavailable"),
    ],
)
def test_task_handle_maps_actor_errors(monkeypatch, error_name, expected):
    monkeypatch.setattr(flotilla, "RayTaskResult", _FakeRayTaskResult)

    handle = flotilla.RaySwordfishTaskHandle(result_handle=_FakeResultHandle(_actor_error(error_name)))
    result = asyncio.run(handle.get_result())

    assert result == expected


def test_task_handle_propagates_non_actor_errors(monkeypatch):
    """Genuine task failures (e.g. UDF errors) must not be masked as worker churn."""
    monkeypatch.setattr(flotilla, "RayTaskResult", _FakeRayTaskResult)

    handle = flotilla.RaySwordfishTaskHandle(result_handle=_FakeResultHandle(ValueError("boom")))

    with pytest.raises(ValueError, match="boom"):
        asyncio.run(handle.get_result())
