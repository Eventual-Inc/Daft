from __future__ import annotations

import asyncio
import logging

from daft.runners import flotilla


class _FakeActor:
    """Stand-in for the Ray actor handle wrapped by ``RaySwordfishActorHandle``."""

    def __init__(self) -> None:
        self.calls: list[list[int]] = []
        self.unregister_shuffles = self

    def remote(self, shuffle_ids: list[int]) -> str:
        self.calls.append(shuffle_ids)
        return f"ref-for-{shuffle_ids}"


def test_actor_handle_starts_the_unregistration_without_waiting():
    """The handle hands back the pending call so the coordinator can wait once.

    Awaiting per worker would serialize the fan-out behind whichever worker is
    slowest to answer, on a path that runs after the query's results are already
    delivered.
    """
    actor = _FakeActor()
    handle = flotilla.RaySwordfishActorHandle(actor)

    ref = handle.unregister_shuffles([7, 9])

    assert actor.calls == [[7, 9]]
    assert ref == "ref-for-[7, 9]"


async def _count(n: int) -> int:
    return n


async def _boom() -> int:
    raise RuntimeError("worker went away")


def test_awaiting_unregistrations_tolerates_a_dead_worker(caplog):
    """One unreachable worker must not fail the others or the query.

    A worker that cannot answer has already lost the registry we were asking it
    to trim, so the failure is logged and the remaining workers still report.
    """
    with caplog.at_level(logging.DEBUG, logger="daft.runners.flotilla"):
        asyncio.run(flotilla.await_flight_shuffle_unregistrations([_count(3), _boom(), _count(5)]))

    assert "worker went away" in caplog.text
    assert "Dropped 8 flight shuffle registration(s) across 3 worker(s)" in caplog.text


def test_awaiting_no_unregistrations_is_a_noop():
    """No shuffles, or no live workers, means nothing to wait for."""
    asyncio.run(flotilla.await_flight_shuffle_unregistrations([]))
