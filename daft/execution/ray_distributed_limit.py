from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.actor import ActorHandle as RayActorHandle

try:
    import ray
except ImportError:
    raise ImportError("Ray is required for the distributed limit operator. Install it with: pip install ray") from None


class _LimitCounterImpl:
    """Pure-Python state machine for the distributed Limit operator.

    Holds `(remaining_skip, remaining_take)` for the whole query and exposes an
    atomic `claim(task_id, num_rows) -> (skip, take, done)` interface.

    Idempotent across SwordfishTask retries: if a task is retried, its prior
    claims are rewound on the next `start_task(task_id)` call so the new
    attempt starts from the same global state the failed one saw.

    Wrapped as a Ray actor by `LimitCounterActor` below. Kept as a separate
    plain class so unit tests can exercise the state-machine logic without
    standing up a Ray cluster.
    """

    def __init__(self, limit: int, offset: int) -> None:
        self.remaining_skip = offset
        self.remaining_take = limit
        # task_id -> (cumulative_skip_claimed, cumulative_take_claimed)
        self.task_claims: dict[str, tuple[int, int]] = {}

    def start_task(self, task_id: str) -> None:
        """Rewind any prior claim made by `task_id` (retry semantics).

        If a prior attempt of `task_id` consumed budget, refund it to the
        global budget; otherwise this is a no-op. Entries are added to
        `task_claims` lazily by `claim` only when budget is actually
        consumed, so `task_claims` stays bounded by the number of boundary
        tasks rather than the total tasks the query ever scheduled.
        """
        prior = self.task_claims.pop(task_id, None)
        if prior is not None:
            skip, take = prior
            self.remaining_skip += skip
            self.remaining_take += take

    def claim(self, task_id: str, num_rows: int) -> tuple[int, int, bool]:
        """Atomically claim up to `num_rows` rows of the global budget for this task.

        Returns `(skip, take, done)`:
          - `skip`: how many rows of the incoming batch this task must drop
            (consume against the offset).
          - `take`: how many of the remaining rows this task should emit.
          - `done`: True if the limit is now fully claimed; the caller should
            finish after emitting `take` rows.

        `skip + take <= num_rows`. The unused `num_rows - skip - take` rows are
        beyond the limit and should be discarded.
        """
        if self.remaining_take == 0:
            return (0, 0, True)

        skip = min(self.remaining_skip, num_rows)
        self.remaining_skip -= skip
        num_rows -= skip

        take = min(self.remaining_take, num_rows)
        self.remaining_take -= take

        if skip > 0 or take > 0:
            prev_skip, prev_take = self.task_claims.get(task_id, (0, 0))
            self.task_claims[task_id] = (prev_skip + skip, prev_take + take)

        done = self.remaining_take == 0
        return (skip, take, done)

    def is_done(self) -> bool:
        return self.remaining_take == 0


LimitCounterActor = ray.remote(num_cpus=0)(_LimitCounterImpl)


class LimitCounterHandle:
    """Rust-facing wrapper around the Ray `LimitCounterActor`.

    Mirrors `UDFActorHandle` in shape so the existing `PyObjectWrapper`
    path picks it up. Async methods are awaitable from Rust via
    `common_runtime::python::execute_python_coroutine`.
    """

    def __init__(self, actor_ref: RayActorHandle) -> None:
        self.actor = actor_ref

    async def start_task(self, task_id: str) -> None:
        await self.actor.start_task.remote(task_id)

    async def claim(self, task_id: str, num_rows: int) -> tuple[int, int, bool]:
        return await self.actor.claim.remote(task_id, num_rows)

    async def is_done(self) -> bool:
        return await self.actor.is_done.remote()

    def teardown(self) -> None:
        ray.kill(self.actor)


async def start_limit_counter_actor(
    limit: int,
    offset: int,
    timeout: int,
) -> LimitCounterHandle:
    """Spawn the `LimitCounterActor` and wait for it to be ready."""
    actor = LimitCounterActor.options(
        scheduling_strategy="DEFAULT",
    ).remote(limit, offset)

    ready_future = asyncio.wrap_future(actor.__ray_ready__.remote().future())
    ready, _ = await asyncio.wait([ready_future], return_when=asyncio.ALL_COMPLETED, timeout=timeout)
    if not ready:
        raise RuntimeError(f"LimitCounterActor failed to start within {timeout} seconds")
    await asyncio.gather(*ready)

    return LimitCounterHandle(actor)
