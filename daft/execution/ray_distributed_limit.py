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
    """`(skip, take, done)` state machine for the distributed Limit operator.

    Kept as a plain class so unit tests can exercise it without standing up
    a Ray cluster; `LimitCounterActor` below wraps it in `ray.remote`.
    """

    def __init__(self, limit: int, offset: int) -> None:
        self.remaining_skip = offset
        self.remaining_take = limit
        # input_id -> (cumulative_skip_claimed, cumulative_take_claimed)
        self.input_claims: dict[str, tuple[int, int]] = {}

    def start_task(self, input_id: str) -> None:
        # Called by a worker once when it begins processing `input_id`. If
        # we've seen this input_id before, it means a prior SwordfishTask
        # attempt crashed mid-claim and is now being retried — refund its
        # partial claims so the retry's claims don't double-count against
        # the global limit. No-op on the common (non-retry) path.
        prior = self.input_claims.pop(input_id, None)
        if prior is not None:
            skip, take = prior
            self.remaining_skip += skip
            self.remaining_take += take

    def claim(self, input_id: str, num_rows: int) -> tuple[int, int, bool]:
        if self.remaining_take == 0:
            return (0, 0, True)

        skip = min(self.remaining_skip, num_rows)
        self.remaining_skip -= skip
        num_rows -= skip

        take = min(self.remaining_take, num_rows)
        self.remaining_take -= take

        if skip > 0 or take > 0:
            prev_skip, prev_take = self.input_claims.get(input_id, (0, 0))
            self.input_claims[input_id] = (prev_skip + skip, prev_take + take)

        return (skip, take, self.remaining_take == 0)

    def is_done(self) -> bool:
        return self.remaining_take == 0

    def contributors(self) -> list[str]:
        return [iid for iid, (_skip, take) in self.input_claims.items() if take > 0]

    async def await_limit_completion(self) -> list[str]:
        while not self.is_done():
            await asyncio.sleep(0.01)
        return self.contributors()


LimitCounterActor = ray.remote(num_cpus=0)(_LimitCounterImpl)


class LimitCounterHandle:
    def __init__(self, actor_ref: RayActorHandle) -> None:
        self.actor = actor_ref

    async def start_task(self, input_id: str) -> None:
        try:
            await self.actor.start_task.remote(input_id)
        except ray.exceptions.ActorDiedError:
            # Safety net for the residual race between `LimitNode`'s
            # `ray.kill` and an in-flight worker call. `LimitNode` drains
            # task cancellations before tearing the actor down, so by the
            # time this fires the corresponding worker task is on its way
            # out and the refund this would have done is moot.
            pass

    async def claim(self, input_id: str, num_rows: int) -> tuple[int, int, bool]:
        try:
            return await self.actor.claim.remote(input_id, num_rows)
        except ray.exceptions.ActorDiedError:
            # Same residual-race rationale as `start_task`. Returning done
            # makes the sink emit an empty slice instead of letting
            # `ActorDiedError` propagate up, kill the worker's pipeline,
            # and surface as "Plan execution task has died" on concurrent
            # inputs sharing the same plan.
            return (0, 0, True)

    async def await_limit_completion(self) -> list[str]:
        return await self.actor.await_limit_completion.remote()

    def teardown(self) -> None:
        ray.kill(self.actor)


async def start_limit_counter_actor(limit: int, offset: int, timeout: int) -> LimitCounterHandle:
    # Pin to the runner's node so claim/done RPCs don't cross-hop.
    node_id = ray.get_runtime_context().get_node_id()
    actor = LimitCounterActor.options(
        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=node_id,
            soft=False,
        ),
    ).remote(limit, offset)
    # Block until the actor's constructor has finished before returning the
    # handle. `__ray_ready__` is Ray's actor-init sentinel; `wrap_future`
    # adapts its concurrent.futures.Future into an asyncio future so we can
    # apply `wait_for` and surface a clean timeout if scheduling stalls.
    try:
        await asyncio.wait_for(
            asyncio.wrap_future(actor.__ray_ready__.remote().future()),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        raise RuntimeError(f"LimitCounterActor failed to start within {timeout} seconds") from None
    return LimitCounterHandle(actor)
