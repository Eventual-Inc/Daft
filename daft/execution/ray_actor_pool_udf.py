from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from daft.expressions.expressions import Expression, ExpressionsProjection
from daft.recordbatch.micropartition import MicroPartition

if TYPE_CHECKING:
    from daft.daft import PyExpr, PyMicroPartition

try:
    import ray
except ImportError:
    raise

MAX_UDFACTOR_ACTOR_RESTARTS = 4
MAX_UDFACTOR_ACTOR_TASK_RETRIES = 4


@ray.remote(
    max_restarts=MAX_UDFACTOR_ACTOR_RESTARTS,
    max_task_retries=MAX_UDFACTOR_ACTOR_TASK_RETRIES,
)
class UDFActor:
    def __init__(self, uninitialized_projection: ExpressionsProjection) -> None:
        self.projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

    def get_node_id(self) -> str:
        return ray.get_runtime_context().get_node_id()

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        mp = MicroPartition._from_pymicropartition(input)
        res = mp.eval_expression_list(self.projection)
        return res._micropartition


class UDFActorHandle:
    def __init__(self, actor_ref: ray.ObjectRef) -> None:
        self.actor = actor_ref

    def actor_ref(self) -> ray.ObjectRef:
        return self.actor

    async def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        return await self.actor.eval_input.remote(input)

    def teardown(self) -> None:
        ray.kill(self.actor)


async def get_ready_actors_by_location(
    actor_handles: list[UDFActorHandle],
    timeout: int,
) -> tuple[list[UDFActorHandle], list[UDFActorHandle]]:
    current_node_id = ray.get_runtime_context().get_node_id()

    # Wait for actors to be ready
    ready_futures = [
        asyncio.wrap_future(handle.actor_ref().__ray_ready__.remote().future()) for handle in actor_handles
    ]
    ready_refs, _ = await asyncio.wait(ready_futures, return_when=asyncio.ALL_COMPLETED, timeout=timeout)
    await asyncio.gather(*ready_refs)

    if not ready_refs:
        raise RuntimeError(
            f"UDF actors failed to start within {timeout} seconds, please increase the actor_udf_ready_timeout config via daft.set_execution_config(actor_udf_ready_timeout=timeout)"
        )

    # Get ready actor handles
    ready_indices = [ready_futures.index(ref) for ref in ready_refs]
    ready_handles = [actor_handles[i] for i in ready_indices]

    # Get node IDs for ready actors
    actor_node_ids = await asyncio.gather(*[actor_handles[i].actor_ref().get_node_id.remote() for i in ready_indices])

    # Categorize by location
    local_actors = [handle for handle, node_id in zip(ready_handles, actor_node_ids) if node_id == current_node_id]
    remote_actors = [handle for handle, node_id in zip(ready_handles, actor_node_ids) if node_id != current_node_id]

    return local_actors, remote_actors


async def start_udf_actors(
    projection: list[PyExpr],
    num_actors: int,
    num_gpus_per_actor: float,
    num_cpus_per_actor: float,
    memory_per_actor: float,
    timeout: int,
) -> list[UDFActorHandle]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])

    actors = [
        UDFActor.options(  # type: ignore
            scheduling_strategy="SPREAD",
            num_gpus=num_gpus_per_actor,
            num_cpus=num_cpus_per_actor,
            memory=memory_per_actor,
        ).remote(expr_projection)
        for _ in range(num_actors)
    ]

    # Wait for actors to be ready
    ready_futures = [asyncio.wrap_future(actor.__ray_ready__.remote().future()) for actor in actors]
    ready_refs, _ = await asyncio.wait(ready_futures, return_when=asyncio.ALL_COMPLETED, timeout=timeout)
    await asyncio.gather(*ready_refs)

    if not ready_refs:
        raise RuntimeError(
            f"UDF actors failed to start within {timeout} seconds, please increase the actor_udf_ready_timeout config via daft.set_execution_config(actor_udf_ready_timeout=timeout)"
        )

    # Return all actors as long as at least one is ready, as some might come up later
    handles = [UDFActorHandle(actor) for actor in actors]
    return handles
