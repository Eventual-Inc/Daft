from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any

from daft.expressions.expressions import Expression, ExpressionsProjection
from daft.recordbatch.micropartition import MicroPartition

if TYPE_CHECKING:
    from ray.actor import ActorHandle as RayActorHandle

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
    def __init__(self, actor_ref: RayActorHandle) -> None:
        self.actor = actor_ref

    def actor_id(self) -> str:
        return self.actor._actor_id.hex()

    async def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        return await self.actor.eval_input.remote(input)

    def teardown(self) -> None:
        ray.kill(self.actor)


def get_ready_actors_by_location(
    actor_handles: list[UDFActorHandle],
) -> tuple[list[UDFActorHandle], list[UDFActorHandle]]:
    from ray._private.state import actors

    current_node_id = ray.get_runtime_context().get_node_id()

    local_actors = []
    remote_actors = []
    for actor_handle in actor_handles:
        actor_id = actor_handle.actor_id()
        actor_state = actors(actor_id)
        if actor_state["Address"]["NodeID"] == current_node_id:
            local_actors.append(actor_handle)
        else:
            remote_actors.append(actor_handle)

    return local_actors, remote_actors


async def start_udf_actors(
    projection: list[PyExpr],
    num_actors: int,
    num_gpus_per_actor: float,
    num_cpus_per_actor: float,
    memory_per_actor: float,
    ray_options: dict[str, Any] | None,
    timeout: int,
    actor_name: str | None = None,
) -> list[UDFActorHandle]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])

    random_suffix = str(uuid.uuid4())[:8]
    base_opts: dict[str, Any] = {
        "scheduling_strategy": "SPREAD",
        "num_gpus": num_gpus_per_actor,
        "num_cpus": num_cpus_per_actor,
        "memory": memory_per_actor,
    }
    if ray_options and isinstance(ray_options.get("label_selector"), dict):
        base_opts["label_selector"] = ray_options["label_selector"]

    # Normalize label_selector for older Ray versions
    from daft.runners.ray_compat import normalize_ray_options_with_label_selector

    normalized_opts = normalize_ray_options_with_label_selector(base_opts.copy())
    actors: list[RayActorHandle] = [
        UDFActor.options(  # type: ignore
            name=None if actor_name is None else f"{actor_name}:{random_suffix}-{rank}",
            **normalized_opts,
        ).remote(expr_projection)
        for rank in range(num_actors)
    ]

    # Wait for actors to be ready
    ready_futures = [asyncio.wrap_future(actor.__ray_ready__.remote().future()) for actor in actors]
    ready_refs, _ = await asyncio.wait(ready_futures, return_when=asyncio.ALL_COMPLETED, timeout=timeout)

    # Verify that the __ray_ready__ calls were successful
    await asyncio.gather(*ready_refs)

    if not ready_refs:
        raise RuntimeError(
            f"UDF actors failed to start within {timeout} seconds, please increase the actor_udf_ready_timeout config via daft.set_execution_config(actor_udf_ready_timeout=timeout)"
        )

    # Return the ready actors
    ready_indices = [ready_futures.index(ref) for ref in ready_refs]
    ready_actors = [UDFActorHandle(actors[i]) for i in ready_indices]
    return ready_actors
