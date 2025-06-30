from __future__ import annotations

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
    def __init__(self, node_id: str, actor_ref: ray.ObjectRef) -> None:
        self.node_id = node_id
        self.actor = actor_ref

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        return ray.get(self.actor.eval_input.remote(input))

    def teardown(self) -> None:
        ray.kill(self.actor)


def start_udf_actors(
    projection: list[PyExpr],
    num_actors: int,
    num_gpus_per_actor: float,
    memory_per_actor: float,
    num_cpus_per_actor: float,
) -> list[tuple[str, list[UDFActorHandle]]]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
    handles: dict[str, list[UDFActorHandle]] = {}
    actors = [
        UDFActor.options(  # type: ignore
            scheduling_strategy="SPREAD",
            num_gpus=num_gpus_per_actor,
            num_cpus=num_cpus_per_actor,
            memory=memory_per_actor,
        ).remote(expr_projection)
        for _ in range(num_actors)
    ]
    node_ids = ray.get([actor.get_node_id.remote() for actor in actors])
    for actor, node_id in zip(actors, node_ids):
        handles.setdefault(node_id, []).append(UDFActorHandle(node_id, actor))

    res = [(node_id, handles) for node_id, handles in handles.items()]
    return res
