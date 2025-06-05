from __future__ import annotations

import os
from typing import TYPE_CHECKING

try:
    import ray
    import ray.util.scheduling_strategies
except ImportError:
    pass

from daft.expressions import Expression, ExpressionsProjection
from daft.recordbatch.micropartition import MicroPartition

if TYPE_CHECKING:
    from daft.daft import PyExpr, PyMicroPartition


@ray.remote(num_cpus=0)
class ActorPoolUDF:
    def __init__(self, uninitialized_projection: ExpressionsProjection) -> None:
        del os.environ["CUDA_VISIBLE_DEVICES"]
        self.projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        mp = MicroPartition._from_pymicropartition(input)
        return mp.eval_expression_list(self.projection)._micropartition


class ActorHandle:
    def __init__(self, projection: list[PyExpr]) -> None:
        expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
        self.actor = ActorPoolUDF.options(  # type: ignore
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=ray.get_runtime_context().get_node_id(),
                soft=False,
            )
        ).remote(expr_projection)

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        return ray.get(self.actor.eval_input.remote(input))

    def teardown(self, timeout: float = 5.0) -> None:
        ray.kill(self.actor)
