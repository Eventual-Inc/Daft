from __future__ import annotations

import logging
import multiprocessing as mp
from typing import TYPE_CHECKING

from daft.expressions import Expression, ExpressionsProjection
from daft.table import MicroPartition

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

    from daft.daft import PyExpr, PyMicroPartition

logger = logging.getLogger(__name__)


def initialize_actor_pool_projection(projection: ExpressionsProjection) -> ExpressionsProjection:
    """Initializes the stateful UDFs in the projection."""

    from daft.daft import extract_partial_stateful_udf_py

    partial_stateful_udfs = {
        name: psu for expr in projection for name, psu in extract_partial_stateful_udf_py(expr._expr).items()
    }

    logger.info("Initializing stateful UDFs: %s", ", ".join(partial_stateful_udfs.keys()))

    initialized_stateful_udfs = {}
    for name, (partial_udf, init_args) in partial_stateful_udfs.items():
        if init_args is None:
            initialized_stateful_udfs[name] = partial_udf.func_cls()
        else:
            args, kwargs = init_args
            initialized_stateful_udfs[name] = partial_udf.func_cls(*args, **kwargs)

    initialized_projection = ExpressionsProjection(
        [e._bind_stateful_udfs(initialized_stateful_udfs) for e in projection]
    )

    return initialized_projection


def stateful_actor_event_loop(uninitialized_projection: ExpressionsProjection, conn: Connection) -> None:
    """
    Event loop that runs in a stateful actor process and receives MicroPartitions to evaluate with a stateful UDF.

    Terminates once it receives None.
    """
    initialized_projection = initialize_actor_pool_projection(uninitialized_projection)

    while True:
        input: MicroPartition | None = conn.recv()
        if input is None:
            break

        output = input.eval_expression_list(initialized_projection)
        conn.send(output)


class StatefulActorHandle:
    """Handle class for initializing, interacting with, and tearing down a single local stateful actor process."""

    def __init__(self, projection: list[PyExpr]) -> None:
        self.handle_conn, actor_conn = mp.Pipe()

        expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
        self.actor_process = mp.Process(target=stateful_actor_event_loop, args=(expr_projection, actor_conn))
        self.actor_process.start()

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        self.handle_conn.send(MicroPartition._from_pymicropartition(input))
        output: MicroPartition = self.handle_conn.recv()
        return output._micropartition

    def teardown(self) -> None:
        self.handle_conn.send(None)
        self.handle_conn.close()
        self.actor_process.join()
