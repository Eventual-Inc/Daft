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


def actor_event_loop(uninitialized_projection: ExpressionsProjection, conn: Connection) -> None:
    """Event loop that runs in a actor process and receives MicroPartitions to evaluate with an initialized UDF projection.

    Terminates once it receives None.
    """
    initialized_projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

    while True:
        input: MicroPartition | None = conn.recv()
        if input is None:
            break

        output = input.eval_expression_list(initialized_projection)
        conn.send(output)


class ActorHandle:
    """Handle class for initializing, interacting with, and tearing down a single local actor process."""

    def __init__(self, projection: list[PyExpr]) -> None:
        self.handle_conn, actor_conn = mp.Pipe()

        expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
        self.actor_process = mp.Process(target=actor_event_loop, args=(expr_projection, actor_conn))
        self.actor_process.start()

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        self.handle_conn.send(MicroPartition._from_pymicropartition(input))
        output: MicroPartition = self.handle_conn.recv()
        return output._micropartition

    def teardown(self) -> None:
        self.handle_conn.send(None)
        self.handle_conn.close()
        self.actor_process.join()
