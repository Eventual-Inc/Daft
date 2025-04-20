from __future__ import annotations

import logging
import multiprocessing as mp
from multiprocessing import shared_memory
from typing import TYPE_CHECKING

from daft.expressions import Expression, ExpressionsProjection
from daft.recordbatch import MicroPartition

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

    from daft.daft import PyExpr, PyMicroPartition

logger = logging.getLogger(__name__)

_SENTINEL = ("__EXIT__", 0)


def _write_to_shared_memory(data: bytes) -> tuple[str, int]:
    shm = shared_memory.SharedMemory(create=True, size=len(data))
    shm.buf[: len(data)] = data
    return shm.name, len(data)


def _read_from_shared_memory(name: str, size: int) -> bytes:
    shm = shared_memory.SharedMemory(name=name)
    data = bytes(shm.buf[:size])
    shm.close()
    shm.unlink()
    return data


def actor_event_loop(uninitialized_projection: ExpressionsProjection, conn: Connection) -> None:
    initialized_projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

    while True:
        name, size = conn.recv()
        if (name, size) == _SENTINEL:
            break

        input_bytes = _read_from_shared_memory(name, size)
        input = MicroPartition.from_ipc_stream(input_bytes)
        evaluated = input.eval_expression_list(initialized_projection)
        output_bytes = evaluated.to_ipc_stream()

        out_name, out_size = _write_to_shared_memory(output_bytes)
        conn.send((out_name, out_size))


class ActorHandle:
    """Handle class for initializing, interacting with, and tearing down a single local actor process."""

    def __init__(self, projection: list[PyExpr]) -> None:
        self.handle_conn, actor_conn = mp.Pipe(duplex=True)
        expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
        self.actor_process = mp.Process(target=actor_event_loop, args=(expr_projection, actor_conn), daemon=True)
        self.actor_process.start()

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        if not self.actor_process.is_alive():
            raise RuntimeError("Actor process is not alive")

        serialized = input.write_to_ipc_stream()
        shm_name, shm_size = _write_to_shared_memory(serialized)
        self.handle_conn.send((shm_name, shm_size))

        out_name, out_size = self.handle_conn.recv()
        output_bytes = _read_from_shared_memory(out_name, out_size)
        deserialized = MicroPartition.from_ipc_stream(output_bytes)
        return deserialized._micropartition

    def teardown(self) -> None:
        try:
            self.handle_conn.send(_SENTINEL)
        except (BrokenPipeError, EOFError):
            pass
        self.handle_conn.close()
        self.actor_process.join()
