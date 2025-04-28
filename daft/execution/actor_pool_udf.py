from __future__ import annotations

import logging
import multiprocessing as mp
import traceback
from multiprocessing import resource_tracker, shared_memory
from typing import TYPE_CHECKING

from daft.expressions import Expression, ExpressionsProjection
from daft.recordbatch import MicroPartition

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

    from daft.daft import PyExpr, PyMicroPartition

logger = logging.getLogger(__name__)

_SENTINEL = ("__EXIT__", 0)


class SharedMemoryTransport:
    def write_and_close(self, data: bytes) -> tuple[str, int]:
        shm = shared_memory.SharedMemory(create=True, size=len(data))
        # DO NOT REMOVE OR CHANGE THIS LINE. It is necessary to prevent the resource tracker from tracking the shared memory object.
        # This is because we are creating and unlinking the shared memory object in different processes.
        resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore[attr-defined]
        shm.buf[: len(data)] = data
        shm.close()
        return shm.name, len(data)

    def read_and_release(self, name: str, size: int) -> bytes:
        shm = shared_memory.SharedMemory(name=name)
        try:
            data = bytes(shm.buf[:size])
        finally:
            shm.close()
            try:
                shm.unlink()
            except FileNotFoundError:
                logger.warning("Shared memory %s already unlinked", name)
        return data


def actor_event_loop(uninitialized_projection: ExpressionsProjection, conn: Connection) -> None:
    transport = SharedMemoryTransport()
    try:
        initialized_projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

        while True:
            name, size = conn.recv()
            if (name, size) == _SENTINEL:
                break

            input_bytes = transport.read_and_release(name, size)
            input = MicroPartition.from_ipc_stream(input_bytes)
            evaluated = input.eval_expression_list(initialized_projection)
            output_bytes = evaluated.to_ipc_stream()

            out_name, out_size = transport.write_and_close(output_bytes)
            conn.send(("success", out_name, out_size))
    except Exception as e:
        try:
            conn.send(("error", type(e).__name__, traceback.format_exc()))
        except Exception:
            # If the connection is broken, it's because the parent process has died.
            # We can just exit here.
            pass
    finally:
        conn.close()


class ActorHandle:
    def __init__(self, projection: list[PyExpr]) -> None:
        self.handle_conn, actor_conn = mp.Pipe(duplex=True)
        expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
        self.actor_process = mp.Process(target=actor_event_loop, args=(expr_projection, actor_conn), daemon=True)
        self.actor_process.start()
        self.transport = SharedMemoryTransport()

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        if not self.actor_process.is_alive():
            raise RuntimeError("Actor process is not alive")

        serialized = input.write_to_ipc_stream()
        shm_name, shm_size = self.transport.write_and_close(serialized)
        self.handle_conn.send((shm_name, shm_size))

        response = self.handle_conn.recv()
        if response[0] == "error":
            error_type, tb_str = response[1], response[2]
            exception_class = getattr(__builtins__, error_type, RuntimeError)
            raise exception_class(tb_str)
        elif response[0] == "success":
            out_name, out_size = response[1], response[2]
            output_bytes = self.transport.read_and_release(out_name, out_size)
            deserialized = MicroPartition.from_ipc_stream(output_bytes)
            return deserialized._micropartition
        else:
            raise RuntimeError(f"Unknown response from actor: {response}")

    def teardown(self, timeout: float = 5.0) -> None:
        try:
            self.handle_conn.send(_SENTINEL)
        except (BrokenPipeError, EOFError):
            # If the connection is broken, just exit and join the actor process.
            pass
        self.handle_conn.close()

        self.actor_process.join(timeout)
        if self.actor_process.is_alive():
            logger.warning("Actor did not shut down in time; terminating...")
            self.actor_process.terminate()
            self.actor_process.join()
