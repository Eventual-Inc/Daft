from __future__ import annotations

import logging
import os
import pickle
import secrets
import subprocess
import sys
import tempfile
from multiprocessing import resource_tracker, shared_memory
from multiprocessing.connection import Listener
from typing import TYPE_CHECKING

from daft.errors import UDFException
from daft.expressions import Expression, ExpressionsProjection
from daft.recordbatch import MicroPartition

if TYPE_CHECKING:
    from daft.daft import PyExpr, PyMicroPartition

logger = logging.getLogger(__name__)

_ENTER = "__ENTER__"
_SUCCESS = "success"
_UDF_ERROR = "udf_error"
_ERROR = "error"
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


class UdfHandle:
    def __init__(self, project_expr: PyExpr, passthrough_exprs: list[PyExpr]) -> None:
        # Construct UNIX socket path for basic communication
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            self.socket_path = tmp.name
        secret = secrets.token_bytes(32)
        self.listener = Listener(self.socket_path, authkey=secret)

        # Start the worker process
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "daft.execution.udf_worker",
                self.socket_path,
                secret.hex(),
            ]
        )

        # Initialize communication
        self.handle_conn = self.listener.accept()
        self.transport = SharedMemoryTransport()

        # Serialize and send the expression projection
        expr_projection = ExpressionsProjection(
            [Expression._from_pyexpr(expr) for expr in passthrough_exprs] + [Expression._from_pyexpr(project_expr)]
        )
        expr_projection_bytes = pickle.dumps(expr_projection)
        self.handle_conn.send((_ENTER, expr_projection_bytes))

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        if self.process.poll() is not None:
            raise RuntimeError("UDF process has terminated")

        serialized = input.write_to_ipc_stream()
        shm_name, shm_size = self.transport.write_and_close(serialized)
        self.handle_conn.send((shm_name, shm_size))

        response = self.handle_conn.recv()
        if response[0] == _UDF_ERROR:
            base_exc: Exception = pickle.loads(response[3])
            if sys.version_info >= (3, 11):
                base_exc.add_note("\n".join(response[2].format()))
            raise UDFException(response[1]) from base_exc
        elif response[0] == _ERROR:
            raise RuntimeError("Actor Pool UDF unexpectedly failed with traceback:\n" + "\n".join(response[1].format()))
        elif response[0] == _SUCCESS:
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
            # If the connection is broken, just exit and join the process.
            pass
        self.handle_conn.close()
        self.listener.close()

        self.process.wait(timeout)
        if self.process.poll() is None:
            logger.warning("UDF did not shut down in time; terminating...")
            self.process.terminate()
            self.process.wait()

        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
