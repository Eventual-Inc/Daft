from __future__ import annotations

import logging
import os
import secrets
import subprocess
import sys
import tempfile
from multiprocessing import resource_tracker, shared_memory
from multiprocessing.connection import Listener
from typing import IO, cast

import daft.pickle
from daft import Expression
from daft.daft import OperatorMetrics, PyExpr, PyRecordBatch
from daft.errors import UDFException
from daft.expressions import ExpressionsProjection

logger = logging.getLogger(__name__)

_INIT = "__INIT__"
_EVAL = "__EVAL__"
_READY = "ready"
_SUCCESS = "success"
_UDF_ERROR = "udf_error"
_CLEANUP_UDF = "__CLEANUP_UDF__"
_ERROR = "error"
_OUTPUT_DIVIDER = b"_DAFT_OUTPUT_DIVIDER_\n"
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


class UdfWorkerHandle:
    """Handle for a udf worker process in the global UDF process pool.

    This worker can cache and execute multiple UDFs, with state persisted
    across invocations.
    """

    def __init__(self) -> None:
        """Start a new udf worker process."""
        # Construct UNIX socket path for basic communication
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            self.socket_path = tmp.name
        secret = secrets.token_bytes(32)
        self.listener = Listener(self.socket_path, authkey=secret)

        # Copy the current process environment
        env = dict(os.environ)

        # Copy the logging configuration of the current process
        root = logging.getLogger()
        env["LOG_LEVEL"] = str(root.level)
        for h in root.handlers:
            if hasattr(h, "formatter") and h.formatter is not None:
                if h.formatter._fmt:
                    env["LOG_FORMAT"] = h.formatter._fmt
                if h.formatter.datefmt:
                    env["LOG_DATE_FORMAT"] = h.formatter.datefmt
                break

        # Python auto-buffers stdout by default, so disable
        env["PYTHONUNBUFFERED"] = "1"

        # Start the worker process
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "daft.execution.udf_worker",
                self.socket_path,
                secret.hex(),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
        )

        # Initialize communication
        self.handle_conn = self.listener.accept()
        self.transport = SharedMemoryTransport()

        # Wait for ready signal
        response = self.handle_conn.recv()
        if response != _READY:
            raise RuntimeError(f"Expected '{_READY}' but got {response}")

    def is_alive(self) -> bool:
        """Check if the worker process is still running."""
        return self.process.poll() is None

    def trace_output(self) -> list[str]:
        """Read any pending stdout from the worker process."""
        lines = []
        while True:
            line = cast("IO[bytes]", self.process.stdout).readline()
            if line == b"" or line == _OUTPUT_DIVIDER or not self.is_alive():
                break
            lines.append(line.decode().rstrip())
        return lines

    def init_udf(self, udf_name: str, py_expr: PyExpr) -> None:
        """Initialize a UDF on this worker.

        Args:
            udf_name: Unique identifier for the UDF
            py_expr: PyExpr object that will be pickled
        """
        if not self.is_alive():
            raise RuntimeError("UDF worker process has terminated")

        expression = Expression._from_pyexpr(py_expr)
        expr_projection = ExpressionsProjection([expression])

        # Serialize the projection
        expr_bytes = daft.pickle.dumps(expr_projection)

        # Send INIT message
        self.handle_conn.send((_INIT, udf_name, expr_bytes))

        try:
            response = self.handle_conn.recv()
        except EOFError:
            stdout = self.trace_output()
            raise RuntimeError(f"UDF worker closed connection unexpectedly, stdout: {stdout}")

        if response != _SUCCESS:
            raise RuntimeError(f"UDF initialization failed: {response}")

    def eval_input(
        self,
        udf_name: str,
        input: PyRecordBatch,
    ) -> tuple[PyRecordBatch, list[str], OperatorMetrics]:
        """Execute a UDF on the given input.

        Args:
            udf_name: Unique identifier for the UDF (must be initialized via init_udf first)
            input: Input data to process

        Returns:
            Tuple of (output data, stdout lines, metrics)
        """
        if not self.is_alive():
            raise RuntimeError("UDF worker process has terminated")

        # Serialize input data to shared memory
        serialized = input.to_ipc_stream()
        shm_name, shm_size = self.transport.write_and_close(serialized)

        # Send EVAL message with UDF name and data location
        self.handle_conn.send((_EVAL, udf_name, shm_name, shm_size))

        try:
            response = self.handle_conn.recv()
        except EOFError:
            stdout = self.trace_output()
            raise RuntimeError(f"UDF worker closed connection unexpectedly, stdout: {stdout}")

        stdout = self.trace_output()

        if response[0] == _UDF_ERROR:
            try:
                base_exc: Exception | None = daft.pickle.loads(response[3])
            except Exception:
                base_exc = None

            tb_payload = response[2]
            if isinstance(tb_payload, bytes):
                try:
                    tb_info = daft.pickle.loads(tb_payload)
                except Exception:
                    tb_info = None
            else:
                tb_info = tb_payload

            if base_exc is None and sys.version_info >= (3, 11):
                raise UDFException(response[1], tb_info)
            if base_exc and tb_info and sys.version_info >= (3, 11):
                base_exc.add_note("\n".join(tb_info.format()).rstrip())  # type: ignore[attr-defined]
            raise UDFException(response[1], tb_info if base_exc is None else None) from base_exc
        elif response[0] == _ERROR:
            raise RuntimeError("UDF unexpectedly failed with traceback:\n" + response[1])
        elif response[0] == _SUCCESS:
            _, out_name, out_size, metrics = response
            output_bytes = self.transport.read_and_release(out_name, out_size)
            deserialized = PyRecordBatch.from_ipc_stream(output_bytes)
            return (deserialized, stdout, metrics)
        else:
            raise RuntimeError(f"Unknown response from UDF worker: {response}")

    def cleanup_udf(self, udf_name: str) -> None:
        """Clean up a UDF on this worker, removing it's state from the worker's cache.

        Args:
            udf_name: Unique identifier for the UDF
        """
        if not self.is_alive():
            raise RuntimeError("UDF worker process has terminated. Cannot cleanup UDF.")

        try:
            self.handle_conn.send((_CLEANUP_UDF, udf_name))
            response = self.handle_conn.recv()

            if response == _SUCCESS:
                return
            else:
                logger.warning("Unexpected response from cleanup_udf: %s", response)
        except Exception as e:
            logger.warning("Error cleaning up UDF: %s", e)

    def shutdown(self, timeout: float = 5.0) -> None:
        """Gracefully shut down the UDF worker process."""
        try:
            self.handle_conn.send(_SENTINEL)
        except (BrokenPipeError, EOFError):
            pass
        self.handle_conn.close()
        self.listener.close()

        self.process.wait(timeout)
        if not self.is_alive():
            logger.warning("UDF worker did not shut down in time; terminating...")
            self.process.terminate()
            self.process.wait()

        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
