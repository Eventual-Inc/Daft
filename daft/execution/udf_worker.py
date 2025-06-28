from __future__ import annotations

import sys
import traceback
from multiprocessing.connection import Client

import cloudpickle

from daft.execution.udf import _SENTINEL, SharedMemoryTransport
from daft.expressions.expressions import ExpressionsProjection
from daft.recordbatch.micropartition import MicroPartition


def udf_event_loop(
    secret: bytes,
    socket_path: str,
) -> None:
    # Initialize the client-side communication
    conn = Client(socket_path, authkey=secret)

    # Wait for the expression projection
    name, expr_projection_bytes = conn.recv()
    if name != "__ENTER__":
        raise ValueError(f"Expected '__ENTER__' but got {name}")
    uninitialized_projection: ExpressionsProjection = cloudpickle.loads(expr_projection_bytes)

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


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m daft.execution.udf_worker <socket_path> <secret>", file=sys.stderr)
        sys.exit(1)

    socket_path = sys.argv[1]
    secret = bytes.fromhex(sys.argv[2])

    udf_event_loop(secret, socket_path)
