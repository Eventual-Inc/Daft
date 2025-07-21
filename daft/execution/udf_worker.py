from __future__ import annotations

import pickle
import sys
from multiprocessing.connection import Client
from traceback import TracebackException

from daft.errors import UDFException
from daft.execution.udf import _ENTER, _ERROR, _SENTINEL, _SUCCESS, _UDF_ERROR, SharedMemoryTransport
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
    if name != _ENTER:
        raise ValueError(f"Expected '{_ENTER}' but got {name}")
    uninitialized_projection: ExpressionsProjection = pickle.loads(expr_projection_bytes)

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
            conn.send((_SUCCESS, out_name, out_size))
    except UDFException as e:
        exc = e.__cause__
        assert exc is not None
        conn.send((_UDF_ERROR, e.message, TracebackException.from_exception(exc), pickle.dumps(exc)))
    except Exception as e:
        try:
            conn.send((_ERROR, TracebackException.from_exception(e)))
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
