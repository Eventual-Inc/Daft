from __future__ import annotations

import logging
import os
import sys
from multiprocessing.connection import Client
from pickle import PicklingError
from traceback import TracebackException

import daft.pickle
from daft.errors import UDFException
from daft.execution.udf import (
    _ENTER,
    _ERROR,
    _OUTPUT_DIVIDER,
    _READY,
    _SENTINEL,
    _SUCCESS,
    _UDF_ERROR,
    SharedMemoryTransport,
)
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

    transport = SharedMemoryTransport()
    try:
        conn.send(_READY)

        expression_projection = None
        while True:
            name, size = conn.recv()
            if (name, size) == _SENTINEL:
                break

            # We initialize after ready to avoid blocking the main thread
            if expression_projection is None:
                uninitialized_projection: ExpressionsProjection = daft.pickle.loads(expr_projection_bytes)
                initialized_projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])
                expression_projection = initialized_projection

            input_bytes = transport.read_and_release(name, size)
            input = MicroPartition.from_ipc_stream(input_bytes)
            evaluated = input.eval_expression_list(expression_projection)

            output_bytes = evaluated.to_ipc_stream()
            out_name, out_size = transport.write_and_close(output_bytes)

            print(_OUTPUT_DIVIDER.decode(), end="", file=sys.stderr, flush=True)
            sys.stdout.flush()
            sys.stderr.flush()
            conn.send((_SUCCESS, out_name, out_size))
    except UDFException as e:
        exc = e.__cause__
        assert exc is not None
        try:
            exc_bytes = daft.pickle.dumps(exc)
        except (PicklingError, AttributeError):
            exc_bytes = None
        conn.send((_UDF_ERROR, e.message, TracebackException.from_exception(exc), exc_bytes))
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

    logging.basicConfig(
        level=int(os.getenv("LOG_LEVEL", logging.WARNING)),
        format=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        datefmt=os.getenv("LOG_DATE_FORMAT", "%Y-%m-%d %H:%M:%S"),
    )

    udf_event_loop(secret, socket_path)
