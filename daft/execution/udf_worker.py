from __future__ import annotations

import logging
import os
import sys
from multiprocessing.connection import Client
from pickle import PicklingError
from traceback import TracebackException

import daft.pickle
from daft.daft import set_compute_runtime_num_worker_threads
from daft.errors import UDFException
from daft.execution.udf import (
    _CLEANUP_UDFS,
    _ERROR,
    _EVAL,
    _INIT,
    _OUTPUT_DIVIDER,
    _READY,
    _SENTINEL,
    _SUCCESS,
    _UDF_ERROR,
    SharedMemoryTransport,
)
from daft.expressions.expressions import ExpressionsProjection
from daft.recordbatch import RecordBatch


def udf_worker_event_loop(
    secret: bytes,
    socket_path: str,
) -> None:
    """UDF worker event loop.

    This worker can cache and execute multiple UDFs. It exits when its cache
    becomes empty after a cleanup operation.
    """
    # Initialize the client-side communication
    conn = Client(socket_path, authkey=secret)
    transport = SharedMemoryTransport()

    # Set the compute runtime num worker threads to 1 for the UDF worker
    set_compute_runtime_num_worker_threads(1)

    # Cache of initialized UDF projections: udf_name -> ExpressionsProjection
    udf_cache: dict[str, ExpressionsProjection] = {}

    try:
        conn.send(_READY)

        while True:
            msg = conn.recv()
            if msg == _SENTINEL:
                break

            msg_type = msg[0]
            if msg_type == _INIT:
                # INIT message: (msg_type, udf_name, expr_bytes)
                _, udf_name, expr_bytes = msg

                # Initialize UDF if not already in cache
                if udf_name not in udf_cache:
                    uninitialized_projection: ExpressionsProjection = daft.pickle.loads(expr_bytes)
                    udf_cache[udf_name] = ExpressionsProjection(
                        [e._initialize_udfs() for e in uninitialized_projection]
                    )
                    conn.send(_SUCCESS)
                else:
                    conn.send(_SUCCESS)

            elif msg_type == _EVAL:
                # EVAL message: (msg_type, udf_name, shm_name, shm_size)
                _, udf_name, shm_name, shm_size = msg

                # UDF must already be in cache (initialized via INIT)
                if udf_name not in udf_cache:
                    conn.send((_ERROR, f"UDF '{udf_name}' not in cache. Call INIT first."))
                    continue

                # Read input from shared memory
                input_bytes = transport.read_and_release(shm_name, shm_size)
                input_batch = RecordBatch.from_ipc_stream(input_bytes)

                # Evaluate the UDF
                projection = udf_cache[udf_name]
                evaluated, metrics = input_batch.eval_expression_list_with_metrics(projection)

                # Write output to shared memory
                output_bytes = evaluated.to_ipc_stream()
                out_name, out_size = transport.write_and_close(output_bytes)

                # Mark end of UDF's stdout and flush
                print(_OUTPUT_DIVIDER.decode(), end="", file=sys.stderr, flush=True)
                sys.stdout.flush()
                sys.stderr.flush()

                conn.send((_SUCCESS, out_name, out_size, metrics))

            elif msg_type == _CLEANUP_UDFS:
                # CLEANUP_UDFS message: (msg_type, udf_names)
                _, udf_names = msg

                # Remove UDF from cache
                for udf_name in udf_names:
                    if udf_name in udf_cache:
                        del udf_cache[udf_name]

                conn.send(_SUCCESS)
            else:
                conn.send((_ERROR, f"Unknown message type: {msg_type}"))

    except UDFException as e:
        exc = e.__cause__
        assert exc is not None
        try:
            exc_bytes = daft.pickle.dumps(exc)
        except (PicklingError, AttributeError, TypeError):
            exc_bytes = None
        try:
            tb_bytes = daft.pickle.dumps(TracebackException.from_exception(exc))
        except (PicklingError, AttributeError, TypeError):
            tb_bytes = None
        conn.send((_UDF_ERROR, e.message, tb_bytes, exc_bytes))
    except Exception as e:
        try:
            tb = "\n".join(TracebackException.from_exception(e).format())
        except Exception:
            # If serialization fails, just send the exception's repr
            # This sometimes happens on 3.10, but unclear why
            # The repr doesn't contain the full traceback
            tb = repr(e)

        try:
            conn.send((_ERROR, tb))
        except Exception:
            # If the connection is broken, it's because the parent process has died.
            # We can just exit here.
            pass
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python -m daft.execution.udf_worker <socket_path> <secret>",
            file=sys.stderr,
        )
        sys.exit(1)

    socket_path = sys.argv[1]
    secret = bytes.fromhex(sys.argv[2])

    logging.basicConfig(
        level=int(os.getenv("LOG_LEVEL", logging.WARNING)),
        format=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        datefmt=os.getenv("LOG_DATE_FORMAT", "%Y-%m-%d %H:%M:%S"),
    )

    udf_worker_event_loop(secret, socket_path)
