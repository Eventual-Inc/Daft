import contextlib
import os
from contextlib import contextmanager

from loguru import logger
from viztracer import VizTracer, get_tracer


def profiler(filename: str) -> VizTracer:
    if os.environ.get("DAFT_PROFILING", 0) == 1:
        return VizTracer(output_file=filename)
    else:
        return contextlib.nullcontext()


import time


@contextmanager
def timingcontext(name: str):
    tracer = get_tracer()

    try:
        start = time.time()
        if tracer is not None:
            with tracer.log_event(name) as event:
                yield event
        else:
            yield None
    finally:
        end = time.time()

        logger.debug(f"log_event:{name}:{(end-start)*1000:.3f}ms")


def log_event(name: str):
    return timingcontext(name)
