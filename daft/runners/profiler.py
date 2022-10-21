from __future__ import annotations

import contextlib
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from viztracer import VizTracer


def profiler(filename: str) -> VizTracer:
    if int(os.environ.get("DAFT_PROFILING", 0)) == 1:
        from viztracer import VizTracer

        return VizTracer(output_file=filename, tracer_entries=10_000_000)
    else:
        return contextlib.nullcontext()


import time


@contextmanager
def timingcontext(name: str):
    from viztracer import get_tracer

    tracer = get_tracer()

    try:
        logger.debug(f"log_event:enter:{name}")
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
