from __future__ import annotations

import os
from contextlib import contextmanager
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from viztracer import VizTracer

ACTIVE = False


@contextmanager
def profiler(filename: str) -> VizTracer:
    if int(os.environ.get("DAFT_PROFILING", 0)) == 1:
        from viztracer import VizTracer, get_tracer

        global ACTIVE
        if not ACTIVE:
            tracer = get_tracer()
            if tracer is None or tracer.output_file != filename:
                tracer = VizTracer(output_file=filename, tracer_entries=10_000_000)
            ACTIVE = True
            with tracer:
                yield tracer
            ACTIVE = False
            return
        else:
            tracer = get_tracer()
            logger.warning(
                f"profiler({filename}) not created. Another profiler({tracer.output_file}) is already active."
            )

    yield None
    return


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
