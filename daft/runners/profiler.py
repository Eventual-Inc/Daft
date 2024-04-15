from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from viztracer import VizTracer

ACTIVE = False


logger = logging.getLogger(__name__)


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
                "profiler(%s) not created. Another profiler(%s) is already active.", filename, tracer.output_file
            )

    yield None
    return


import time


@contextmanager
def timingcontext(name: str):
    from viztracer import get_tracer

    tracer = get_tracer()

    try:
        logger.debug("log_event:enter:%s", name)
        start = time.time()
        if tracer is not None:
            with tracer.log_event(name) as event:
                yield event
        else:
            yield None
    finally:
        end = time.time()

        logger.debug("log_event:%s:%sms", name, f"{(end-start)*1000:.3f}")


def log_event(name: str):
    return timingcontext(name)
