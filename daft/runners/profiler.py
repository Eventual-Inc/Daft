from __future__ import annotations

import logging
import os
import signal
import subprocess
from collections.abc import Generator
from contextlib import _GeneratorContextManager, contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

    from viztracer import VizTracer

ACTIVE = False


logger = logging.getLogger(__name__)


@contextmanager
def profiler(filename: str) -> Generator[VizTracer | None, Any, None]:
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
                "profiler(%s) not created. Another profiler(%s) is already active.",
                filename,
                tracer.output_file,
            )

    yield None
    return


import time


@contextmanager
def timingcontext(name: str) -> Generator[None, Any, None]:
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


def log_event(name: str) -> _GeneratorContextManager[None]:
    return timingcontext(name)


ENABLE_SAMPLY_PROFILING = os.getenv("DAFT_DEV_ENABLE_SAMPLY_PROFILING", "false").lower() == "true"


@contextmanager
def profile() -> Generator[None, None, None]:
    """Context manager to profile the current process with samply."""
    if not ENABLE_SAMPLY_PROFILING:
        yield
        return

    current_pid = os.getpid()

    # Run the profiler from a bash script to avoid issues with the profiler process being unable to receive signals.
    # https://github.com/ray-project/ray/issues/31805
    cmd = [
        "bash",
        "-c",
        f"""
        samply record --pid {current_pid} -s -o /tmp/ray/session_latest/logs/daft/samply_{current_pid}_{time.time()}.json.gz
        """,
    ]

    try:
        logger.debug("Starting samply profiler...")
        # Start the profiler process
        profiler_process = subprocess.Popen(cmd)

        # Yield control back to the context
        yield

    finally:
        # Stop profiling by sending SIGINT (Ctrl+C) to samply
        if profiler_process.poll() is None:  # Process is still running
            logger.debug("Stopping samply profiler...")
            profiler_process.send_signal(signal.SIGINT)
            logger.debug("Waiting for samply profiler to finish...")
            profiler_process.wait()
            logger.debug("Samply profiler finished")
