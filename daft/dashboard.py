from __future__ import annotations

import os
import sys
import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

from daft.context import get_context


def _dashboard_module():
    try:
        import daft_dashboard
    except ImportError:
        warnings.warn(
            "Unable to import Daft's dashboard features"
            "Consider re-installing Daft with the 'dashboard' feature installed, e.g.:"
            'pip install "getdaft[dashboard]"'
        )
        raise

    if not daft_dashboard.__file__:
        raise ImportError

    return daft_dashboard


def should_run() -> bool:
    try:
        dashboard = _dashboard_module()
    except ImportError:
        return False

    enable_dashboard_str = os.environ.get(dashboard.DAFT_DASHBOARD_ENV_NAME)

    if not enable_dashboard_str:
        return False
    try:
        enable_dashboard = int(enable_dashboard_str)
    except ValueError:
        return False
    if not enable_dashboard:
        return False

    return True


class NativeLogBroadcaster:
    def __init__(self):
        self.original_stdout = sys.stdout
        self.buffer = ""

    def write(self, text: str):
        self.original_stdout.write(text)
        self.buffer += text

    def flush(self):
        self.original_stdout.flush()

    def close(self):
        self.original_stdout.close()


def broadcast_query_information(
    mermaid_plan: str,
    plan_time_start: datetime,
    plan_time_end: datetime,
):
    dashboard = _dashboard_module()
    dashboard.broadcast_query_information(
        mermaid_plan=mermaid_plan,
        plan_time_start=plan_time_start,
        plan_time_end=plan_time_end,
        logs=_get_logs(),
    )


log_capturer = None


def _get_logs():
    return log_capturer.buffer if log_capturer else ""


try:
    dashboard = _dashboard_module()
    launch = dashboard.launch
    shutdown = dashboard.shutdown

    runner = get_context().get_or_create_runner()

    if runner.name == "ray":
        pass
    elif runner.name == "py":
        pass
    elif runner.name == "native":
        log_capturer = NativeLogBroadcaster()

    if log_capturer:
        sys.stdout = log_capturer

    # re-export some symbols defined inside of `daft_dashboard`
    __all__ = [
        "launch",
        "shutdown",
    ]
except Exception:
    pass
