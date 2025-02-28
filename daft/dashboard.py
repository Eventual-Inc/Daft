"""The Daft Dashboard module.

The primary functionality of this module is just to load the `daft_dashboard` python library and re-export some of the APIs that it exposes.
"""

from __future__ import annotations

import os
import sys
import warnings


def dashboard_module():
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


def _should_run() -> bool:
    try:
        dashboard = dashboard_module()
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


try:
    dashboard = dashboard_module()
    broadcast_query_information = dashboard.broadcast_query_information
    launch = dashboard.launch
    shutdown = dashboard.shutdown

    nlb = NativeLogBroadcaster()
    sys.stdout = nlb

    # re-export some symbols defined inside of `daft_dashboard`
    __all__ = [
        "broadcast_query_information",
        "launch",
        "nlb",
        "shutdown",
    ]
except Exception:
    pass
