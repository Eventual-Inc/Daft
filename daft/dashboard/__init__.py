from __future__ import annotations

import warnings
from urllib import request
from urllib.error import URLError
import uuid
import json
import os
from pathlib import Path
from daft.daft import dashboard as native
from importlib import resources
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


def should_run() -> bool:
    enable_dashboard_str = os.environ.get(native.DAFT_DASHBOARD_ENV_ENABLED)
    if not enable_dashboard_str:
        return False
    try:
        enable_dashboard = int(enable_dashboard_str)
    except ValueError:
        return False
    if not enable_dashboard:
        return False

    return True


def launch(detach: bool = False, noop_if_initialized: bool = False):
    """Launches the Daft dashboard server on port 3238.

    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards `http://localhost:3238` and view information regarding your queries.

    # Arguments:
        - detach: bool = False
            Will detach the Daft dashboard server process from this current process.
            This make this API non-blocking; otherwise, this API is blocking.
        - noop_if_initialized: bool = False
            Will not throw an exception a Daft dashboard server process is already launched and running.
            Otherwise, an exception will be thrown.
    """
    os.environ[native.DAFT_DASHBOARD_ENV_ENABLED] = "1"
    native.launch(detach=detach, noop_if_initialized=noop_if_initialized)


def shutdown(noop_if_shutdown: bool = False):
    """Sends a signal to the Daft dashboard server to shutdown.

    # Arguments:
        - noop_if_shutdown: bool = False
            Will not throw an exception the Daft dashboard server process was already shut down.
            Otherwise, an exception will be thrown.

    # Exceptions
        Will raise a runtime error if the Daft dashboard server responds with an error code after being requested to shutdown.
    """
    os.environ[native.DAFT_DASHBOARD_ENV_ENABLED] = "1"
    native.shutdown(noop_if_shutdown=noop_if_shutdown)


def broadcast_query_information(
    mermaid_plan: str,
    plan_time_start: datetime,
    plan_time_end: datetime,
):
    headers = {
        "Content-Type": "application/json",
    }
    data = json.dumps(
        {
            "id": str(uuid.uuid4()),
            "mermaid_plan": mermaid_plan,
            "plan_time_start": str(plan_time_start),
            "plan_time_end": str(plan_time_end),
            "logs": "",  # todo: implement logs
        }
    ).encode("utf-8")

    req = request.Request(native.DAFT_DASHBOARD_QUERIES_URL, headers=headers, data=data)

    try:
        request.urlopen(req, timeout=1)
    except URLError as e:
        warnings.warn(f"Failed to broadcast metrics over {native.DAFT_DASHBOARD_QUERIES_URL}: {e}")


def _cli():
    """Runs the Daft dashboard CLI."""
    os.environ[native.DAFT_DASHBOARD_ENV_ENABLED] = "1"
    native.cli(sys.argv)


__all__ = [
    "_cli",
    "broadcast_query_information",
    "launch",
    "should_run",
    "shutdown",
]
