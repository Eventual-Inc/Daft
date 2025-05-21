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


def _should_run() -> bool:
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


def launch(noop_if_initialized: bool = False) -> None:
    """Launches the Daft dashboard server on port 3238.

    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards `http://localhost:3238` and view information regarding your queries.

    # Arguments:
        - noop_if_initialized: bool = False
            Will not raise an exception a Daft dashboard server process is already launched and running.
            Otherwise, an exception will be raised.
    """
    os.environ[native.DAFT_DASHBOARD_ENV_ENABLED] = "1"

    handle = native.launch(noop_if_initialized=noop_if_initialized)

    import atexit

    atexit.register(handle.shutdown, noop_if_shutdown=True)


def broadcast_query_information(
    unoptimized_plan: str,
    optimized_plan: str,
    mermaid_plan: str,
    plan_time_start: datetime,
    plan_time_end: datetime,
) -> None:
    headers = {
        "Content-Type": "application/json",
    }

    url = os.environ.get("DAFT_DASHBOARD_URL") if os.environ.get("DAFT_DASHBOARD_URL") else native.DAFT_DASHBOARD_URL

    queries_url = f"{url}/api/queries"

    data = json.dumps(
        {
            "id": str(uuid.uuid4()),
            "unoptimized_plan": unoptimized_plan,
            "optimized_plan": optimized_plan,
            "mermaid_plan": mermaid_plan,
            "plan_time_start": str(plan_time_start),
            "plan_time_end": str(plan_time_end),
            "logs": "",  # todo: implement logs
        }
    ).encode("utf-8")

    req = request.Request(queries_url, headers=headers, data=data)

    try:
        request.urlopen(req, timeout=1)
    except URLError as e:
        warnings.warn(f"Failed to broadcast metrics over {queries_url}: {e}")


__all__ = [
    "_should_run",
    "broadcast_query_information",
    "launch",
]
