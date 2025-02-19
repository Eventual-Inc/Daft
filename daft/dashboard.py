from __future__ import annotations

import json
import os
import uuid
import warnings
from typing import TYPE_CHECKING
from urllib import request
from urllib.error import URLError

if TYPE_CHECKING:
    from datetime import datetime

try:
    # re-export all of the symbols defined inside of `daft_dashboard`
    # from daft_dashboard import *
    from daft_dashboard import DAFT_DASHBOARD_ENV_NAME, DAFT_DASHBOARD_QUERIES_URL, cli, launch, shutdown
except ImportError:
    warnings.warn(
        "Unable to import Daft's dashboard features"
        "Consider re-installing Daft with the 'dashboard' feature installed, e.g.:"
        'pip install "getdaft[dashboard]"'
    )
    raise


def _should_run() -> bool:
    enable_dashboard_str = os.environ.get(DAFT_DASHBOARD_ENV_NAME)

    if not enable_dashboard_str:
        return False
    try:
        enable_dashboard = int(enable_dashboard_str)
    except ValueError:
        return False
    if not enable_dashboard:
        return False

    return True


def _broadcast_query_plan(
    mermaid_plan: str,
    plan_time_start: datetime,
    plan_time_end: datetime,
):
    # try launching the dashboard
    # if dashboard is already launched, this will do nothing
    launch(detach=True)

    headers = {
        "Content-Type": "application/json",
    }
    data = json.dumps(
        {
            "id": str(uuid.uuid4()),
            "mermaid_plan": mermaid_plan,
            "plan_time_start": str(plan_time_start),
            "plan_time_end": str(plan_time_end),
        }
    ).encode("utf-8")
    req = request.Request(DAFT_DASHBOARD_QUERIES_URL, headers=headers, data=data)

    try:
        request.urlopen(req, timeout=1)
    except URLError as e:
        warnings.warn(f"Failed to broadcast metrics over {DAFT_DASHBOARD_QUERIES_URL}: {e}")


__all__ = [
    "cli",
    "launch",
    "shutdown",
]
