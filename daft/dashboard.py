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


def _broadcast_query_plan(
    mermaid_plan: str,
    plan_time_start: datetime,
    plan_time_end: datetime,
):
    dashboard = dashboard_module()

    # try launching the dashboard
    # if dashboard is already launched, this will do nothing
    dashboard.launch(detach=True, noop_if_initialized=True)

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
    req = request.Request(dashboard.DAFT_DASHBOARD_QUERIES_URL, headers=headers, data=data)

    try:
        request.urlopen(req, timeout=1)
    except URLError as e:
        warnings.warn(f"Failed to broadcast metrics over {dashboard.DAFT_DASHBOARD_QUERIES_URL}: {e}")


try:
    dashboard = dashboard_module()
    from dashboard import cli, launch, shutdown

    # re-export all of the symbols defined inside of `daft_dashboard`
    __all__ = [
        "cli",
        "launch",
        "shutdown",
    ]
except Exception:
    ...
