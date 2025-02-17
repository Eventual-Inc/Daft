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

DAFT_DASHBOARD_ENV_NAME = "DAFT_DASHBOARD"
DAFT_DASHBOARD_URL = "http://localhost:3238/api/queries"


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
    launch(block=False)

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
    req = request.Request(DAFT_DASHBOARD_URL, headers=headers, data=data)

    try:
        request.urlopen(req, timeout=1)
    except URLError as e:
        warnings.warn(f"Failed to broadcast metrics over {DAFT_DASHBOARD_URL}: {e}")


def launch(block: bool = False):
    """Launches the Daft dashboard server on port 3238.

    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards `http://localhost:3238` and view information regarding your queries.
    """
    try:
        from daft_dashboard import launch

        launch(block=block)
    except ImportError:
        warnings.warn(
            "Unable to import Daft's dashboard features"
            "Consider re-installing Daft with the 'dashboard' feature installed, e.g.:"
            'pip install "getdaft[dashboard]"'
        )
        raise
