from __future__ import annotations

import json
import os
import uuid
import warnings
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from urllib import request
from urllib.error import URLError

import daft.daft as native
from daft import DataFrame
from daft.dataframe.display import MermaidFormatter

DAFT_DASHBOARD_ENV_NAME = "DAFT_DASHBOARD"
DAFT_DASHBOARD_URL = "http://localhost:3238/api/queries"


def _broadcast_query_plan(df: DataFrame):
    enable_dashboard_str = os.environ.get(DAFT_DASHBOARD_ENV_NAME)

    if not enable_dashboard_str:
        return
    try:
        enable_dashboard = int(enable_dashboard_str)
    except ValueError:
        return
    if not enable_dashboard:
        return

    # try launching the dashboard
    # if dashboard is already launched, this will do nothing
    launch()

    is_cached = df._result_cache is not None
    plan_time_start = datetime.now(timezone.utc)
    mermaid_plan = MermaidFormatter(
        builder=df._builder, show_all=True, simple=False, is_cached=is_cached
    )._repr_markdown_()
    plan_time_end = datetime.now(timezone.utc)

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
    path = Path(str(resources.files("daft"))) / "static_dashboard_assets"

    if not path.exists():
        raise ImportError(
            "Unable to import Daft's dashboard features"
            "Consider re-installing Daft with the 'dashboard' feature installed, e.g.:"
            'pip install "getdaft[dashboard]"'
        )

    os.environ[DAFT_DASHBOARD_ENV_NAME] = "1"
    native.launch_dashboard(static_assets_path=str(path), block=block)
