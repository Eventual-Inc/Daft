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
    plan_time_start: datetime,
    plan_time_end: datetime,
) -> None:
    import ssl

    headers = {
        "Content-Type": "application/json",
    }

    if (url := os.environ.get("DAFT_DASHBOARD_URL")) is not None:
        url = f"{url}/queries"
    else:
        url = native.DAFT_DASHBOARD_QUERIES_URL

    if (auth_token := os.environ.get("DAFT_DASHBOARD_AUTH_TOKEN")) is not None:
        headers["Authorization"] = f"Bearer {auth_token}"

    data = json.dumps(
        {
            "id": str(uuid.uuid4()),
            "optimized_plan": optimized_plan,
            "run_id": os.environ.get("DAFT_DASHBOARD_RUN_ID", None),
            "logs": "",  # todo: implement logs
        }
    ).encode("utf-8")

    ctx = ssl.create_default_context()

    # if it's a localhost uri we can skip ssl verification
    if "localhost" in url:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    req = request.Request(url, headers=headers, data=data)

    try:
        request.urlopen(req, timeout=1, context=ctx)
    except URLError as e:
        warnings.warn(f"Failed to broadcast metrics over {url}: {e}")


__all__ = [
    "_should_run",
    "broadcast_query_information",
    "launch",
]
