from __future__ import annotations

import os
from pathlib import Path
from daft_dashboard import daft_dashboard as native
from importlib import resources


DAFT_DASHBOARD_ENV_NAME = "DAFT_DASHBOARD"
DAFT_DASHBOARD_URL = "http://localhost:3238/api/queries"


def launch(block: bool = False):
    """Launches the Daft dashboard server on port 3238.

    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards `http://localhost:3238` and view information regarding your queries.
    """
    path = Path(str(resources.files("daft_dashboard"))) / "static-dashboard-assets"

    if not path.exists():
        raise ImportError(
            "Unable to serve daft-dashboard's static assets because they couldn't be found"
            "Consider re-installing Daft with the 'dashboard' feature installed, e.g.:"
            'pip install "getdaft[dashboard]"'
        )

    os.environ[DAFT_DASHBOARD_ENV_NAME] = "1"
    native.launch_dashboard(static_assets_path=str(path), block=block)


__all__ = [
    "launch",
]
