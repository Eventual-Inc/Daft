from __future__ import annotations

import os
from pathlib import Path
from . import daft_dashboard as native
from importlib import resources


DAFT_DASHBOARD_ENV_NAME = "DAFT_DASHBOARD"
DAFT_DASHBOARD_QUERIES_URL = "http://localhost:3238/api/queries"
DAFT_DASHBOARD_URL_SHUTDOWN = "http://localhost:3238/api/shutdown"


def _static_assets_path() -> Path:
    path = Path(str(resources.files("daft_dashboard"))) / "static-dashboard-assets"

    if not path.exists():
        raise ImportError(
            "Unable to serve daft-dashboard's static assets because they couldn't be found"
            "Consider re-installing Daft with the 'dashboard' feature installed, e.g.:"
            'pip install "getdaft[dashboard]"'
        )

    return path


def launch(detach: bool = False):
    """Launches the Daft dashboard server on port 3238.

    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards `http://localhost:3238` and view information regarding your queries.
    """
    os.environ[DAFT_DASHBOARD_ENV_NAME] = "1"
    path = _static_assets_path()
    native.launch(static_assets_path=str(path), detach=detach)


def shutdown():
    """Sends a signal to the Daft dashboard server to shutdown.

    # Exceptions
    Will raise a runtime error if:
    - There is no Daft dashboard server running currently.
    - The Daft dashboard server responds with an error code after being requested to shutdown.
    """
    os.environ[DAFT_DASHBOARD_ENV_NAME] = "1"
    native.shutdown()


def cli():
    """Sends a signal to the Daft dashboard server to shutdown.

    # Exceptions
    Will raise a runtime error if the Daft dashboard server responds with an error code after being requested to shutdown.
    """
    import sys

    os.environ[DAFT_DASHBOARD_ENV_NAME] = "1"
    path = _static_assets_path()
    native.cli(sys.argv, static_assets_path=str(path))
