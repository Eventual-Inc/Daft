import os
from importlib import resources
from pathlib import Path

import daft.daft as native

DAFT_DASHBOARD_ENV_NAME = "DAFT_DASHBOARD"
DAFT_DASHBOARD_ADDR = "http://localhost:3238/broadcast"


def launch():
    """Launches the Daft dashboard server on port 3000.

    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards `http://localhost:3000` and view information regarding your queries.
    """
    path = Path(str(resources.files("daft"))) / "static_dashboard_assets"
    os.environ[DAFT_DASHBOARD_ENV_NAME] = "1"
    native.launch(str(path))
