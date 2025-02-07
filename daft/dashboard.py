import os

import daft.daft as native

DAFT_DASHBOARD_ENV_NAME = "DAFT_DASHBOARD"
DAFT_DASHBOARD_ADDR = "http://localhost:3238"


def launch():
    """Launches the Daft dashboard server on port 3000.

    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards `http://localhost:3000` and view information regarding your queries.
    """
    os.environ[DAFT_DASHBOARD_ENV_NAME] = "1"
    native.launch()
