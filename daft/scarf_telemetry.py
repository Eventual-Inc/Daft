import os
import platform
import urllib.parse
import urllib.request

from daft import get_build_type, get_version


def scarf_telemetry(runner: str):
    # Track analytics for Daft usage via Scarf.

    version = get_version()
    build_type = get_build_type()
    scarf_opt_out = os.getenv("SCARF_NO_ANALYTICS") == "true" or os.getenv("DO_NOT_TRACK") == "true"
    daft_analytics_enabled = os.getenv("DAFT_ANALYTICS_ENABLED") != "0"

    # Skip analytics for dev builds or if user opted out
    if build_type == "dev" or scarf_opt_out or daft_analytics_enabled:
        return None, None

    try:
        python_version = ".".join(platform.python_version().split(".")[:2])

        params = {
            "version": version,
            "platform": platform.system(),
            "python": python_version,
            "arch": platform.machine(),
            "runner": runner,
        }

        # Prepare the query string
        query_string = urllib.parse.urlencode(params)

        # Make the GET request
        url = f"https://daft.gateway.scarf.sh/daft-runner?{query_string}"
        with urllib.request.urlopen(url) as response:
            return f"Response status: {response.status}", runner

    except Exception as e:
        return f"Analytics error: {e!s}", None
