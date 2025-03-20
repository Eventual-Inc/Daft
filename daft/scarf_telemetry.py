import os
import platform
import urllib.parse
import urllib.request
from typing import Optional


def _track_on_scarf(endpoint: str, extra_params: Optional[dict] = None):
    """Common implementation for Scarf telemetry tracking.

    Args:
        endpoint: The Scarf endpoint to use (e.g., "daft-runner" or "daft-import")
        extra_params: Optional additional parameters to include in the request

    Returns:
        A tuple of (response_message, extra_value) or (None, None) if telemetry is disabled
    """
    from daft import get_build_type, get_version

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
        }

        # Add any extra parameters
        if extra_params:
            params.update(extra_params)

        # Prepare the query string
        query_string = urllib.parse.urlencode(params)

        # Make the GET request
        url = f"https://daft.gateway.scarf.sh/{endpoint}?{query_string}"
        with urllib.request.urlopen(url) as response:
            return f"Response status: {response.status}", extra_params.get("runner") if extra_params else None

    except Exception as e:
        return f"Analytics error: {e!s}", None


def track_runner_on_scarf(runner: str):
    """Track analytics for Daft usage via Scarf."""
    return _track_on_scarf("daft-runner", {"runner": runner})


def track_import_on_scarf():
    """Track analytics for Daft imports via Scarf."""
    return _track_on_scarf("daft-import")
