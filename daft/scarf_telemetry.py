import os
import platform
import threading
import urllib.parse
import urllib.request
from typing import Optional


def _track_on_scarf(endpoint: str, extra_params: Optional[dict] = None) -> tuple[Optional[threading.Thread], dict]:
    """Common implementation for Scarf telemetry tracking. Executes the request in a separate daemon thread to avoid blocking the main thread.

    Args:
        endpoint: The Scarf endpoint to use (e.g., "daft-runner" or "daft-import")
        extra_params: Optional additional parameters to include in the request

    Returns:
        Tuple containing:
            - Thread handle if telemetry is enabled, None otherwise
            - Dictionary containing the response status and extra value
    """
    from daft import get_build_type, get_version

    version = get_version()
    build_type = get_build_type()
    scarf_opt_out = os.getenv("SCARF_NO_ANALYTICS") == "true" or os.getenv("DO_NOT_TRACK") == "true"
    daft_analytics_disabled = os.getenv("DAFT_ANALYTICS_ENABLED") == "0"
    result_container = {"response_status": None, "extra_value": None}

    # Skip analytics for dev builds or if user opted out
    if build_type == "dev" or scarf_opt_out or daft_analytics_disabled:
        return None, result_container

    def send_request(result_container):
        response_status = None
        extra_value = extra_params.get("runner") if extra_params else None

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
                response_status = f"Response status: {response.status}"
        except Exception as e:
            response_status = f"Analytics error: {e}"

        result_container["response_status"] = response_status
        result_container["extra_value"] = extra_value

    # Start a daemon thread to send the request
    thread = threading.Thread(target=send_request, daemon=True, args=(result_container,))
    thread.start()

    return thread, result_container


def track_runner_on_scarf(runner: str) -> tuple[Optional[threading.Thread], dict]:
    """Track analytics for Daft usage via Scarf."""
    return _track_on_scarf("daft-runner", {"runner": runner})


def track_import_on_scarf() -> tuple[Optional[threading.Thread], dict]:
    """Track analytics for Daft imports via Scarf."""
    return _track_on_scarf("daft-import")
