import os
import platform
import urllib.parse
import urllib.request
from typing import Union
from daft import get_version, get_build_type

def scarf_telemetry(scarf_opt_out: bool, runner: str) -> tuple[Union[str, None], Union[str, None]]:
    """Track analytics for Daft usage via Scarf.

    Args:
        user_opted_out (bool): Whether the user has opted out of analytics
        runner (str): The runner being used (py, ray, or native)

    Returns:
        tuple[str | None, str | None]: Response status and runner type, or (None, None) if analytics disabled/failed
    """

    version = get_version()
    build_type = get_build_type()

    try:
        # Skip analytics for dev builds or if user opted out
        if build_type == "dev" or scarf_opt_out:
            return None, None

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

    return None, None

# def scarf_analytics(
#     scarf_opt_out: bool, build_type: str, version: str, runner: str
# ) -> tuple[Union[str, None], Union[str, None]]:
#     """Track analytics for Daft usage via Scarf.

#     Args:
#         user_opted_out (bool): Whether the user has opted out of analytics
#         build_type (str): The build type from get_build_type()
#         version (str): The version from get_version()
#         runner (str): The runner being used (py, ray, or native)

#     Returns:
#         tuple[str | None, str | None]: Response status and runner type, or (None, None) if analytics disabled/failed
#     """
#     try:
#         # Skip analytics for dev builds or if user opted out
#         if build_type == "dev" or scarf_opt_out:
#             return None, None

#         if os.getenv("SCARF_NO_ANALYTICS") != "true" and os.getenv("DO_NOT_TRACK") != "true":
#             python_version = ".".join(platform.python_version().split(".")[:2])

#             params = {
#                 "version": version,
#                 "platform": platform.system(),
#                 "python": python_version,
#                 "arch": platform.machine(),
#                 "runner": runner,
#             }

#             # Prepare the query string
#             query_string = urllib.parse.urlencode(params)

#             # Make the GET request
#             url = f"https://daft.gateway.scarf.sh/daft-runner?{query_string}"
#             with urllib.request.urlopen(url) as response:
#                 return f"Response status: {response.status}", runner

#     except Exception as e:
#         return f"Analytics error: {e!s}", None

#     return None, None