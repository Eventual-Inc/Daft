import os
import platform
import urllib.parse
import urllib.request

from daft import get_version

__version__ = get_version()

# General Scarf call
# def scarf_analytics():
#     try:
#         if os.getenv("SCARF_NO_ANALYTICS") != "true" and os.getenv("DO_NOT_TRACK") != "true":
#             python_version = ".".join(platform.python_version().split(".")[:2])

#             params = {
#                 "version": __version__,
#                 "platform": platform.system(),
#                 "python": python_version,
#                 "arch": platform.machine(),
#             }

#             # Prepare the query string
#             query_string = urllib.parse.urlencode(params)

#             # Make the GET request
#             url = f"https://daft.gateway.scarf.sh/daft-import?{query_string}"
#             with urllib.request.urlopen(url) as response:
#                 print(f"Response status: {response.status}")

#     except Exception:
#         pass

# For Py, Ray, or Native runner
def scarf_analytics():
    try:
        if os.getenv("SCARF_NO_ANALYTICS") != "true" and os.getenv("DO_NOT_TRACK") != "true":
            python_version = ".".join(platform.python_version().split(".")[:2])

            params = {
                "version": __version__,
                "platform": platform.system(),
                "python": python_version,
                "arch": platform.machine(),
            }

            # Prepare the query string
            query_string = urllib.parse.urlencode(params)

            # Define URLs for different runners
            runner_urls = {
                "py": f"https://daft.gateway.scarf.sh/daft-runner-py?{query_string}",
                "ray": f"https://daft.gateway.scarf.sh/daft-runner-ray?{query_string}",
                "native": f"https://daft.gateway.scarf.sh/daft-runner-native?{query_string}",
            }

            # Get the DAFT_RUNNER environment variable
            daft_runner = os.getenv("DAFT_RUNNER", "py").lower()  # Default to "py" if not set

            # Select the appropriate URL based on the runner
            if daft_runner in runner_urls:
                url = runner_urls[daft_runner]
            else:
                raise ValueError(f"Unknown DAFT_RUNNER: {daft_runner}. Please set it to 'py', 'ray', or 'native'.")

            # Make the GET request
            with urllib.request.urlopen(url) as response:
                return f"Response status: {response.status}", daft_runner

    except Exception as e:
        return f"Analytics error: {str(e)}", None

    return None, None