import os
import platform
import urllib.parse
import urllib.request

from daft import get_version

__version__ = get_version()


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

            # Make the GET request
            url = f"https://daft.gateway.scarf.sh/getdaft?{query_string}"
            with urllib.request.urlopen(url) as response:
                print(f"Response status: {response.status}")

    except Exception:
        pass


# For testing
# logging.basicConfig(level=logging.DEBUG)

# def scarf_analytics():
#     __version__ = get_version()
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

#             # Make the GET request using urllib
#             url = f"https://daft.gateway.scarf.sh/getdaft?{query_string}"
#             print(f"Sending analytics to: {url}")  # Print message for verification
#             with urllib.request.urlopen(url) as response:
#                 print(f"Response status: {response.status}")

#     except Exception as e:
#         print(f"Analytics error: {str(e)}")
