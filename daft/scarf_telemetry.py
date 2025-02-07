import os
import platform

import requests

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

            requests.get("https://daft.gateway.scarf.sh/getdaft", params=params)

    except Exception:
        pass


# For testing
# logging.basicConfig(level=logging.DEBUG)

# def scarf_analytics():
#     __version__ = get_version()
#     try:
#         if os.getenv("SCARF_NO_ANALYTICS") != "true" and os.getenv("DO_NOT_TRACK") != "true":
#             python_version = ".".join(platform.python_version().split(".")[:2])

#             url = "https://daft.gateway.scarf.sh/getdaft?version=" + __version__ + "&platform=" + platform.system() + "&python=" + python_version + "&arch=" + platform.machine()

#             print(f"Sending analytics to: {url}")
#             response = requests.get(url)
#             print(f"Response status: {response.status_code}")

#     except Exception as e:
#         print(f"Analytics error: {str(e)}")
