from __future__ import annotations

import time

import pytest
import requests
from requests.exceptions import RequestException

from daft.unity_catalog import UnityCatalog


@pytest.fixture(scope="session")
def local_unity_catalog() -> UnityCatalog:
    endpoint = "http://127.0.0.1:8080"
    max_retries = 25
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            # Try to connect to the server
            response = requests.get(endpoint)
            response.raise_for_status()
            # If successful, return the UnityCatalog instance
            return UnityCatalog(endpoint=endpoint, token="not-used")
        except RequestException:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise RuntimeError(f"Failed to connect to Unity Catalog server at {endpoint} after {max_retries} attempts")
