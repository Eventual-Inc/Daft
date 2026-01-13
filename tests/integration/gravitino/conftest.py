from __future__ import annotations

import os
import time

import pytest

from daft.dependencies import requests
from daft.gravitino import GravitinoClient
from daft.io import IOConfig

_GRAVITINO_HEADERS = {
    "Accept": "application/vnd.gravitino.v1+json",
    "Content-Type": "application/json",
}


def _wait_for_gravitino(endpoint: str, timeout_secs: int = 120) -> None:
    """Blocks until a Gravitino server responds or times out."""
    deadline = time.time() + timeout_secs
    last_error: Exception | None = None
    version_url = f"{endpoint.rstrip('/')}/api/version"

    while time.time() < deadline:
        try:
            response = requests.get(version_url, headers=_GRAVITINO_HEADERS, timeout=5)
            response.raise_for_status()
            return
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(3)

    raise RuntimeError(f"Failed to connect to Gravitino server at {endpoint} within {timeout_secs}s") from last_error


@pytest.fixture(scope="session")
def gravitino_endpoint() -> str:
    return os.environ.get("GRAVITINO_ENDPOINT", "http://127.0.0.1:8090")


@pytest.fixture(scope="session")
def gravitino_metalake() -> str:
    return os.environ.get("GRAVITINO_METALAKE", "metalake_demo")


@pytest.fixture(scope="session")
def gravitino_auth_type() -> str:
    return os.environ.get("GRAVITINO_AUTH_TYPE", "simple")


@pytest.fixture(scope="session")
def local_gravitino_client(
    gravitino_endpoint: str,
    gravitino_metalake: str,
    gravitino_auth_type: str,
) -> GravitinoClient:
    _wait_for_gravitino(gravitino_endpoint)

    username = os.environ.get("GRAVITINO_USERNAME", "admin" if gravitino_auth_type == "simple" else None)
    password = os.environ.get("GRAVITINO_PASSWORD")
    token = os.environ.get("GRAVITINO_TOKEN")

    return GravitinoClient(
        endpoint=gravitino_endpoint,
        metalake_name=gravitino_metalake,
        auth_type=gravitino_auth_type,
        username=username,
        password=password,
        token=token,
    )


@pytest.fixture(scope="session")
def gravitino_io_config(local_gravitino_client: GravitinoClient) -> IOConfig:
    return local_gravitino_client.to_io_config()


@pytest.fixture(scope="session")
def gravitino_sample_file() -> str:
    """Returns a gvfs:// URL for sample file access."""
    return os.environ.get(
        "GRAVITINO_TEST_FILE",
        "gvfs://fileset/s3_fileset_catalog3/test_schema/test_fileset/"
        "part-00000-a1a42661-7a85-42da-b831-f489a5545d61-c000.snappy.parquet",
    )


@pytest.fixture(scope="session")
def gravitino_sample_dir() -> str:
    """Returns a gvfs:// directory URL for sample fileset access."""
    return os.environ.get("GRAVITINO_TEST_DIR", "gvfs://fileset/s3_fileset_catalog3/test_schema/test_fileset/")


@pytest.fixture(scope="session")
def gravitino_minio_io_config():
    """IOConfig for MinIO S3-compatible storage used in Gravitino tests."""
    import daft

    return daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url="http://127.0.0.1:9001",
            key_id="minioadmin",
            access_key="minioadmin",
            region_name="us-east-1",
            use_ssl=False,
        )
    )
