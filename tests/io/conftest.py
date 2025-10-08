from __future__ import annotations

import io
import os
from collections.abc import Iterator

import pytest
import requests

from tests.io.mock_aws_server import start_service, stop_process

############################
### AWS-specific fixtures ###
############################


@pytest.fixture(scope="session")
def aws_credentials() -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
    }


@pytest.fixture(scope="session")
def aws_server_ip() -> str:
    return "127.0.0.1"


@pytest.fixture(scope="session")
def aws_server_port() -> int:
    return 5000


@pytest.fixture(scope="session")
def aws_log_file(tmp_path_factory: pytest.TempPathFactory) -> Iterator[io.IOBase]:
    # NOTE(Clark): We have to use a log file for the mock AWS server's stdout/sterr.
    # - If we use None, then the server output will spam stdout.
    # - If we use PIPE, then the server will deadlock if the (relatively small) buffer fills, and the server is pretty
    #   noisy.
    # - If we use DEVNULL, all log output is lost.
    # With a tmp_path log file, we can prevent spam and deadlocks while also providing an avenue for debuggability, via
    # changing this fixture to something persistent, or dumping the file to stdout before closing the file, etc.
    tmp_path = tmp_path_factory.mktemp("aws_logging")
    with open(tmp_path / "aws_log.txt", "w") as f:
        yield f


@pytest.fixture(scope="session")
def aws_server(aws_server_ip: str, aws_server_port: int, aws_log_file: io.IOBase) -> Iterator[str]:
    # NOTE(Clark): The background-threaded moto server tends to lock up under concurrent access, so we run a background
    # moto_server process.
    aws_server_url = f"http://{aws_server_ip}:{aws_server_port}"
    old_env = os.environ.copy()
    # Set required AWS environment variables before starting server.
    # Required to opt out of concurrent writing, since we don't provide a LockClient.
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    try:
        # Start moto server.
        process = start_service(aws_server_ip, aws_server_port, aws_log_file)
        yield aws_server_url
    finally:
        # Shutdown moto server.
        stop_process(process)
        # Restore old set of environment variables.
        os.environ = old_env


@pytest.fixture(scope="function")
def reset_aws(aws_server) -> Iterator[None]:
    # Clears local AWS server of all state (e.g. S3 buckets and objects).
    yield
    requests.post(f"{aws_server}/moto-api/reset")
