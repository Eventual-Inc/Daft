from __future__ import annotations

import datetime
import decimal
import io
import os
import pathlib
import posixpath
import time
from collections.abc import Iterator

import boto3
import docker
import pyarrow as pa
import pytest
import requests
from azure.storage.blob import BlobServiceClient
from pytest_lazyfixture import lazy_fixture

import daft
from daft.delta_lake.delta_lake_scan import _io_config_to_storage_options
from tests.io.delta_lake.mock_s3_server import start_service, stop_process

deltalake = pytest.importorskip("deltalake")


@pytest.fixture(params=[1, 2, 8])
def num_partitions(request) -> int:
    return request.param


@pytest.fixture(
    params=[
        pytest.param((lambda _: None, "a"), id="unpartitioned"),
        pytest.param((lambda i: i, "a"), id="int_partitioned"),
        pytest.param((lambda i: i * 1.5, "b"), id="float_partitioned"),
        pytest.param((lambda i: f"foo_{i}", "c"), id="string_partitioned"),
        pytest.param((lambda i: f"foo_{i}".encode(), "d"), id="string_partitioned"),
        pytest.param((lambda i: datetime.datetime(2024, 2, i + 1), "f"), id="timestamp_partitioned"),
        pytest.param((lambda i: datetime.date(2024, 2, i + 1), "g"), id="date_partitioned"),
        pytest.param((lambda i: decimal.Decimal(str(1000 + i) + ".567"), "h"), id="decimal_partitioned"),
    ]
)
def partition_generator(request) -> tuple[callable, str]:
    return request.param


@pytest.fixture
def base_table() -> pa.Table:
    return pa.table(
        {
            "a": [1, 2, 3],
            "b": [1.1, 2.2, 3.3],
            "c": ["foo", "bar", "baz"],
            "d": [b"foo", b"bar", b"baz"],
            "e": [True, False, True],
            "f": [datetime.datetime(2024, 2, 10), datetime.datetime(2024, 2, 11), datetime.datetime(2024, 2, 12)],
            "g": [datetime.date(2024, 2, 10), datetime.date(2024, 2, 11), datetime.date(2024, 2, 12)],
            "h": pa.array(
                [decimal.Decimal("1234.567"), decimal.Decimal("1233.456"), decimal.Decimal("1232.345")],
                type=pa.decimal128(7, 3),
            ),
            "i": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            "j": [{"x": 1, "y": False}, {"y": True, "z": "foo"}, {"x": 5, "z": "bar"}],
            "k": pa.array(
                [[("x", 1), ("y", 0)], [("a", 2), ("b", 45)], [("c", 4), ("d", 18)]],
                type=pa.map_(pa.string(), pa.int64()),
            ),
            # TODO(Clark): Wait for more temporal type support in Delta Lake.
            # "l": [
            #     datetime.time(hour=1, minute=2, second=4, microsecond=5),
            #     datetime.time(hour=3, minute=4, second=5, microsecond=6),
            #     datetime.time(hour=4, minute=5, second=6, microsecond=7),
            # ],
            # "m": [
            #     datetime.timedelta(days=1, seconds=2, minutes=5, hours=6, weeks=7),
            #     datetime.timedelta(days=2),
            #     datetime.timedelta(hours=4),
            # ],
        }
    )


@pytest.fixture(scope="session")
def data_dir() -> str:
    return "test_data"


############################
### S3-specific fixtures ###
############################


@pytest.fixture(scope="session")
def s3_credentials() -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
    }


@pytest.fixture(scope="session")
def s3_server_ip() -> str:
    return "127.0.0.1"


@pytest.fixture(scope="session")
def s3_server_port() -> int:
    return 5000


@pytest.fixture(scope="session")
def s3_log_file(tmp_path_factory: pytest.TempPathFactory) -> Iterator[io.IOBase]:
    # NOTE(Clark): We have to use a log file for the mock S3 server's stdout/sterr.
    # - If we use None, then the server output will spam stdout.
    # - If we use PIPE, then the server will deadlock if the (relatively small) buffer fills, and the server is pretty
    #   noisy.
    # - If we use DEVNULL, all log output is lost.
    # With a tmp_path log file, we can prevent spam and deadlocks while also providing an avenue for debuggability, via
    # changing this fixture to something persistent, or dumping the file to stdout before closing the file, etc.
    tmp_path = tmp_path_factory.mktemp("s3_logging")
    with open(tmp_path / "s3_log.txt", "w") as f:
        yield f


@pytest.fixture(scope="session")
def s3_server(s3_server_ip: str, s3_server_port: int, s3_log_file: io.IOBase) -> Iterator[str]:
    # NOTE(Clark): The background-threaded moto server tends to lock up under concurrent access, so we run a background
    # moto_server process.
    s3_server_url = f"http://{s3_server_ip}:{s3_server_port}"
    old_env = os.environ.copy()
    # Set required S3 environment variables before starting server.
    # Required to opt out of concurrent writing, since we don't provide a LockClient.
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    try:
        # Start moto server.
        process = start_service("s3", s3_server_ip, s3_server_port, s3_log_file)
        yield s3_server_url
    finally:
        # Shutdown moto server.
        stop_process(process)
        # Restore old set of environment variables.
        os.environ = old_env


@pytest.fixture(scope="function")
def reset_s3(s3_server) -> Iterator[None]:
    # Clears local S3 server of all objects and buckets.
    yield
    requests.post(f"{s3_server}/moto-api/reset")


@pytest.fixture(scope="function")
def s3_path(
    tmp_path: pathlib.Path, data_dir: str, s3_server: str, s3_credentials: dict[str, str], reset_s3: None
) -> tuple[str, daft.io.IOConfig]:
    path = posixpath.join(tmp_path, data_dir).strip("/")

    s3 = boto3.resource(
        "s3",
        region_name="us-west-2",
        use_ssl=False,
        endpoint_url=s3_server,
        aws_access_key_id=s3_credentials["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=s3_credentials["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=s3_credentials["AWS_SESSION_TOKEN"],
    )
    io_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url=s3_server,
            region_name="us-west-2",
            key_id=s3_credentials["AWS_ACCESS_KEY_ID"],
            access_key=s3_credentials["AWS_SECRET_ACCESS_KEY"],
            session_token=s3_credentials["AWS_SESSION_TOKEN"],
            use_ssl=False,
        )
    )
    # Create bucket for first element of tmp path.
    bucket = s3.Bucket(path.split("/")[0])
    bucket.create(CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    # Bucket will get cleared by reset_s3 fixture, so we don't need to delete it at the end of the test via the
    # typical try-yield-finally block.
    return "s3://" + path, io_config


###############################
### Azure-specific fixtures ###
###############################


@pytest.fixture(scope="session")
def az_credentials() -> dict[str, str]:
    return {
        # These are the well-known values
        # https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#well-known-storage-account-and-key
        "ACCOUNT_NAME": "devstoreaccount1",
        "KEY": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    }


@pytest.fixture(scope="session")
def az_server_ip() -> str:
    return "0.0.0.0"


@pytest.fixture(scope="session")
def az_server_port() -> int:
    return 10000


@pytest.fixture(scope="session")
def az_server(az_server_ip: str, az_server_port: int) -> Iterator[str]:
    az_server_url = f"http://{az_server_ip}:{az_server_port}"
    client = docker.from_env()
    azurite = client.containers.run(
        "mcr.microsoft.com/azure-storage/azurite",
        f"azurite-blob --loose --blobHost {az_server_ip}",
        detach=True,
        ports={str(az_server_port): str(az_server_port)},
    )
    for _ in range(100):
        if azurite.status == "running":
            break
        elif azurite.status == "exited":
            output = azurite.logs()
            print(f"Azurite container exited without executing tests: {output}")
            pytest.fail(f"Cannot start mock Azure Blob Storage server")
        time.sleep(0.1)
        azurite.reload()
    try:
        yield az_server_url
    finally:
        azurite.stop()


@pytest.fixture(scope="function")
def az_path(data_dir: str, az_server: str, az_credentials: dict[str, str]) -> Iterator[tuple[str, daft.io.IOConfig]]:
    path = data_dir.strip("/").replace("_", "-")
    account_name = az_credentials["ACCOUNT_NAME"]
    key = az_credentials["KEY"]
    endpoint_url = f"{az_server}/{account_name}"
    conn_str = f"DefaultEndpointsProtocol=http;AccountName={account_name};AccountKey={key};BlobEndpoint={endpoint_url};"

    io_config = daft.io.IOConfig(
        azure=daft.io.AzureConfig(
            storage_account=az_credentials["ACCOUNT_NAME"],
            access_key=az_credentials["KEY"],
            endpoint_url=endpoint_url,
            use_ssl=False,
        )
    )
    bbs = BlobServiceClient.from_connection_string(conn_str)
    container = path.split("/")[0]
    bbs.create_container(container)
    try:
        yield "az://" + path, io_config
    finally:
        bbs.delete_container(container)


@pytest.fixture(scope="function")
def local_path(tmp_path: pathlib.Path, data_dir: str) -> tuple[str, None]:
    path = os.path.join(tmp_path, data_dir)
    os.mkdir(path)
    return path, None


@pytest.fixture(
    scope="function",
    params=[
        lazy_fixture("local_path"),
        lazy_fixture("s3_path"),
        lazy_fixture("az_path"),
    ],
)
def deltalake_table(
    request, base_table: pa.Table, num_partitions: int, partition_generator: callable
) -> tuple[str, daft.io.IOConfig | None, dict[str, str], list[pa.Table]]:
    partition_generator, _ = partition_generator
    path, io_config = request.param
    storage_options = _io_config_to_storage_options(io_config, path) if io_config is not None else None
    parts = []
    for i in range(num_partitions):
        # Generate partition value and add partition column.
        part_value = partition_generator(i)
        part = base_table.append_column("part_idx", pa.array([part_value if part_value is not None else i] * 3))
        parts.append(part)
    table = pa.concat_tables(parts)
    deltalake.write_deltalake(
        path,
        table,
        partition_by="part_idx" if partition_generator(0) is not None else None,
        storage_options=storage_options,
    )
    return path, io_config, parts
