from __future__ import annotations

import datetime
import decimal
import io
import os
import pathlib
import posixpath
import time
import unittest.mock as mock
from collections.abc import Iterator

import boto3
import pyarrow as pa
import pytest
import requests
from azure.storage.blob import BlobServiceClient
from pytest_lazyfixture import lazy_fixture

import daft
from daft import DataCatalogTable, DataCatalogType
from daft.io.object_store_options import io_config_to_storage_options
from tests.io.mock_aws_server import start_service, stop_process


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
        pytest.param((lambda i: i if i % 2 == 0 else None, "a"), id="partitioned_with_nulls"),
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


##############################
### Glue-specific fixtures ###
##############################


@pytest.fixture(scope="session")
def glue_database_name() -> str:
    return "glue_db"


@pytest.fixture(scope="session")
def glue_table_name() -> str:
    return "glue_table"


@pytest.fixture(scope="function")
def glue_table(
    aws_server: str,
    glue_database_name: str,
    glue_table_name: str,
    s3_uri: str,
    aws_credentials: dict[str, str],
) -> DataCatalogTable:
    glue = boto3.client(
        "glue",
        region_name="us-west-2",
        use_ssl=False,
        endpoint_url=aws_server,
        aws_access_key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=aws_credentials["AWS_SESSION_TOKEN"],
    )
    glue.create_database(DatabaseInput={"Name": glue_database_name})
    glue.create_table(
        DatabaseName=glue_database_name,
        TableInput={
            "Name": glue_table_name,
            "StorageDescriptor": {
                "Location": s3_uri,
            },
        },
    )
    return DataCatalogTable(
        catalog=DataCatalogType.GLUE,
        database_name=glue_database_name,
        table_name=glue_table_name,
    )


##############################
### Unity-specific fixtures ###
##############################


@pytest.fixture(scope="session")
def unity_catalog_id() -> str:
    return "unity_catalog"


@pytest.fixture(scope="session")
def unity_database_name() -> str:
    return "unity_db"


@pytest.fixture(scope="session")
def unity_table_name() -> str:
    return "unity_table"


@pytest.fixture(scope="function")
def unity_table_s3(
    s3_uri: str,
    unity_catalog_id: str,
    unity_database_name: str,
    unity_table_name: str,
) -> Iterator[DataCatalogTable]:
    yield from _unity_table(s3_uri, unity_catalog_id, unity_database_name, unity_table_name)


@pytest.fixture(scope="function")
def unity_table_az(
    az_uri: str,
    unity_catalog_id: str,
    unity_database_name: str,
    unity_table_name: str,
) -> Iterator[DataCatalogTable]:
    yield from _unity_table(az_uri, unity_catalog_id, unity_database_name, unity_table_name)


def _unity_table(
    uri: str,
    unity_catalog_id: str,
    unity_database_name: str,
    unity_table_name: str,
) -> Iterator[DataCatalogTable]:
    from databricks.sdk.service.catalog import TableInfo

    with mock.patch("databricks.sdk.WorkspaceClient") as mock_workspace_client:
        instance = mock_workspace_client.return_value
        instance.tables.get.return_value = TableInfo(storage_location=uri)
        # Set required Databricks environment variables before using SDK.
        # NOTE: This URI won't be used by the Databricks SDK since we mock the WorkspaceClient.
        old_env = os.environ.copy()
        os.environ["DATABRICKS_HOST"] = "http://localhost"
        try:
            yield DataCatalogTable(
                catalog=DataCatalogType.UNITY,
                database_name=unity_database_name,
                table_name=unity_table_name,
                catalog_id=unity_catalog_id,
            )
        finally:
            # Restore old set of environment variables.
            os.environ = old_env


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


@pytest.fixture(scope="function")
def s3_uri(tmp_path: pathlib.Path, data_dir: str) -> str:
    path = posixpath.join(tmp_path, data_dir).strip("/")
    return "s3://" + path


@pytest.fixture(
    scope="function",
    params=[
        None,
        pytest.param(lazy_fixture("glue_table"), marks=pytest.mark.glue),
        pytest.param(lazy_fixture("unity_table_s3"), marks=pytest.mark.unity),
    ],
)
def s3_path(
    request, s3_uri: str, aws_server: str, aws_credentials: dict[str, str], reset_aws: None
) -> tuple[str, daft.io.IOConfig, DataCatalogTable | None]:
    s3 = boto3.resource(
        "s3",
        region_name="us-west-2",
        use_ssl=False,
        endpoint_url=aws_server,
        aws_access_key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=aws_credentials["AWS_SESSION_TOKEN"],
    )
    io_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url=aws_server,
            region_name="us-west-2",
            key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
            access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
            session_token=aws_credentials["AWS_SESSION_TOKEN"],
            use_ssl=False,
        )
    )
    # Create bucket for first element of tmp path.
    bucket = s3.Bucket(s3_uri[5:].split("/")[0])
    bucket.create(CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    # Bucket will get cleared by reset_s3 fixture, so we don't need to delete it at the end of the test via the
    # typical try-yield-finally block.
    return s3_uri, io_config, request.param


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


@pytest.fixture(scope="function")
def az_uri(tmp_path: pathlib.Path, data_dir: str) -> str:
    path = data_dir.strip("/").replace("_", "-")
    return "az://" + path


@pytest.fixture(scope="session")
def az_server(az_server_ip: str, az_server_port: int) -> Iterator[str]:
    docker = pytest.importorskip("docker")
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
            pytest.fail("Cannot start mock Azure Blob Storage server")
        time.sleep(0.1)
        azurite.reload()
    try:
        yield az_server_url
    finally:
        azurite.stop()


@pytest.fixture(
    scope="function",
    params=[
        None,
        pytest.param(lazy_fixture("unity_table_az"), marks=pytest.mark.unity),
    ],
)
def az_path(
    az_uri: str, az_server: str, az_credentials: dict[str, str]
) -> Iterator[tuple[str, daft.io.IOConfig, None]]:
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
    container = az_uri[5:].split("/")[0]
    bbs.create_container(container)
    try:
        yield az_uri, io_config, None
    finally:
        bbs.delete_container(container)


@pytest.fixture(scope="function")
def local_path(tmp_path: pathlib.Path, data_dir: str) -> tuple[str, None, None]:
    path = os.path.join(tmp_path, data_dir)
    os.mkdir(path)
    return path, None, None


@pytest.fixture(
    scope="function",
    params=[
        pytest.param(lazy_fixture("local_path"), marks=pytest.mark.local),
        pytest.param(lazy_fixture("s3_path"), marks=pytest.mark.s3),
        # Azure tests require starting a Docker container + mock server that (1) requires a dev Docker dependency, and
        # (2) takes 15+ seconds to start on every run, so we current mark it as an integration test.
        pytest.param(lazy_fixture("az_path"), marks=(pytest.mark.az, pytest.mark.integration)),
    ],
)
def cloud_paths(request) -> tuple[str, daft.io.IOConfig | None, DataCatalogTable | None]:
    return request.param


@pytest.fixture(scope="function")
def deltalake_table(
    cloud_paths, base_table: pa.Table, num_partitions: int, partition_generator: callable
) -> tuple[str, daft.io.IOConfig | None, dict[str, str], list[pa.Table]]:
    partition_generator, col = partition_generator
    path, io_config, catalog_table = cloud_paths
    storage_options = io_config_to_storage_options(io_config, path) if io_config is not None else None
    parts = []
    for i in range(num_partitions):
        # Generate partition value and add partition column.
        part_value = partition_generator(i)
        part = base_table.append_column("part_idx", pa.array([part_value] * 3, type=base_table.column(col).type))
        parts.append(part)
    table = pa.concat_tables(parts)
    deltalake = pytest.importorskip("deltalake")
    deltalake.write_deltalake(
        path,
        table,
        partition_by="part_idx" if partition_generator(0) is not None else None,
        storage_options=storage_options,
    )
    return path, catalog_table, io_config, parts
