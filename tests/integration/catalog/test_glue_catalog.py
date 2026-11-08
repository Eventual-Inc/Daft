from __future__ import annotations

from collections.abc import Iterator

import boto3
import pytest

import daft
from daft.catalog import Identifier, NotFoundError
from daft.catalog.__glue import GlueCatalog, GlueParquetTable, load_glue

pytest.importorskip("moto")
from moto.server import ThreadedMotoServer

pytestmark = pytest.mark.integration()

REGION = "us-west-2"


@pytest.fixture(scope="module")
def glue_endpoint() -> Iterator[str]:
    """Runs moto as a standalone HTTP server so the catalog talks to Glue over a real network endpoint."""
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def glue_catalog(glue_endpoint: str) -> Iterator[GlueCatalog]:
    catalog = load_glue(
        "glue_integration",
        region_name=REGION,
        endpoint_url=glue_endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    yield catalog
    # Leave the shared server clean for the next test.
    client = boto3.client(
        "glue",
        region_name=REGION,
        endpoint_url=glue_endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    for db in client.get_databases()["DatabaseList"]:
        client.delete_database(Name=db["Name"])


def _register_parquet_table(catalog: GlueCatalog, database: str, name: str, location: str) -> None:
    catalog._client.create_table(
        DatabaseName=database,
        TableInput={
            "Name": name,
            "Parameters": {"classification": "parquet"},
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint"}, {"Name": "name", "Type": "string"}],
                "Location": location,
            },
        },
    )


def test_namespace_lifecycle(glue_catalog: GlueCatalog) -> None:
    glue_catalog.create_namespace("ns_a")
    glue_catalog.create_namespace_if_not_exists("ns_a")
    glue_catalog.create_namespace("ns_b")

    assert glue_catalog.has_namespace("ns_a")
    assert set(glue_catalog.list_namespaces()) == {Identifier("ns_a"), Identifier("ns_b")}

    with pytest.raises(ValueError, match="already exists"):
        glue_catalog.create_namespace("ns_a")

    glue_catalog.drop_namespace("ns_a")
    assert not glue_catalog.has_namespace("ns_a")
    with pytest.raises(NotFoundError):
        glue_catalog.drop_namespace("ns_a")


def test_table_listing_and_lookup(glue_catalog: GlueCatalog, tmp_path) -> None:
    glue_catalog.create_namespace("db")
    # Table.schema() resolves the schema by scanning the location, so it needs data files.
    daft.from_pydict({"id": [1], "name": ["a"]}).write_parquet(f"{tmp_path}/t1")
    _register_parquet_table(glue_catalog, "db", "t1", f"file://{tmp_path}/t1")
    _register_parquet_table(glue_catalog, "db", "t2", f"file://{tmp_path}/t2")

    assert set(glue_catalog.list_tables("db")) == {Identifier("db", "t1"), Identifier("db", "t2")}
    assert glue_catalog.list_tables("db.t1") == [Identifier("db", "t1")]

    table = glue_catalog.get_table("db.t1")
    assert isinstance(table, GlueParquetTable)
    assert table.name == "t1"
    assert [(f.name, f.dtype) for f in table.schema()] == [
        ("id", daft.DataType.int64()),
        ("name", daft.DataType.string()),
    ]

    with pytest.raises(NotFoundError):
        glue_catalog.get_table("db.missing")

    glue_catalog.drop_table("db.t1")
    assert not glue_catalog.has_table("db.t1")
    assert glue_catalog.list_tables("db") == [Identifier("db", "t2")]


def test_parquet_table_roundtrip(glue_catalog: GlueCatalog, tmp_path) -> None:
    glue_catalog.create_namespace("db")
    _register_parquet_table(glue_catalog, "db", "people", f"file://{tmp_path}/people")

    table = glue_catalog.get_table("db.people")
    table.append(daft.from_pydict({"id": [1, 2], "name": ["a", "b"]}))
    table.append(daft.from_pydict({"id": [3], "name": ["c"]}))

    assert sorted(table.read().to_pydict()["id"]) == [1, 2, 3]

    table.overwrite(daft.from_pydict({"id": [9], "name": ["z"]}))
    assert table.read().to_pydict() == {"id": [9], "name": ["z"]}
