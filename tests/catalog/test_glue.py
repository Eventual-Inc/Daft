from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import boto3
import pytest
from moto import mock_aws

from daft.catalog import Identifier, NotFoundError
from daft.catalog.__glue import GlueCatalog, GlueTable, load_glue
from daft.dataframe import DataFrame

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_glue.type_defs import ColumnOutputTypeDef as GlueColumnInfo
    from mypy_boto3_glue.type_defs import TableTypeDef as GlueTableInfo

    from daft.dataframe import DataFrame
else:
    GlueClient = Any
    GlueColumnInfo = Any
    GlueTableInfo = Any


@pytest.fixture
def glue_client():
    with mock_aws():
        yield boto3.client("glue", region_name="us-west-2")


@pytest.fixture
def glue_catalog(glue_client):
    gc = GlueCatalog.from_client("mock_glue_catalog", glue_client)
    gc._table_impls.append(GlueTestTable)  # !! REGISTER GLUE TEST TABLE !!
    return gc


###
# catalog constructors
###


def test_load_glue():
    catalog = load_glue(
        "mock_glue_catalog",
        region_name="us-west-2",
        api_version="2017-03-31",
        use_ssl=False,
        verify=True,
        endpoint_url="http://localhost",
        aws_access_key_id="test_access_key",
        aws_secret_access_key="test_secret_key",
        aws_session_token="test_session_token",
    )
    assert catalog.name == "mock_glue_catalog"


def test_catalog_from_session(glue_client):
    # Test creating a catalog from a session
    session = boto3.Session(region_name="us-east-1")
    catalog = GlueCatalog.from_session("mock_glue_catalog", session)
    assert catalog.name == "mock_glue_catalog"


###
# catalog methods
###


def test_create_namespace(glue_catalog, glue_client):
    # create a namespace with catalog
    glue_catalog.create_namespace("test_namespace")

    # verify with client
    res = glue_client.get_databases()
    assert len(res["DatabaseList"]) == 1
    assert res["DatabaseList"][0]["Name"] == "test_namespace"

    # err. already exists
    with pytest.raises(ValueError, match="already exists"):
        glue_catalog.create_namespace("test_namespace")


def test_create_namespace_if_not_exists(glue_catalog, glue_client):
    # create a namespace with catalog
    glue_catalog.create_namespace_if_not_exists("test_namespace")
    glue_catalog.create_namespace_if_not_exists("test_namespace")

    # verify we only have one with client
    res = glue_client.get_databases()
    assert len(res["DatabaseList"]) == 1


def test_drop_namespace(glue_catalog, glue_client):
    # create then drop the namespace
    glue_catalog.create_namespace("test_namespace")
    glue_catalog.drop_namespace("test_namespace")

    # verify was dropped with client
    res = glue_client.get_databases()
    assert len(res["DatabaseList"]) == 0

    # drop if doesn"t exist should error
    with pytest.raises(NotFoundError, match="not found"):
        glue_catalog.drop_namespace("nonexistent_namespace")


def test_list_tables(glue_catalog, glue_client):
    # create some tables
    create_glue_database(glue_client, "test_database")
    create_glue_table(glue_client, "test_database", "table1")
    create_glue_table(glue_client, "test_database", "table2")

    # list tables with catalog
    tables = glue_catalog.list_tables("test_database")
    assert len(tables) == 2
    assert "test_database.table1" in tables
    assert "test_database.table2" in tables

    # test listing tables with pattern
    tables = glue_catalog.list_tables("test_database.table1")
    assert len(tables) == 1
    assert "test_database.table1" in tables

    # test listing tables with no pattern
    with pytest.raises(ValueError, match="requires the pattern to contain a namespace"):
        glue_catalog.list_tables(None)


def test_list_namespaces(glue_catalog, glue_client):
    # create namespaces with the client
    create_glue_database(glue_client, "namespace1")
    create_glue_database(glue_client, "namespace2")

    # list namespaces with the catalog
    namespaces = glue_catalog.list_namespaces()
    assert len(namespaces) == 2
    assert Identifier("namespace1") in namespaces
    assert Identifier("namespace2") in namespaces

    # glue doesn"t have any patterns/filters for get_databases
    with pytest.raises(ValueError):
        glue_catalog.list_namespaces("namespace1")


def test_get_table(glue_catalog, glue_client):
    # create a table with the client
    create_glue_database(glue_client, "test_database")
    create_glue_table(glue_client, "test_database", "test_table")

    # Get the table
    table = glue_catalog.get_table("test_database.test_table")
    assert table.name == "test_table"

    # Test getting a table that doesn"t exist
    with pytest.raises(NotFoundError, match="not found"):
        glue_catalog.get_table("test_database.nonexistent_table")


def test_drop_table(glue_catalog, glue_client):
    # create a table with the client
    create_glue_database(glue_client, "test_database")
    create_glue_table(glue_client, "test_database", "test_table")

    # drop the table with the catalog
    glue_catalog.drop_table("test_database.test_table")

    with pytest.raises(NotFoundError):
        glue_catalog.get_table("test_database.test_table")
    with pytest.raises(NotFoundError):
        glue_catalog.drop_table("test_database.nonexistent_table")


###
# helpers
###


def create_glue_database(client, database_name):
    client.create_database(
        DatabaseInput={
            "Name": database_name,
        }
    )


def create_glue_table(client, database_name, table_name, location=None):
    if location is None:
        location = f"s3://bucket/{table_name}/"
    client.create_table(
        DatabaseName=database_name,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "col1", "Type": "string"},
                ],
                "Location": location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            },
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                # !! marker that this is a test table !!
                "pytest": "True"
            },
        },
    )


class GlueTestTable(GlueTable):
    """GlueTestTable shows how we register custom table implementations."""

    @classmethod
    def from_table_info(cls, catalog: GlueCatalog, table: GlueTableInfo) -> GlueTable:
        if bool(table["Parameters"].get("pytest")):
            return cls(catalog, table)
        raise ValueError("Expected Parameter pytest='True'")

    def read(self, **options) -> DataFrame:
        raise NotImplementedError

    def write(self, df: DataFrame, mode: Literal["append"] | Literal["overwrite"] = "append", **options) -> None:
        raise NotImplementedError
