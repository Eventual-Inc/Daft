from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3
import botocore
import pytest
from moto import mock_aws

from daft.catalog import Catalog, Identifier, NotFoundError
from daft.catalog.__glue import (
    GlueCatalog,
    GlueCsvTable,
    GlueIcebergTable,
    GlueParquetTable,
    GlueTable,
    _convert_glue_schema,
    load_glue,
)
from daft.dataframe import DataFrame
from daft.logical.schema import DataType, Schema

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
def mock_boto3_session():
    with mock_aws():
        yield boto3.Session(region_name="us-west-2")


@pytest.fixture
def mock_botocore_session():
    with mock_aws():
        sess = botocore.session.Session()
        sess.set_config_variable("region", "us-west-2")
        yield sess


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


def test_catalog_from_client(glue_client):
    assert Catalog.from_glue("gc", client=glue_client)
    assert GlueCatalog.from_client("gc", glue_client)


def test_catalog_from_session(mock_boto3_session, mock_botocore_session):
    # Test both boto3 and botocore sessions.
    assert Catalog.from_glue("gc", session=mock_boto3_session)
    assert GlueCatalog.from_session("gc", session=mock_boto3_session)

    assert Catalog.from_glue("gc", session=mock_botocore_session)
    assert GlueCatalog.from_session("gc", session=mock_botocore_session)


def test_catalog_no_args():
    with pytest.raises(ValueError, match="Must provide either a client or session."):
        Catalog.from_glue("foo")


def test_glue_catalog_init():
    with pytest.raises(ValueError, match="not supported"):
        GlueCatalog()


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

    # drop if doesn't exist should error
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
    assert Identifier("test_database", "table1") in tables
    assert Identifier("test_database", "table2") in tables

    # test listing tables with pattern
    tables = glue_catalog.list_tables("test_database.table1")
    assert len(tables) == 1
    assert Identifier("test_database", "table1") in tables

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

    # glue doesn't have any patterns/filters for get_databases
    with pytest.raises(ValueError):
        glue_catalog.list_namespaces("namespace1")


def test_get_table(glue_catalog, glue_client):
    # create a table with the client
    create_glue_database(glue_client, "test_database")
    create_glue_table(glue_client, "test_database", "test_table")

    # Get the table
    table = glue_catalog.get_table("test_database.test_table")
    assert table.name == "test_table"

    # Test getting a table that doesn't exist
    with pytest.raises(NotFoundError, match="not found"):
        glue_catalog.get_table("test_database.nonexistent_table")

    # Invalid identifiers
    with pytest.raises(ValueError, match="Expected identifier with form `<database_name>.<table_name>`"):
        glue_catalog.drop_table("a.b.c")
    with pytest.raises(ValueError, match="Expected identifier with form `<database_name>.<table_name>`"):
        glue_catalog.drop_table("a")


def test_get_table_no_impls(glue_catalog, glue_client):
    create_glue_database(glue_client, "test_database")
    create_glue_table(glue_client, "test_database", "test_table")

    # no impls!
    glue_catalog._table_impls = []

    # get_table should fail
    with pytest.raises(ValueError, match="No supported table implementation"):
        glue_catalog.get_table("test_database.test_table")


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

    with pytest.raises(ValueError, match="Expected identifier with form `<database_name>.<table_name>`"):
        glue_catalog.drop_table("a.b.c")
    with pytest.raises(ValueError, match="Expected identifier with form `<database_name>.<table_name>`"):
        glue_catalog.drop_table("a")


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
                "Location": location,
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

    def append(
        self,
        df: DataFrame,
        **options,
    ) -> None:
        raise NotImplementedError

    def overwrite(
        self,
        df: DataFrame,
        **options,
    ) -> None:
        raise NotImplementedError


###
# GlueCsvTable Testing
###


def test_glue_csv_table_init_not_supported():
    with pytest.raises(ValueError, match="not supported"):
        GlueCsvTable()


def test_glue_csv_table_from_table_info_1(glue_catalog):
    table_info = {
        "Name": "test_csv_table",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "col1", "Type": "string"},
                {"Name": "col2", "Type": "integer"},
                {"Name": "col3", "Type": "double"},
            ],
            "Location": "s3://bucket/test_csv_table/",
        },
        "Parameters": {
            "classification": "CSV",
        },
    }
    csv_table = GlueCsvTable.from_table_info(glue_catalog, table_info)
    assert csv_table._path == "s3://bucket/test_csv_table/"
    assert len(csv_table._schema) == 3
    assert csv_table._schema == Schema.from_pydict(
        {
            "col1": DataType.string(),
            "col2": DataType.int32(),
            "col3": DataType.float64(),
        }
    )
    assert csv_table._has_headers is False
    assert csv_table._delimiter == ","


def test_glue_csv_table_from_table_info_2(glue_catalog):
    table_info = {
        "Name": "test_csv_table",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "col1", "Type": "string"},
            ],
            "Location": "s3://bucket/test_csv_table/",
        },
        "Parameters": {
            "classification": "CSV",
            "skip.header.line.count": "0",
            "delimiter": "\t",
        },
    }
    csv_table = GlueCsvTable.from_table_info(glue_catalog, table_info)
    assert csv_table._has_headers is False
    assert csv_table._delimiter == "\t"


def test_glue_csv_table_from_table_info_3(glue_catalog):
    table_info = {
        "Name": "test_csv_table",
        "Parameters": {},  # !! ERR !! no classification
    }
    with pytest.raises(
        ValueError,
        match="GlueTableInfo is missing the required parameter 'classification'",
    ):
        GlueCsvTable.from_table_info(glue_catalog, table_info)


def test_glue_csv_table_from_table_info_4(glue_catalog):
    table_info = {
        "Name": "test_csv_table",
        "Parameters": {
            "classification": "parquet",
        },
    }
    with pytest.raises(ValueError, match="GlueTableInfo had classification parquet, but expected 'CSV'"):
        GlueCsvTable.from_table_info(glue_catalog, table_info)


###
# GlueParquetTable
###


def test_glue_parquet_table_init_not_supported():
    with pytest.raises(ValueError, match="not supported"):
        GlueParquetTable()


def test_glue_parquet_table_from_table_info_1(glue_catalog):
    table_info = {
        "Name": "test_parquet_table",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "col1", "Type": "string"},
            ],
            "Location": "s3://bucket/test_parquet_table/",
        },
        "Parameters": {
            "classification": "parquet",
        },
    }
    parquet_table = GlueParquetTable.from_table_info(glue_catalog, table_info)
    assert parquet_table.name == "test_parquet_table"


def test_glue_parquet_table_from_table_info_2(glue_catalog):
    table_info = {
        "Name": "test_parquet_table",
        "Parameters": {},  # !! ERR !! no classification
    }
    with pytest.raises(
        ValueError,
        match="GlueTableInfo is missing the required parameter 'classification'",
    ):
        GlueParquetTable.from_table_info(glue_catalog, table_info)


def test_glue_parquet_table_from_table_info_3(glue_catalog):
    table_info = {
        "Name": "test_parquet_table",
        "Parameters": {
            "classification": "CSV",
        },
    }
    with pytest.raises(ValueError, match="GlueTableInfo had classification CSV, but expected 'parquet'"):
        GlueParquetTable.from_table_info(glue_catalog, table_info)


###
# GlueIcebergTable
###


def test_glue_iceberg_table_init_not_supported():
    with pytest.raises(ValueError, match="not supported"):
        GlueIcebergTable()


def test_glue_iceberg_table_from_table_info_1(glue_catalog):
    table_info = {
        "DatabaseName": "test_iceberg_database",
        "Name": "test_iceberg_table",
        "StorageDescriptor": {},
        "Parameters": {
            "table_type": "iceberg",
            "metadata_location": "s3://bucket/test_iceberg_table/metadata.json",
        },
    }
    with pytest.raises(Exception):
        # Fails in pyarrow as OSError, which is ok for testing because it means we made it that far.
        # i.e. the daft logic is correctly picking up the metadata and passing to pyiceberg/pyarrow.
        GlueIcebergTable.from_table_info(glue_catalog, table_info)


def test_glue_iceberg_table_from_table_info_2(glue_catalog):
    table_info = {
        "Name": "test_iceberg_table",
        "Parameters": {},  # !! ERR !! no table_type
    }
    with pytest.raises(ValueError, match="missing the required 'table_type' parameter"):
        GlueIcebergTable.from_table_info(glue_catalog, table_info)


def test_glue_iceberg_table_from_table_info_3(glue_catalog):
    table_info = {
        "Name": "test_iceberg_table",
        "Parameters": {
            "table_type": "delta",
        },
    }
    with pytest.raises(ValueError, match="expected 'ICEBERG'"):
        GlueIcebergTable.from_table_info(glue_catalog, table_info)


###
# schema tests
###


def test_convert_glue_schema():
    columns = [
        {"Name": "bool_col", "Type": "boolean"},
        {"Name": "byte_col", "Type": "byte"},
        {"Name": "short_col", "Type": "short"},
        {"Name": "int_col", "Type": "integer"},
        {"Name": "long_col", "Type": "long"},
        {"Name": "bigint_col", "Type": "bigint"},
        {"Name": "float_col", "Type": "float"},
        {"Name": "double_col", "Type": "double"},
        {"Name": "decimal_col", "Type": "decimal"},
        {"Name": "string_col", "Type": "string"},
        {"Name": "timestamp_col", "Type": "timestamp"},
        {"Name": "date_col", "Type": "date"},
    ]

    # Test schema conversion
    assert _convert_glue_schema(columns) == Schema.from_pydict(
        {
            "bool_col": DataType.bool(),
            "byte_col": DataType.int8(),
            "short_col": DataType.int16(),
            "int_col": DataType.int32(),
            "long_col": DataType.int64(),
            "bigint_col": DataType.int64(),
            "float_col": DataType.float32(),
            "double_col": DataType.float64(),
            "decimal_col": DataType.decimal128(precision=38, scale=18),
            "string_col": DataType.string(),
            "timestamp_col": DataType.timestamp(timeunit="us", timezone="UTC"),
            "date_col": DataType.date(),
        }
    )
