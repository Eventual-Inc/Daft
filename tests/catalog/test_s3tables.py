from __future__ import annotations

import os
from typing import TYPE_CHECKING

import boto3
import pytest
from moto import mock_aws

from daft import Catalog, Identifier
from daft.logical.schema import DataType as dt
from daft.logical.schema import Field, Schema

if TYPE_CHECKING:
    from mypy_boto3_s3tables import S3TablesClient
else:
    S3TablesClient = object


@pytest.mark.skip("skipped for integration testing")
def test_s3tables_iceberg_rest():
    import daft

    table_bucket_arn = "..."
    catalog = Catalog.from_s3tables(table_bucket_arn)
    print(catalog.list_tables("demo"))
    catalog.read_table("demo.points").show()
    catalog.write_table(
        "demo.points",
        daft.from_pydict(
            {
                "x": [True],
                "y": [4],
                "z": ["d"],
            }
        ),
    )
    catalog.read_table("demo.points").show()


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"


@pytest.fixture(scope="function")
def client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3tables", region_name="us-west-2")


@pytest.fixture(scope="function")
def catalog(client: S3TablesClient):
    res = client.create_table_bucket(name="test_bucket")
    arn = res["arn"]
    yield Catalog.from_s3tables(arn, client=client)


def schema(fields: dict[str, dt]) -> Schema:
    return Schema._from_fields([Field.create(k, v) for k, v in fields.items()])


def test_create_namespace(catalog: Catalog):
    namespace = "ns"
    catalog.create_namespace(namespace)
    namespaces = catalog.list_namespaces()
    assert Identifier("ns") in namespaces


def test_create_table(catalog: Catalog):
    ns = "create_table_ns"
    table_name = f"{ns}.T"
    catalog.create_namespace(ns)
    table = catalog.create_table(
        table_name,
        schema(
            {
                "x": dt.bool(),
                "y": dt.int64(),
                "z": dt.string(),
            }
        ),
    )
    tables = catalog.list_tables()
    assert len(tables) == 1
    assert Identifier.from_str(table_name) in tables
    assert table.name == "T"


def test_drop(catalog: Catalog):
    ns = "ns_to_drop"
    table_name = f"{ns}.table_to_drop"
    catalog.create_namespace(ns)
    catalog.create_table(table_name, schema({"a": dt.bool()}))
    #
    assert Identifier(ns) in catalog.list_namespaces()
    assert Identifier.from_str(table_name) in catalog.list_tables(ns)
    #
    catalog.drop_table(table_name)
    assert Identifier.from_str(table_name) not in catalog.list_tables(ns)
    #
    catalog.drop_namespace(ns)
    assert Identifier(ns) not in catalog.list_namespaces()


def test_get_table(catalog: Catalog):
    ns = "ns_to_get"
    table_name = f"{ns}.table_to_get"
    catalog.create_namespace(ns)
    catalog.create_table(table_name, schema({"a": dt.bool()}))
    # verify
    res = catalog.get_table(table_name)
    assert res.name == "table_to_get"


def test_list_namespaces(catalog: Catalog):
    namespaces = [
        Identifier("ns1_1"),
        Identifier("ns1_2"),
        Identifier("ns2_1"),
    ]
    for ns in namespaces:
        catalog.create_namespace(ns)
    #
    # list one
    res = catalog.list_namespaces("ns1_1")
    assert len(res) == 1
    assert Identifier("ns1_1") == res[0]
    #
    # list some
    res = catalog.list_namespaces("ns1_")
    assert len(res) == 2
    assert Identifier("ns1_1") in res
    assert Identifier("ns1_2") in res
    #
    # list all
    res = catalog.list_namespaces()
    for ns in namespaces:
        assert ns in res


def test_list_tables(catalog: Catalog):
    ns1 = "ns1"
    ns2 = "ns2"
    tables = [
        f"{ns1}.tbl1_1",
        f"{ns1}.tbl1_2",
        f"{ns1}.tbl2_1",
        f"{ns2}.tbl1_1",
        f"{ns2}.tbl1_2",
        f"{ns2}.tbl2_1",
    ]
    catalog.create_namespace(ns1)
    catalog.create_namespace(ns2)
    for table in tables:
        catalog.create_table(table, schema({}))
    #
    # list one
    res = catalog.list_tables(f"{ns1}.tbl1_1")
    assert len(res) == 1
    assert Identifier(ns1, "tbl1_1") == res[0]
    #
    # list some
    res = catalog.list_tables(ns1)
    assert len(res) == 3
    assert Identifier(ns1, "tbl1_1") in res
    assert Identifier(ns1, "tbl1_2") in res
    assert Identifier(ns1, "tbl2_1") in res
    #
    # list all
    res = catalog.list_tables()
    assert len(res) == len(tables)
    for table in tables:
        assert Identifier.from_str(table) in res


def test_existence_checks(catalog: Catalog):
    ns1 = "ns1"
    catalog.create_namespace(ns1)

    assert catalog.has_namespace(ns1)
    assert not catalog.has_namespace("does_not_exist")


@pytest.mark.skip("S3 Tables read tests are done via integration tests.")
def test_read_table():
    pass  # https://github.com/Eventual-Inc/Daft/issues/3925


@pytest.mark.skip("S3 Tables read tests are done via integration tests.")
def test_write_table():
    pass  # https://github.com/Eventual-Inc/Daft/issues/3925
