from __future__ import annotations

import pandas as pd
import pytest

import daft
from daft.catalog import Catalog, NotFoundError
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import TEST_TABLE_NAME


@pytest.fixture(scope="session")
def pdf(test_db):
    return pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)


@pytest.mark.integration()
def test_postgres_catalog_from_connection_string(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    assert catalog.name == "postgres"


@pytest.mark.integration()
def test_postgres_catalog_list_namespaces(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    namespaces = catalog.list_namespaces()
    # Should at least contain 'public' schema.
    assert any(ns[0] == "public" for ns in namespaces)


@pytest.mark.integration()
def test_postgres_catalog_list_tables(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    tables = catalog.list_tables()
    # Should contain our test table.
    assert any(str(table) == f"public.{TEST_TABLE_NAME}" for table in tables)


@pytest.mark.integration()
def test_postgres_catalog_has_table(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)

    # Should have our test table.
    assert catalog.has_table(f"public.{TEST_TABLE_NAME}")
    assert catalog.has_table(TEST_TABLE_NAME)  # Should work without schema too.

    # Should not have a non-existent table.
    assert not catalog.has_table("the_voices_in_my_head")


@pytest.mark.integration()
def test_postgres_catalog_get_table(test_db, pdf) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    table = catalog.get_table(f"public.{TEST_TABLE_NAME}")

    df = table.read()
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
def test_postgres_table_schema(test_db, pdf) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    table = catalog.get_table(f"public.{TEST_TABLE_NAME}")

    schema = table.schema()
    assert len(schema) == len(pdf.columns)
    assert all(field.name in pdf.columns for field in schema)


@pytest.mark.integration()
def test_postgres_table_read_with_options(test_db, pdf) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    table = catalog.get_table(f"public.{TEST_TABLE_NAME}")

    # Test with partition.
    df = table.read(partition_col="id", num_partitions=2)
    assert df.num_partitions() == 2
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
def test_postgres_catalog_create_and_drop_schema(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_schema = "test_catalog_schema"

    assert not catalog.has_namespace(test_schema)
    catalog.create_namespace(test_schema)
    assert catalog.has_namespace(test_schema)

    namespaces = catalog.list_namespaces()
    assert any(ns[0] == test_schema for ns in namespaces)
    catalog.drop_namespace(test_schema)
    assert not catalog.has_namespace(test_schema)


@pytest.mark.integration()
def test_postgres_catalog_create_and_drop_empty_table(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_catalog_table"

    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", daft.DataType.int64()),
            ("name", daft.DataType.string()),
            ("value", daft.DataType.float64()),
        ]
    )

    table = catalog.create_table(f"public.{test_table}", schema)
    assert catalog.has_table(f"public.{test_table}")
    assert table.name == test_table

    tables = catalog.list_tables()
    assert any(str(t) == f"public.{test_table}" for t in tables)

    df = table.read()
    assert len(df.collect()) == 0

    catalog.drop_table(f"public.{test_table}")
    assert not catalog.has_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_catalog_create_and_drop_table_with_data(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_catalog_table"

    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [1.0, 2.0, 3.0],
        }
    )

    table = catalog.create_table(f"public.{test_table}", df)
    assert catalog.has_table(f"public.{test_table}")
    assert table.name == test_table
    assert len(table.read().collect()) == 3

    catalog.drop_table(f"public.{test_table}")
    assert not catalog.has_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_catalog_create_table_if_not_exists(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_create_table_if_not_exists_table"

    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", daft.DataType.int64()),
            ("name", daft.DataType.string()),
        ]
    )

    assert not catalog.has_table(f"public.{test_table}")
    table = catalog.create_table_if_not_exists(f"public.{test_table}", schema)
    assert catalog.has_table(f"public.{test_table}")
    assert table.name == test_table
    table = catalog.create_table_if_not_exists(f"public.{test_table}", schema)
    assert catalog.has_table(f"public.{test_table}")
    assert table.name == test_table

    catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_catalog_create_table_with_options(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_create_table_with_options_table"

    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", daft.DataType.int64()),
            ("name", daft.DataType.string()),
        ]
    )

    table = catalog.create_table(f"public.{test_table}", schema, properties={"enable_rls": True})
    assert catalog.has_table(f"public.{test_table}")
    assert table.name == test_table

    catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_table_append_and_overwrite(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_append_overwrite_table"

    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", daft.DataType.int64()),
            ("name", daft.DataType.string()),
        ]
    )

    table = catalog.create_table(f"public.{test_table}", schema)
    test_df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    table.append(test_df)
    result_df = table.read()
    assert len(result_df.collect()) == 3

    new_df = daft.from_pydict(
        {
            "id": [4, 5],
            "name": ["David", "Eve"],
        }
    )
    table.overwrite(new_df)
    result_df = table.read()
    result = result_df.collect()
    assert len(result) == 2
    assert result.to_pydict()["name"] == ["David", "Eve"]

    catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_table_with_schema_qualified_names(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_schema = "test_qualified_schema"
    test_table = "test_qualified_table"

    table_schema = daft.Schema._from_field_name_and_types(
        [
            ("id", daft.DataType.int64()),
            ("data", daft.DataType.string()),
        ]
    )

    catalog.create_namespace(test_schema)

    qualified_table = catalog.create_table(f"{test_schema}.{test_table}", table_schema)

    assert catalog.has_table(f"{test_schema}.{test_table}")
    assert qualified_table.name == test_table

    df = qualified_table.read()
    assert len(df.collect()) == 0

    catalog.drop_table(f"{test_schema}.{test_table}")
    catalog.drop_namespace(test_schema)


@pytest.mark.integration()
def test_postgres_catalog_error_handling(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)

    with pytest.raises(NotFoundError):
        catalog.get_table("non_existent_schema.non_existent_table")

    with pytest.raises(NotFoundError):
        catalog.drop_table("non_existent_table")

    with pytest.raises(ValueError):
        catalog.drop_namespace("non_existent_schema")


@pytest.mark.integration()
def test_postgres_table_with_different_data_types(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_data_types_table"

    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", daft.DataType.int64()),
            ("name", daft.DataType.string()),
            ("active", daft.DataType.bool()),
            ("score", daft.DataType.float64()),
            ("tags", daft.DataType.list(daft.DataType.string())),
        ]
    )

    table = catalog.create_table(f"public.{test_table}", schema)

    test_df = daft.from_pydict(
        {
            "id": [1, 2],
            "name": ["Test1", "Test2"],
            "active": [True, False],
            "score": [95.5, 87.2],
            "tags": [["tag1", "tag2"], ["tag3"]],
        }
    )

    table.append(test_df)
    result_df = table.read()
    result = result_df.collect()

    assert len(result) == 2
    assert result.to_pydict()["name"] == ["Test1", "Test2"]
    assert result.to_pydict()["active"] == [True, False]

    catalog.drop_table(f"public.{test_table}")
