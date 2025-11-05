from __future__ import annotations

import pytest

import daft
from daft.catalog import Catalog
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import TEST_TABLE_NAME


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
    # Should at least contain 'public' schema
    assert any(ns[0] == "public" for ns in namespaces)


@pytest.mark.integration()
def test_postgres_catalog_list_tables(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    tables = catalog.list_tables()
    # Should contain our test table
    assert any(str(table) == f"public.{TEST_TABLE_NAME}" for table in tables)


@pytest.mark.integration()
def test_postgres_catalog_has_table(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)

    # Should have our test table
    assert catalog.has_table(f"public.{TEST_TABLE_NAME}")
    assert catalog.has_table(TEST_TABLE_NAME)  # Should work without schema too

    # Should not have a non-existent table
    assert not catalog.has_table("non_existent_table")


@pytest.mark.integration()
def test_postgres_catalog_get_table(test_db, pdf) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    table = catalog.get_table(f"public.{TEST_TABLE_NAME}")

    # Should be able to read from the table
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

    # Test with limit
    df = table.read(limit=10)
    assert len(df.collect()) == 10

    # Test with partition
    df = table.read(partition_col="id", num_partitions=2)
    assert df.num_partitions() == 2
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
def test_postgres_catalog_create_and_drop_schema(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_schema = "test_catalog_schema"

    try:
        # Create schema
        catalog.create_namespace(test_schema)
        assert catalog.has_namespace(test_schema)

        # Should be in the list of namespaces
        namespaces = catalog.list_namespaces()
        assert any(ns[0] == test_schema for ns in namespaces)

    finally:
        # Clean up
        try:
            catalog.drop_namespace(test_schema)
        except Exception:
            pass  # Schema might not exist


@pytest.mark.integration()
def test_postgres_catalog_create_and_drop_table(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_catalog_table"

    # Create a simple schema for testing
    schema = daft.Schema.from_dict(
        {
            "id": daft.DataType.int64(),
            "name": daft.DataType.string(),
            "value": daft.DataType.float64(),
        }
    )

    try:
        # Create table
        table = catalog.create_table(f"public.{test_table}", schema)
        assert catalog.has_table(f"public.{test_table}")
        assert table.name == test_table

        # Should be in the list of tables
        tables = catalog.list_tables()
        assert any(str(t) == f"public.{test_table}" for t in tables)

        # Test reading from empty table
        df = table.read()
        assert len(df.collect()) == 0

    finally:
        # Clean up
        try:
            catalog.drop_table(f"public.{test_table}")
        except Exception:
            pass  # Table might not exist


@pytest.mark.integration()
def test_postgres_table_append_and_overwrite(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_append_overwrite_table"

    # Create a simple schema for testing
    schema = daft.Schema.from_dict(
        {
            "id": daft.DataType.int64(),
            "name": daft.DataType.string(),
        }
    )

    try:
        # Create table
        table = catalog.create_table(f"public.{test_table}", schema)

        # Create test data
        test_df = daft.from_pydict(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Test append
        table.append(test_df)
        result_df = table.read()
        assert len(result_df.collect()) == 3

        # Test overwrite
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
        assert result["name"].to_pylist() == ["David", "Eve"]

    finally:
        # Clean up
        try:
            catalog.drop_table(f"public.{test_table}")
        except Exception:
            pass  # Table might not exist


@pytest.mark.integration()
def test_postgres_table_with_schema_qualified_names(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_schema = "test_qualified_schema"
    test_table = "test_qualified_table"

    # Create a simple schema for testing
    table_schema = daft.Schema.from_dict(
        {
            "id": daft.DataType.int64(),
            "data": daft.DataType.string(),
        }
    )

    try:
        # Create schema
        catalog.create_namespace(test_schema)

        # Create table with qualified name
        qualified_table = catalog.create_table(f"{test_schema}.{test_table}", table_schema)

        # Test operations with qualified names
        assert catalog.has_table(f"{test_schema}.{test_table}")
        assert qualified_table.name == test_table

        # Test reading
        df = qualified_table.read()
        assert len(df.collect()) == 0

    finally:
        # Clean up
        try:
            catalog.drop_table(f"{test_schema}.{test_table}")
            catalog.drop_namespace(test_schema)
        except Exception:
            pass  # Might not exist


@pytest.mark.integration()
def test_postgres_catalog_error_handling(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)

    # Test getting non-existent table
    with pytest.raises(Exception):  # Should raise NotFoundError
        catalog.get_table("non_existent_schema.non_existent_table")

    # Test dropping non-existent table
    with pytest.raises(Exception):  # Should raise NotFoundError
        catalog.drop_table("non_existent_table")

    # Test dropping non-existent schema
    with pytest.raises(Exception):  # Should raise NotFoundError
        catalog.drop_namespace("non_existent_schema")


@pytest.mark.integration()
def test_postgres_table_with_different_data_types(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_data_types_table"

    # Create schema with various data types
    schema = daft.Schema.from_dict(
        {
            "id": daft.DataType.int64(),
            "name": daft.DataType.string(),
            "active": daft.DataType.bool(),
            "score": daft.DataType.float64(),
            "tags": daft.DataType.list(daft.DataType.string()),
        }
    )

    try:
        # Create table
        table = catalog.create_table(f"public.{test_table}", schema)

        # Create test data with various types
        test_df = daft.from_pydict(
            {
                "id": [1, 2],
                "name": ["Test1", "Test2"],
                "active": [True, False],
                "score": [95.5, 87.2],
                "tags": [["tag1", "tag2"], ["tag3"]],
            }
        )

        # Test append and read
        table.append(test_df)
        result_df = table.read()
        result = result_df.collect()

        assert len(result) == 2
        assert result["name"].to_pylist() == ["Test1", "Test2"]
        assert result["active"].to_pylist() == [True, False]

    finally:
        # Clean up
        try:
            catalog.drop_table(f"public.{test_table}")
        except Exception:
            pass  # Table might not exist
