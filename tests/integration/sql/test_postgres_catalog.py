from __future__ import annotations

import datetime
import decimal
from typing import Any

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

    try:
        assert not catalog.has_namespace(test_schema)
        catalog.create_namespace(test_schema)
        assert catalog.has_namespace(test_schema)

        namespaces = catalog.list_namespaces()
        assert any(ns[0] == test_schema for ns in namespaces)
    finally:
        try:
            if catalog.has_namespace(test_schema):
                catalog.drop_namespace(test_schema)
        except Exception:
            pass


@pytest.mark.integration()
def test_postgres_catalog_create_and_drop_empty_table(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_catalog_table"

    try:
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

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")
        assert not catalog.has_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_catalog_create_and_drop_table_with_data(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_catalog_table"

    try:
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

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")
        assert not catalog.has_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_catalog_create_table_if_not_exists(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_create_table_if_not_exists_table"

    try:
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

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_catalog_create_table_with_options(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_create_table_with_options_table"

    try:
        schema = daft.Schema._from_field_name_and_types(
            [
                ("id", daft.DataType.int64()),
                ("name", daft.DataType.string()),
            ]
        )

        table = catalog.create_table(f"public.{test_table}", schema, properties={"enable_rls": True})
        assert catalog.has_table(f"public.{test_table}")
        assert table.name == test_table

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_table_append_and_overwrite(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_append_overwrite_table"

    try:
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

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_table_with_schema_qualified_names(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_schema = "test_qualified_schema"
    test_table = "test_qualified_table"

    try:
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

    finally:
        try:
            if catalog.has_table(f"{test_schema}.{test_table}"):
                catalog.drop_table(f"{test_schema}.{test_table}")
        except Exception:
            pass
        try:
            if catalog.has_namespace(test_schema):
                catalog.drop_namespace(test_schema)
        except Exception:
            pass


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

    try:
        # Test all supported Daft data types
        schema = daft.Schema._from_field_name_and_types(
            [
                # Integer types
                ("int8_col", daft.DataType.int8()),
                ("int16_col", daft.DataType.int16()),
                ("int32_col", daft.DataType.int32()),
                ("int64_col", daft.DataType.int64()),
                ("uint8_col", daft.DataType.uint8()),
                ("uint16_col", daft.DataType.uint16()),
                ("uint32_col", daft.DataType.uint32()),
                ("uint64_col", daft.DataType.uint64()),
                # Float types
                ("float32_col", daft.DataType.float32()),
                ("float64_col", daft.DataType.float64()),
                # Boolean
                ("bool_col", daft.DataType.bool()),
                # String
                ("string_col", daft.DataType.string()),
                # Binary
                ("binary_col", daft.DataType.binary()),
                # Temporal types
                ("date_col", daft.DataType.date()),
                ("timestamp_col", daft.DataType.timestamp(timeunit="ns")),
                ("timestamp_tz_col", daft.DataType.timestamp(timeunit="ns", timezone="UTC")),
                ("time_col", daft.DataType.time(timeunit="ns")),
                ("duration_col", daft.DataType.duration(timeunit="ns")),
                ("interval_col", daft.DataType.interval()),
                # List types
                ("list_string_col", daft.DataType.list(daft.DataType.string())),
                ("list_int_col", daft.DataType.list(daft.DataType.int32())),
                ("fixed_size_list_col", daft.DataType.fixed_size_list(daft.DataType.float32(), 3)),
                # Struct
                ("struct_col", daft.DataType.struct({"a": daft.DataType.int32(), "b": daft.DataType.string()})),
                # Map
                ("map_col", daft.DataType.map(daft.DataType.string(), daft.DataType.int32())),
                # Extension (will use jsonb)
                ("extension_col", daft.DataType.extension("custom_type", daft.DataType.string(), "custom")),
                # Image types
                ("image_col", daft.DataType.image()),
                ("fixed_shape_image_col", daft.DataType.fixed_shape_image("RGB", 10, 10)),
                # Embedding
                ("embedding_col", daft.DataType.embedding(daft.DataType.float32(), 3)),
                # Tensor types
                ("tensor_col", daft.DataType.tensor(daft.DataType.float32())),
                ("fixed_shape_tensor_col", daft.DataType.tensor(daft.DataType.float32(), shape=(2, 3))),
                ("sparse_tensor_col", daft.DataType.sparse_tensor(daft.DataType.float32())),
                ("fixed_shape_sparse_tensor_col", daft.DataType.sparse_tensor(daft.DataType.float32(), shape=(2, 3))),
                # Python
                ("python_col", daft.DataType.python()),
                # Decimal
                ("decimal_col", daft.DataType.decimal128(10, 2)),
                # Null
                ("null_col", daft.DataType.null()),
            ]
        )

        table = catalog.create_table(f"public.{test_table}", schema)

        # Create test data for all types
        test_data: dict[str, list[Any]] = {
            # Integer types
            "int8_col": [1, 2],
            "int16_col": [100, 200],
            "int32_col": [1000, 2000],
            "int64_col": [10000, 20000],
            "uint8_col": [1, 2],
            "uint16_col": [100, 200],
            "uint32_col": [1000, 2000],
            "uint64_col": [10000, 20000],
            # Float types
            "float32_col": [1.5, 2.5],
            "float64_col": [1.123456789, 2.987654321],
            # Boolean
            "bool_col": [True, False],
            # String
            "string_col": ["hello", "world"],
            # Binary
            "binary_col": [b"binary_data1", b"binary_data2"],
            # Temporal types
            "date_col": [datetime.date(2023, 1, 1), datetime.date(2023, 2, 1)],
            "timestamp_col": [datetime.datetime(2023, 1, 1, 12, 0, 0), datetime.datetime(2023, 2, 1, 13, 0, 0)],
            "timestamp_tz_col": [
                datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime(2023, 2, 1, 13, 0, 0, tzinfo=datetime.timezone.utc),
            ],
            "time_col": [datetime.time(12, 30, 45), datetime.time(13, 45, 30)],
            "duration_col": [datetime.timedelta(days=1, hours=2), datetime.timedelta(days=2, hours=3)],
            "interval_col": [datetime.timedelta(days=1), datetime.timedelta(days=2)],
            # List types
            "list_string_col": [["a", "b"], ["c", "d"]],
            "list_int_col": [[1, 2, 3], [4, 5]],
            "fixed_size_list_col": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
            # Struct
            "struct_col": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
            # Map
            "map_col": [{"key1": 10, "key2": 20}, {"key3": 30}],
            # Extension (stored as jsonb)
            "extension_col": ["value1", "value2"],
            # Image types (stored as bytea)
            "image_col": [b"fake_image_data1", b"fake_image_data2"],
            "fixed_shape_image_col": [b"fake_rgb_image_data1", b"fake_rgb_image_data2"],
            # Embedding
            "embedding_col": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
            # Tensor types (stored as jsonb)
            "tensor_col": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
            "fixed_shape_tensor_col": [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]],
            "sparse_tensor_col": [{"indices": [0, 1], "values": [1.0, 2.0]}, {"indices": [2, 3], "values": [3.0, 4.0]}],
            "fixed_shape_sparse_tensor_col": [
                {"indices": [0, 1], "values": [1.0, 2.0]},
                {"indices": [2, 3], "values": [3.0, 4.0]},
            ],
            # Python (stored as jsonb)
            "python_col": [{"type": "custom", "value": 1}, {"type": "custom", "value": 2}],
            # Decimal
            "decimal_col": [decimal.Decimal("123.45"), decimal.Decimal("678.90")],
            # Null
            "null_col": [None, None],
        }

        # Create DataFrame and cast embedding column properly
        test_df = daft.from_pydict(test_data)
        test_df = test_df.with_column(
            "embedding_col", daft.col("embedding_col").cast(daft.DataType.embedding(daft.DataType.float32(), 3))
        )

        table.append(test_df)
        result_df = table.read()
        result = result_df.collect()

        # Verify we have the expected number of rows
        assert len(result) == 2

        # Basic type checks
        result_dict = result.to_pydict()
        assert result_dict["int64_col"] == [10000, 20000]
        assert result_dict["string_col"] == ["hello", "world"]
        assert result_dict["bool_col"] == [True, False]
        assert result_dict["float64_col"] == [1.123456789, 2.987654321]

        # Integer types
        assert result_dict["int8_col"] == [1, 2]
        assert result_dict["int16_col"] == [100, 200]
        assert result_dict["int32_col"] == [1000, 2000]
        assert result_dict["uint8_col"] == [1, 2]  # Stored as smallint
        assert result_dict["uint16_col"] == [100, 200]  # Stored as integer
        assert result_dict["uint32_col"] == [1000, 2000]  # Stored as bigint
        assert result_dict["uint64_col"] == [10000, 20000]  # Stored as bigint

        # Float types
        assert abs(result_dict["float32_col"][0] - 1.5) < 0.001
        assert abs(result_dict["float32_col"][1] - 2.5) < 0.001

        # Binary
        assert result_dict["binary_col"] == [b"binary_data1", b"binary_data2"]

        # Temporal types
        assert result_dict["date_col"] == [datetime.date(2023, 1, 1), datetime.date(2023, 2, 1)]
        assert result_dict["timestamp_col"] == [
            datetime.datetime(2023, 1, 1, 12, 0, 0),
            datetime.datetime(2023, 2, 1, 13, 0, 0),
        ]
        assert result_dict["time_col"] == [datetime.time(12, 30, 45), datetime.time(13, 45, 30)]

        # List types
        assert result_dict["list_string_col"] == [["a", "b"], ["c", "d"]]
        assert result_dict["list_int_col"] == [[1, 2, 3], [4, 5]]
        assert result_dict["fixed_size_list_col"] == [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]

        # Struct
        assert result_dict["struct_col"] == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

        # Map
        assert result_dict["map_col"] == [{"key1": 10, "key2": 20}, {"key3": 30}]

        # Embedding
        assert result_dict["embedding_col"] == [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
def test_postgres_catalog_with_embedding_columns(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db, extensions=["vector"])
    test_table = "test_pgvector_embeddings"

    try:
        schema = daft.Schema._from_field_name_and_types(
            [
                ("id", daft.DataType.int64()),
                ("text", daft.DataType.string()),
                ("embedding", daft.DataType.embedding(daft.DataType.float32(), 3)),
            ]
        )

        table = catalog.create_table(f"public.{test_table}", schema)
        assert catalog.has_table(f"public.{test_table}")
        assert table.name == test_table

        test_df = daft.from_pydict(
            {
                "id": [1, 2, 3],
                "text": ["hello", "world", "test"],
                "embedding": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
            }
        ).with_column("embedding", daft.col("embedding").cast(daft.DataType.embedding(daft.DataType.float32(), 3)))

        table.append(test_df)

        result_df = table.read()
        result = result_df.collect()

        assert len(result) == 3
        assert result.to_pydict()["id"] == [1, 2, 3]
        assert result.to_pydict()["text"] == ["hello", "world", "test"]

        embeddings = result.to_pydict()["embedding"]
        assert len(embeddings) == 3
        assert embeddings[0] == [1.0, 2.0, 3.0]
        assert embeddings[1] == [4.0, 5.0, 6.0]
        assert embeddings[2] == [7.0, 8.0, 9.0]

        new_df = daft.from_pydict(
            {
                "id": [10, 20],
                "text": ["new", "data"],
                "embedding": [[10.0, 11.0, 12.0], [13.0, 14.0, 15.0]],
            }
        ).with_column("embedding", daft.col("embedding").cast(daft.DataType.embedding(daft.DataType.float32(), 3)))
        table.overwrite(new_df)

        result_df = table.read()
        result = result_df.collect()

        assert len(result) == 2
        assert result.to_pydict()["id"] == [10, 20]
        assert result.to_pydict()["text"] == ["new", "data"]

        new_embeddings = result.to_pydict()["embedding"]
        assert new_embeddings[0] == [10.0, 11.0, 12.0]
        assert new_embeddings[1] == [13.0, 14.0, 15.0]

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")
