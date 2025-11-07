from __future__ import annotations

import datetime
import decimal
import json
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest
from sqlalchemy import create_engine, text

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
@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
def test_postgres_table_with_different_data_types(test_db, write_mode) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

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
                # Fixed Size Binary
                ("fixed_size_binary_col", daft.DataType.fixed_size_binary(12)),
                # Temporal types
                ("date_col", daft.DataType.date()),
                ("timestamp_col", daft.DataType.timestamp(timeunit="ns")),
                ("timestamp_tz_col", daft.DataType.timestamp(timeunit="ns", timezone="UTC")),
                ("time_col", daft.DataType.time(timeunit="ns")),
                # List types
                ("list_string_col", daft.DataType.list(daft.DataType.string())),
                ("list_int_col", daft.DataType.list(daft.DataType.int32())),
                ("fixed_size_list_col", daft.DataType.fixed_size_list(daft.DataType.float32(), 3)),
                # TODO(desmond): Support nested lists.
                ("list_of_list_col", daft.DataType.list(daft.DataType.list(daft.DataType.int32()))),
                # Struct
                ("struct_col", daft.DataType.struct({"a": daft.DataType.int32(), "b": daft.DataType.string()})),
                (
                    "nested_struct_col",
                    daft.DataType.struct(
                        {"nested": daft.DataType.struct({"a": daft.DataType.int32(), "b": daft.DataType.string()})}
                    ),
                ),
                # Extension (will use jsonb)
                ("extension_col", daft.DataType.extension("custom_type", daft.DataType.string(), "custom")),
                # Image types
                ("image_col", daft.DataType.image()),
                ("fixed_shape_image_col", daft.DataType.image("RGB", 10, 10)),
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

        catalog = Catalog.from_postgres(test_db, extensions=["vector"])
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
            "fixed_size_binary_col": [b"binary_data1", b"binary_data2"],
            # Temporal types
            "date_col": [datetime.date(2023, 1, 1), datetime.date(2023, 2, 1)],
            "timestamp_col": [datetime.datetime(2023, 1, 1, 12, 0, 0), datetime.datetime(2023, 2, 1, 13, 0, 0)],
            "timestamp_tz_col": [
                datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime(2023, 2, 1, 13, 0, 0, tzinfo=datetime.timezone.utc),
            ],
            "time_col": [datetime.time(12, 30, 45), datetime.time(13, 45, 30)],
            # List types
            "list_string_col": [["a", "b"], ["c", "d"]],
            "list_int_col": [[1, 2, 3], [4, 5]],
            "fixed_size_list_col": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
            "list_of_list_col": [[[1.0, 2.0, 3.0]], [[4.0, 5.0, 6.0]]],
            # Struct
            "struct_col": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
            "nested_struct_col": [{"nested": {"a": 1, "b": "x"}}, {"nested": {"a": 2, "b": "y"}}],
            # Extension
            "extension_col": [{"type": "custom", "value": "value1"}, {"type": "custom", "value": "value2"}],
            # Image types
            # "image_col": [_create_fake_image(), _create_fake_image()],
            # "fixed_shape_image_col": [_create_fake_image(width=10, height=10, mode="RGB"), _create_fake_image(width=10, height=10, mode="RGB")],
            # Embedding
            "embedding_col": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
            # Tensor types
            # "tensor_col": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
            # "fixed_shape_tensor_col": [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]],
            # "sparse_tensor_col": [{"indices": [0, 1], "values": [1.0, 2.0]}, {"indices": [2, 3], "values": [3.0, 4.0]}],
            # "fixed_shape_sparse_tensor_col": [
            #     {"indices": [0, 1], "values": [1.0, 2.0]},
            #     {"indices": [2, 3], "values": [3.0, 4.0]},
            # ],
            # # Python
            # "python_col": [daft.lit(1), daft.lit(2)],
            # Decimal
            "decimal_col": [decimal.Decimal("123.45"), decimal.Decimal("678.90")],
            # Null
            "null_col": [None, None],
        }

        # Create DataFrame and cast columns to match the schema
        test_df = daft.from_pydict(test_data)

        # Cast columns to match the schema types
        test_df = test_df.with_column("int8_col", daft.col("int8_col").cast(daft.DataType.int8()))
        test_df = test_df.with_column("int16_col", daft.col("int16_col").cast(daft.DataType.int16()))
        test_df = test_df.with_column("int32_col", daft.col("int32_col").cast(daft.DataType.int32()))
        test_df = test_df.with_column("int64_col", daft.col("int64_col").cast(daft.DataType.int64()))
        test_df = test_df.with_column("uint8_col", daft.col("uint8_col").cast(daft.DataType.uint8()))
        test_df = test_df.with_column("uint16_col", daft.col("uint16_col").cast(daft.DataType.uint16()))
        test_df = test_df.with_column("uint32_col", daft.col("uint32_col").cast(daft.DataType.uint32()))
        test_df = test_df.with_column("uint64_col", daft.col("uint64_col").cast(daft.DataType.uint64()))
        test_df = test_df.with_column("float32_col", daft.col("float32_col").cast(daft.DataType.float32()))
        test_df = test_df.with_column("float64_col", daft.col("float64_col").cast(daft.DataType.float64()))
        test_df = test_df.with_column(
            "fixed_size_binary_col", daft.col("fixed_size_binary_col").cast(daft.DataType.fixed_size_binary(12))
        )
        test_df = test_df.with_column(
            "list_int_col", daft.col("list_int_col").cast(daft.DataType.list(daft.DataType.int32()))
        )
        test_df = test_df.with_column(
            "fixed_size_list_col",
            daft.col("fixed_size_list_col").cast(daft.DataType.fixed_size_list(daft.DataType.float32(), 3)),
        )
        test_df = test_df.with_column(
            "embedding_col", daft.col("embedding_col").cast(daft.DataType.embedding(daft.DataType.float32(), 3))
        )
        test_df = test_df.with_column(
            "struct_col",
            daft.col("struct_col").cast(
                daft.DataType.struct({"a": daft.DataType.int32(), "b": daft.DataType.string()})
            ),
        )
        test_df = test_df.with_column(
            "nested_struct_col",
            daft.col("nested_struct_col").cast(
                daft.DataType.struct(
                    {"nested": daft.DataType.struct({"a": daft.DataType.int32(), "b": daft.DataType.string()})}
                )
            ),
        )

        table.write(test_df, mode=write_mode)
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
        assert result_dict["fixed_size_binary_col"] == [b"binary_data1", b"binary_data2"]

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
        # TODO(desmond): Support nested lists.
        # assert result_dict["list_of_list_col"] == [[[1.0, 2.0, 3.0]], [[4.0, 5.0, 6.0]]]

        # Struct
        # Structs are stored as JSONB data which come back as double-encoded JSON strings, so we need to parse them twice.
        parsed_struct_col = [
            json.loads(json.loads(item)) if isinstance(item, str) else item for item in result_dict["struct_col"]
        ]
        assert parsed_struct_col == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

        # Nested Struct
        parsed_nested_struct_col = [
            json.loads(json.loads(item)) if isinstance(item, str) else item for item in result_dict["nested_struct_col"]
        ]
        assert parsed_nested_struct_col == [{"nested": {"a": 1, "b": "x"}}, {"nested": {"a": 2, "b": "y"}}]

        # Embedding
        assert [list(embedding) for embedding in result_dict["embedding_col"]] == [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]

        # Decimal
        assert result_dict["decimal_col"] == [decimal.Decimal("123.45"), decimal.Decimal("678.90")]

        # Null
        assert result_dict["null_col"] == [None, None]

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")


@pytest.mark.integration()
@pytest.mark.parametrize(
    "extensions",
    [
        ["vector"],
        None,
    ],
)
def test_postgres_table_with_embedding_columns(test_db, extensions) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    if extensions is None:
        # Pgvector extension is should be installed by default if available.
        catalog = Catalog.from_postgres(test_db)
    else:
        catalog = Catalog.from_postgres(test_db, extensions=extensions)
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
        assert list(embeddings[0]) == [1.0, 2.0, 3.0]
        assert list(embeddings[1]) == [4.0, 5.0, 6.0]
        assert list(embeddings[2]) == [7.0, 8.0, 9.0]

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
        assert list(new_embeddings[0]) == [10.0, 11.0, 12.0]
        assert list(new_embeddings[1]) == [13.0, 14.0, 15.0]

    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")


@pytest.mark.skip(reason="Skipping test for now until we have a better way to handle map columns")
@pytest.mark.integration()
def test_postgres_table_with_map_columns(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db, extensions=["vector"])
    test_table = "test_maps"

    try:
        schema = daft.Schema._from_field_name_and_types(
            [
                ("a", daft.DataType.int64()),
                ("map_col", daft.DataType.map(daft.DataType.string(), daft.DataType.int32())),
            ]
        )

        table = catalog.create_table(f"public.{test_table}", schema)
        assert catalog.has_table(f"public.{test_table}")
        assert table.name == test_table

        pa_array = pa.array([[("a", 1)], [], [("b", 2)]], type=pa.map_(pa.string(), pa.int64()))
        df = daft.from_arrow(pa.table({"map_col": pa_array}))
        df = df.with_column("a", df["map_col"].map_get("a"))
        table.append(df)

        result_df = table.read()
        result = result_df.collect()

        assert len(result) == 3
        assert result.to_pydict()["a"] == [1, None, None]
        # The map column should contain the original data: [{"a": 1}], [], [{"b": 2}]
        assert result.to_pydict()["map_col"] == [{"a": 1}, {}, {"b": 2}]
    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")


def _check_rls_enabled(test_db: str, schema_name: str, table_name: str) -> bool:
    engine = create_engine(test_db)
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT relrowsecurity FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = :table_name AND n.nspname = :schema_name
            """),
            {"table_name": table_name, "schema_name": schema_name},
        ).fetchone()
        return result[0] if result else False


@pytest.mark.integration()
@pytest.mark.parametrize("enable_rls", [None, True, False])
def test_postgres_catalog_create_table_rls_defaults(test_db, enable_rls) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    catalog = Catalog.from_postgres(test_db)
    test_table = "test_rls_table"

    try:
        df = daft.from_pydict(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        if enable_rls is None:
            catalog.create_table(f"public.{test_table}", df)
            assert _check_rls_enabled(test_db, "public", test_table)
        else:
            catalog.create_table(f"public.{test_table}", df, properties={"enable_rls": enable_rls})
            assert _check_rls_enabled(test_db, "public", test_table) == enable_rls
    finally:
        if catalog.has_table(f"public.{test_table}"):
            catalog.drop_table(f"public.{test_table}")
