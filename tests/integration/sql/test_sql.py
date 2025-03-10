from __future__ import annotations

import datetime
import math

import pandas as pd
import pytest
import sqlalchemy

import daft
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import EMPTY_TEST_TABLE_NAME, TEST_TABLE_NAME


@pytest.fixture(scope="session")
def pdf(test_db):
    return pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)


@pytest.mark.integration()
def test_sql_show(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df.show()


@pytest.mark.integration()
def test_sql_create_dataframe_ok(test_db, pdf) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 3, 4])
@pytest.mark.parametrize("partition_bound_strategy", ["min-max", "percentile"])
def test_sql_partitioned_read(test_db, num_partitions, partition_bound_strategy, pdf) -> None:
    row_size_bytes = daft.from_pandas(pdf).schema().estimate_row_size_bytes()
    num_rows_per_partition = len(pdf) / num_partitions
    with daft.execution_config_ctx(
        read_sql_partition_size_bytes=math.ceil(row_size_bytes * num_rows_per_partition),
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_sql(
            f"SELECT * FROM {TEST_TABLE_NAME}",
            test_db,
            partition_col="id",
            partition_bound_strategy=partition_bound_strategy,
        )
        assert df.num_partitions() == num_partitions
        assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2, 3, 4])
@pytest.mark.parametrize("partition_col", ["id", "float_col", "date_col", "date_time_col"])
@pytest.mark.parametrize("partition_bound_strategy", ["min-max", "percentile"])
def test_sql_partitioned_read_with_custom_num_partitions_and_partition_col(
    test_db, num_partitions, partition_col, partition_bound_strategy, pdf
) -> None:
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_sql(
            f"SELECT * FROM {TEST_TABLE_NAME}",
            test_db,
            partition_col=partition_col,
            num_partitions=num_partitions,
            partition_bound_strategy=partition_bound_strategy,
        )
        assert df.num_partitions() == num_partitions
        assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [0, 1, 2])
@pytest.mark.parametrize("partition_col", ["id", "string_col"])
def test_sql_partitioned_read_on_empty_table(empty_test_db, num_partitions, partition_col) -> None:
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_sql(
            f"SELECT * FROM {EMPTY_TEST_TABLE_NAME}",
            empty_test_db,
            partition_col=partition_col,
            num_partitions=num_partitions,
            schema={"id": daft.DataType.int64(), "string_col": daft.DataType.string()},
        )
        assert df.num_partitions() == 1
        empty_pdf = pd.read_sql_query(
            f"SELECT * FROM {EMPTY_TEST_TABLE_NAME}",
            empty_test_db,
            dtype={"id": "int64", "string_col": "str"},
        )
        assert_df_equals(df.to_pandas(), empty_pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2, 3, 4])
def test_sql_partitioned_read_with_non_uniformly_distributed_column(test_db, num_partitions, pdf) -> None:
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_sql(
            f"SELECT * FROM {TEST_TABLE_NAME}",
            test_db,
            partition_col="non_uniformly_distributed_col",
            num_partitions=num_partitions,
        )
        assert df.num_partitions() == num_partitions
        assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("partition_col", ["string_col", "null_col"])
def test_sql_partitioned_read_with_non_partionable_column(test_db, partition_col) -> None:
    with pytest.raises(ValueError, match="Failed to get partition bounds"):
        df = daft.read_sql(
            f"SELECT * FROM {TEST_TABLE_NAME}",
            test_db,
            partition_col=partition_col,
            num_partitions=2,
        )
        df = df.collect()


@pytest.mark.integration()
def test_sql_read_with_partition_num_without_partition_col(test_db) -> None:
    with pytest.raises(ValueError, match="Failed to execute sql"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, num_partitions=2)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "operator",
    ["<", ">", "=", "!=", ">=", "<="],
)
@pytest.mark.parametrize(
    "column, value",
    [
        ("id", 100),
        ("float_col", 100.0123),
        ("string_col", "row_100"),
        ("bool_col", True),
        ("date_col", datetime.date(2021, 1, 1)),
        ("date_time_col", datetime.datetime(2020, 1, 1, 10, 0, 0)),
    ],
)
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_binary_filter_pushdowns(test_db, column, operator, value, num_partitions, pdf) -> None:
    # Skip invalid comparisons for bool_col
    if column == "bool_col" and operator not in ("=", "!="):
        pytest.skip(f"Operator {operator} not valid for bool_col")

    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )

    if operator == ">":
        df = df.where(df[column] > value)
        pdf = pdf[pdf[column] > value]
    elif operator == "<":
        df = df.where(df[column] < value)
        pdf = pdf[pdf[column] < value]
    elif operator == "=":
        df = df.where(df[column] == value)
        pdf = pdf[pdf[column] == value]
    elif operator == "!=":
        df = df.where(df[column] != value)
        pdf = pdf[pdf[column] != value]
    elif operator == ">=":
        df = df.where(df[column] >= value)
        pdf = pdf[pdf[column] >= value]
    elif operator == "<=":
        df = df.where(df[column] <= value)
        pdf = pdf[pdf[column] <= value]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_is_null_filter_pushdowns(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.where(df["null_col"].is_null())

    pdf = pdf[pdf["null_col"].isnull()]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_not_null_filter_pushdowns(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.where(df["null_col"].not_null())

    pdf = pdf[pdf["null_col"].notnull()]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_non_pushdowned_predicate(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )

    # If_else is not supported as a pushdown to read_sql, but it should still work
    df = df.where((df["id"] > 100).if_else(df["float_col"] > 150, df["float_col"] < 50))

    pdf = pdf[(pdf["id"] > 100) & (pdf["float_col"] > 150) | (pdf["float_col"] < 50)]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_is_in_filter_pushdown(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.where(df["id"].is_in([1, 2, 3]))

    pdf = pdf[pdf["id"].isin([1, 2, 3])]
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_all_pushdowns(test_db, num_partitions) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.where(~(df["id"] < 1))
    df = df.where(df["string_col"].is_in([f"row_{i}" for i in range(10)]))
    df = df.select(df["id"], df["float_col"], df["string_col"])
    df = df.limit(5)

    df = df.collect()
    assert len(df) == 5
    assert df.column_names == ["id", "float_col", "string_col"]

    pydict = df.to_pydict()
    assert all(i >= 1 for i in pydict["id"])
    assert all(s in [f"row_{i}" for i in range(10)] for s in pydict["string_col"])


@pytest.mark.integration()
@pytest.mark.parametrize("limit", [0, 1, 10, 100, 200])
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_limit_pushdown(test_db, limit, num_partitions) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.limit(limit)

    df = df.collect()
    assert len(df) == limit


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_projection_pushdown(test_db, generated_data, num_partitions) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.select(df["id"], df["string_col"])

    df = df.collect()
    assert df.column_names == ["id", "string_col"]
    assert len(df) == len(generated_data)


@pytest.mark.integration()
def test_postgres_read_with_posix_operators(test_db) -> None:
    if not test_db.startswith("postgres"):
        pytest.skip("Skipping test for non-PostgreSQL databases")

    # regex to match on strings that end with 0
    df = daft.read_sql(
        f"SELECT id, string_col FROM {TEST_TABLE_NAME} WHERE string_col ~ '0$'",
        test_db,
    )
    df = df.collect()
    assert df.column_names == ["id", "string_col"]
    # There's 200 rows from row_0 to row_199, so there's 20 rows that end with 0
    assert len(df) == 20


@pytest.mark.integration()
def test_sql_read_without_schema_inference(test_db, generated_data) -> None:
    schema = {
        "id": daft.DataType.int32(),
        "float_col": daft.DataType.float64(),
        "string_col": daft.DataType.string(),
        "bool_col": daft.DataType.bool(),
        "date_col": daft.DataType.date(),
        "date_time_col": daft.DataType.timestamp(timeunit="ns"),
        "null_col": daft.DataType.null(),
        "non_uniformly_distributed_col": daft.DataType.int32(),
    }
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, infer_schema=False, schema=schema)
    df = df.collect()

    assert df.column_names == list(schema.keys())
    for field in df.schema():
        assert field.dtype == schema[field.name]

    assert len(df) == len(generated_data)


@pytest.mark.integration()
def test_sql_read_with_schema_inference_and_provided_schema(test_db, pdf) -> None:
    schema = {
        "id": daft.DataType.string(),  # Should be inferred as int32 but overridden to string
    }
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, infer_schema=True, schema=schema)
    df = df.collect()

    assert df.schema()["id"].dtype == daft.DataType.string()
    pdf = pdf.astype({"id": str})
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
def test_sql_read_with_custom_infer_schema_length(test_db, pdf) -> None:
    # Use sql alchemy to test inference length because connectorx does not use inference length
    def create_conn():
        return sqlalchemy.create_engine(test_db).connect()

    # The null column's first value is None, so the inferred schema should be null
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        create_conn,
        infer_schema=True,
        infer_schema_length=1,
    )
    df = df.collect()

    assert df.schema()["null_col"].dtype == daft.DataType.null()
    assert len(df) == len(pdf)

    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        create_conn,
        infer_schema=True,
        infer_schema_length=2,
    )
    df = df.collect()
    assert df.schema()["null_col"].dtype == daft.DataType.string()
    assert len(df) == len(pdf)
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
def test_sql_with_infer_schema_false_and_no_schema_provided(test_db) -> None:
    with pytest.raises(
        ValueError,
        match="Cannot read DataFrame with infer_schema=False and schema=None, please provide a schema or set infer_schema=True",
    ):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, infer_schema=False)


@pytest.mark.integration()
def test_sql_bad_url() -> None:
    with pytest.raises(RuntimeError, match="Failed to execute sql"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", "bad_url://")


@pytest.mark.integration()
def test_sql_connection_factory_ok(test_db, pdf) -> None:
    def create_conn():
        return sqlalchemy.create_engine(test_db).connect()

    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", create_conn)
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
def test_sql_bad_connection_factory() -> None:
    with pytest.raises(ValueError):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", lambda: "bad_conn")


@pytest.mark.integration()
def test_sql_unsupported_dialect() -> None:
    with pytest.raises(ValueError, match="Unsupported dialect"):
        daft.read_sql(
            f"SELECT * FROM {TEST_TABLE_NAME}",
            "sqlheavy://user:password@localhost:5432/db",
        )
