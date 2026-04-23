from __future__ import annotations

import os
from datetime import date, datetime

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

import daft

NUM_ROWS = 100
SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("date_col", pa.date32()),
        ("timestamp_col", pa.timestamp("ns")),
        ("value", pa.int64()),
        ("nullable_int", pa.int64()),
        ("int_col", pa.int64()),
        ("str_col", pa.large_string()),
        ("nullable_str", pa.large_string()),
    ]
)
SAMPLE_DATA = pa.table(
    {
        "id": range(100),
        "str_col": ["str" + str(i % 3) for i in range(100)],
        "int_col": [i % 3 for i in range(100)],
        # We use nonsensical coarse partitions for data and timestamp types to reduce test latency.
        "date_col": [date(2024, 1, i % 3 + 1) for i in range(100)],
        "timestamp_col": [datetime(2024, 1, i % 3 + 1, i % 3) for i in range(100)],
        "value": range(100),
        # Include some nulls.
        "nullable_str": [None if i % 10 == 1 else f"val{i % 3}" for i in range(100)],
        "nullable_int": [None if i % 7 == 1 else i % 3 for i in range(100)],
    },
    schema=SCHEMA,
)


def unify_timestamp(table):
    return table.set_column(
        table.schema.get_field_index("timestamp_col"), "timestamp_col", table["timestamp_col"].cast(pa.timestamp("us"))
    )


def assert_tables_equal(daft_recordbatch, pa_table):
    sorted_pa_table = pa_table.sort_by([("id", "ascending")]).select(SCHEMA.names)
    sorted_pa_table = unify_timestamp(sorted_pa_table)
    sorted_daft_recordbatch = daft_recordbatch.sort_by([("id", "ascending")]).select(SCHEMA.names)
    sorted_daft_recordbatch = unify_timestamp(sorted_daft_recordbatch)
    assert sorted_pa_table == sorted_daft_recordbatch


@pytest.mark.parametrize(
    "partition_by",
    [
        ["str_col"],
        ["int_col"],
        ["date_col"],
        ["timestamp_col"],
        ["nullable_str"],
        ["nullable_int"],
        ["str_col", "int_col"],  # Test multiple partition columns.
        ["nullable_str", "nullable_int"],  # Test multiple partition columns with nulls.
    ],
)
# Pyarrow does not currently support writing partitioned JSON.
@pytest.mark.parametrize("file_format", ["csv", "parquet"])
@pytest.mark.parametrize("filter", [True, False])
def test_hive_pyarrow_daft_compatibility(tmpdir, partition_by, file_format, filter):
    ds.write_dataset(
        SAMPLE_DATA,
        tmpdir,
        format=file_format,
        partitioning=ds.partitioning(pa.schema([SAMPLE_DATA.schema.field(col) for col in partition_by]), flavor="hive"),
    )

    glob_path = os.path.join(tmpdir, "**")
    daft_df = ()
    if file_format == "csv":
        daft_df = daft.read_csv(
            glob_path,
            schema={
                "nullable_str": daft.DataType.string(),
                "nullable_int": daft.DataType.int64(),
            },
            hive_partitioning=True,
        )

    if file_format == "json":
        daft_df = daft.read_json(
            glob_path,
            schema={
                "nullable_str": daft.DataType.string(),
                "nullable_int": daft.DataType.int64(),
            },
            hive_partitioning=True,
        )
    if file_format == "parquet":
        daft_df = daft.read_parquet(
            glob_path,
            schema={
                "nullable_str": daft.DataType.string(),
                "nullable_int": daft.DataType.int64(),
            },
            hive_partitioning=True,
        )
    pa_ds = ds.dataset(
        tmpdir,
        format=file_format,
        partitioning=ds.HivePartitioning.discover(),
        schema=SCHEMA,
    )
    pa_table = pa_ds.to_table()
    if filter:
        first_col = partition_by[0]
        sample_value = SAMPLE_DATA[first_col][0].as_py()
        daft_df = daft_df.where(daft.col(first_col) == sample_value)
        pa_table = pa_ds.to_table(filter=ds.field(first_col) == sample_value)
    assert_tables_equal(daft_df.to_arrow(), pa_table)


@pytest.mark.parametrize(
    "partition_by",
    [
        ["str_col"],
        ["int_col"],
        ["date_col"],
        ["timestamp_col"],
        ["nullable_str"],
        ["nullable_int"],
        ["str_col", "int_col"],  # Test multiple partition columns.
        ["nullable_str", "nullable_int"],  # Test multiple partition columns with nulls.
    ],
)
# TODO(desmond): Daft does not currently have a write_json API.
@pytest.mark.parametrize("file_format", ["csv", "parquet"])
@pytest.mark.parametrize("filter", [True, False])
def test_hive_daft_roundtrip(tmpdir, partition_by, file_format, filter):
    filepath = f"{tmpdir}"
    source = daft.from_arrow(SAMPLE_DATA)

    glob_path = os.path.join(tmpdir, "**")
    target = ()
    if file_format == "csv":
        source.write_csv(filepath, partition_cols=[daft.col(col) for col in partition_by])
        target = daft.read_csv(
            glob_path,
            schema={
                "nullable_str": daft.DataType.string(),
                "nullable_int": daft.DataType.int64(),
            },
            hive_partitioning=True,
        )
        # TODO(desmond): Daft has an inconsistency with handling null string columns when using
        # `from_arrow` vs `read_csv`. Only applies when nullable_str goes through CSV body;
        # as a partition col it round-trips correctly via hive directory names.
        if "nullable_str" not in partition_by:
            plain_filepath = f"{tmpdir}-plain"
            source.write_csv(plain_filepath)
            source = daft.read_csv(
                f"{plain_filepath}/**",
                schema={
                    "nullable_str": daft.DataType.string(),
                    "nullable_int": daft.DataType.int64(),
                },
            )
    if file_format == "json":
        source.write_json(filepath, partition_cols=[daft.col(col) for col in partition_by])
        target = daft.read_json(
            glob_path,
            schema={
                "nullable_str": daft.DataType.string(),
                "nullable_int": daft.DataType.int64(),
            },
            hive_partitioning=True,
        )
    if file_format == "parquet":
        source.write_parquet(filepath, partition_cols=[daft.col(col) for col in partition_by])
        target = daft.read_parquet(
            glob_path,
            schema={
                "nullable_str": daft.DataType.string(),
                "nullable_int": daft.DataType.int64(),
            },
            hive_partitioning=True,
        )
    if filter:
        first_col = partition_by[0]
        sample_value = SAMPLE_DATA[first_col][0].as_py()
        source = source.where(daft.col(first_col) == sample_value)
        target = target.where(daft.col(first_col) == sample_value)
    assert_tables_equal(target.to_arrow(), source.to_arrow())


# Regression tests for https://github.com/Eventual-Inc/Daft/issues/2111:
# partition values must live only in directory paths (Hive), not duplicated
# in file bodies, so pandas/pyarrow can read the dataset.


def _issue_2111_sample_pydict():
    import datetime

    return {
        "first_name": ["Ernesto", "Sari", "Wolfgang", "Jackie", "Zoya"],
        "last_name": ["Evergreen", "Salama", "Winter", "Jale", "Zee"],
        "age": [34, 57, 23, 62, 40],
        "DoB": [
            datetime.date(1990, 4, 3),
            datetime.date(1967, 1, 2),
            datetime.date(2001, 2, 12),
            datetime.date(1962, 3, 24),
            datetime.date(1984, 4, 7),
        ],
        "country": ["Canada", "United Kingdom", "Germany", "Canada", "United Kingdom"],
        "has_dog": [True, True, False, True, True],
    }


def test_partition_cols_not_duplicated_in_parquet_file_bodies(tmpdir):
    import pyarrow.parquet as pq

    daft.from_pydict(_issue_2111_sample_pydict()).write_parquet(str(tmpdir), partition_cols=["country"])

    written_files = [
        os.path.join(root, f) for root, _, files in os.walk(tmpdir) for f in files if f.endswith(".parquet")
    ]
    assert written_files, "expected at least one written parquet file"

    for file_path in written_files:
        schema_names = pq.read_schema(file_path).names
        assert "country" not in schema_names, (
            f"partition column 'country' must not appear in {file_path} (found {schema_names})"
        )


def test_pandas_can_read_daft_partitioned_parquet(tmpdir):
    pd = pytest.importorskip("pandas")

    sample = _issue_2111_sample_pydict()
    daft.from_pydict(sample).write_parquet(str(tmpdir), partition_cols=["country"])

    result = pd.read_parquet(str(tmpdir)).sort_values("first_name").reset_index(drop=True)
    expected = pd.DataFrame(sample).sort_values("first_name").reset_index(drop=True)
    assert set(result["country"]) == set(expected["country"])
    assert result["first_name"].tolist() == expected["first_name"].tolist()


def test_pyarrow_dataset_can_read_daft_partitioned_parquet(tmpdir):
    sample = _issue_2111_sample_pydict()
    daft.from_pydict(sample).write_parquet(str(tmpdir), partition_cols=["country"])

    pa_ds = ds.dataset(str(tmpdir), format="parquet", partitioning="hive")
    table = pa_ds.to_table()
    assert "country" in table.schema.names
    assert set(table.column("country").to_pylist()) == set(sample["country"])


def test_partition_cols_not_duplicated_in_csv_file_bodies(tmpdir):
    daft.from_pydict(_issue_2111_sample_pydict()).write_csv(str(tmpdir), partition_cols=["country"])

    written_files = [os.path.join(root, f) for root, _, files in os.walk(tmpdir) for f in files if f.endswith(".csv")]
    assert written_files, "expected at least one written csv file"

    for file_path in written_files:
        with open(file_path) as fh:
            header = fh.readline().strip().split(",")
        assert "country" not in header, (
            f"partition column 'country' must not appear in {file_path} header (found {header})"
        )


def test_partition_by_all_columns_falls_back(tmpdir):
    # Stripping would leave zero-column files; fall back to keeping columns in bodies.
    import pyarrow.parquet as pq

    daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_parquet(str(tmpdir), partition_cols=["a", "b"])
    written_files = [
        os.path.join(root, f) for root, _, files in os.walk(tmpdir) for f in files if f.endswith(".parquet")
    ]
    assert written_files
    schema = pq.read_schema(written_files[0])
    assert set(schema.names) == {"a", "b"}
