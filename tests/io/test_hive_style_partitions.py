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
        # TODO(desmond): pyarrow does not conform to RFC 3339, so their timestamp format differs
        # from ours. Specifically, their timestamps are output as `%Y-%m-%d %H:%M:%S%.f%:z` but we
        # parse ours as %Y-%m-%dT%H:%M:%S%.f%:z.
        # ['timestamp_col'],
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
        # TODO(desmond): Same issue as the timestamp issue mentioned above.
        # ['timestamp_col'],
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
        # `from_arrow` vs `read_csv`. For now we read back an unpartitioned CSV table to check the
        # result against.
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
