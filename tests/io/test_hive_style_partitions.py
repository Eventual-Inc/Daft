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
        "str_col": ["str" + str(i % 5) for i in range(100)],
        "int_col": [i % 7 for i in range(100)],
        "date_col": [date(2024, 1, i % 28 + 1) for i in range(100)],
        "timestamp_col": [datetime(2024, 1, i % 28 + 1, i % 24) for i in range(100)],
        "value": range(100),
        # Include some nulls.
        "nullable_str": [None if i % 10 == 1 else f"val{i}" for i in range(100)],
        "nullable_int": [None if i % 7 == 1 else i for i in range(100)],
    },
    schema=SCHEMA,
)


def unify_timestamp(table):
    return table.set_column(
        table.schema.get_field_index("timestamp_col"), "timestamp_col", table["timestamp_col"].cast(pa.timestamp("us"))
    )


def assert_tables_equal(daft_df, pa_table):
    sorted_pa_table = pa_table.sort_by([("id", "ascending")]).select(SCHEMA.names)
    sorted_pa_table = unify_timestamp(sorted_pa_table)
    sorted_daft_table = daft_df.sort(daft.col("id")).to_arrow().combine_chunks().select(SCHEMA.names)
    sorted_daft_table = unify_timestamp(sorted_daft_table)
    assert sorted_pa_table == sorted_daft_table


@pytest.mark.parametrize(
    "partition_by",
    [
        ["str_col"],
        ["int_col"],
        ["date_col"],
        # TODO(desmond): pyarrow does not conform to RFC 3339, so their timestamp format differs from
        # ours. Specifically, their timestamps are output as `%Y-%m-%d %H:%M:%S%.f%:z` but we parse ours
        # as %Y-%m-%dT%H:%M:%S%.f%:z.
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
        daft_df = daft.read_json(glob_path, hive_partitioning=True)
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
    assert_tables_equal(daft_df, pa_table)


# def test_hive_daft_write_read(tmpdir, sample_data):
#     # Convert PyArrow table to Daft DataFrame
#     daft_df = daft.from_pyarrow(sample_data)

#     # Write using Daft with different partition columns
#     partition_configs = [
#         ["str_col"],
#         ["int_col"],
#         ["date_col"],
#         ["timestamp_col"],
#         ["nullable_str"],
#         ["str_col", "int_col"]
#     ]

#     for partition_by in partition_configs:
#         partition_dir = os.path.join(tmpdir, "_".join(partition_by))
#         os.makedirs(partition_dir, exist_ok=True)

#         # Write partitioned dataset using Daft
#         daft_df.write_parquet(
#             partition_dir,
#             partition_cols=partition_by
#         )

#         # Read back with Daft
#         read_df = daft.read_parquet(
#             os.path.join(partition_dir, "**"),
#             hive_partitioning=True
#         )

#         assert_tables_equal(read_df, sample_data)

# def test_null_handling(sample_data):
#     with tempfile.TemporaryDirectory() as tmpdir:
#         # Write dataset with nullable columns as partitions
#         ds.write_dataset(
#             sample_data,
#             tmpdir,
#             format="parquet",
#             partitioning=ds.partitioning(
#                 pa.schema([
#                     sample_data.schema.field("nullable_str"),
#                     sample_data.schema.field("nullable_int")
#                 ]),
#                 flavor="hive"
#             )
#         )

#         # Read back with Daft
#         daft_df = daft.read_parquet(
#             os.path.join(tmpdir, "**"),
#             hive_partitioning=True
#         )

#         # Verify null values are handled correctly
#         # PyArrow uses __HIVE_DEFAULT_PARTITION__ for nulls
#         null_count_daft = daft_df.where(daft.col("nullable_str").is_null()).count_rows()
#         expected_null_count = len([x for x in sample_data["nullable_str"] if x.is_null()])
#         assert null_count_daft == expected_null_count


# @pytest.fixture(scope="session")
# def public_storage_io_config() -> daft.io.IOConfig:
#     return daft.io.IOConfig(
#         azure=daft.io.AzureConfig(storage_account="dafttestdata", anonymous=True),
#         s3=daft.io.S3Config(region_name="us-west-2", anonymous=True),
#         gcs=daft.io.GCSConfig(anonymous=True),
#     )


# def check_file(public_storage_io_config, read_fn, uri):
#     # These tables are partitioned on id1 (utf8) and id4 (int64).
#     df = read_fn(uri, hive_partitioning=True, io_config=public_storage_io_config)
#     column_names = df.schema().column_names()
#     assert "id1" in column_names
#     assert "id4" in column_names
#     assert len(column_names) == 9
#     assert df.count_rows() == 100000
#     # Test schema inference on partition columns.
#     pa_schema = df.schema().to_pyarrow_schema()
#     assert pa_schema.field("id1").type == pa.large_string()
#     assert pa_schema.field("id4").type == pa.int64()
#     # Test that schema hints work on partition columns.
#     df = read_fn(
#         uri,
#         hive_partitioning=True,
#         io_config=public_storage_io_config,
#         schema={
#             "id4": daft.DataType.int32(),
#         },
#     )
#     pa_schema = df.schema().to_pyarrow_schema()
#     assert pa_schema.field("id1").type == pa.large_string()
#     assert pa_schema.field("id4").type == pa.int32()

#     # Test selects on a partition column and a non-partition columns.
#     df_select = df.select("id2", "id1")
#     column_names = df_select.schema().column_names()
#     assert "id1" in column_names
#     assert "id2" in column_names
#     assert "id4" not in column_names
#     assert len(column_names) == 2
#     # TODO(desmond): .count_rows currently returns 0 if the first column in the Select is a
#     #                partition column.
#     assert df_select.count_rows() == 100000

#     # Test filtering on partition columns.
#     df_filter = df.where((daft.col("id1") == "id003") & (daft.col("id4") == 4) & (daft.col("id3") == "id0000000971"))
#     column_names = df_filter.schema().column_names()
#     assert "id1" in column_names
#     assert "id4" in column_names
#     assert len(column_names) == 9
#     assert df_filter.count_rows() == 1


# @pytest.mark.integration()
# def test_hive_style_reads_s3_csv(public_storage_io_config):
#     uri = "s3://daft-public-data/test_fixtures/hive-style/test.csv/**"
#     check_file(public_storage_io_config, daft.read_csv, uri)


# @pytest.mark.integration()
# def test_hive_style_reads_s3_json(public_storage_io_config):
#     uri = "s3://daft-public-data/test_fixtures/hive-style/test.json/**"
#     check_file(public_storage_io_config, daft.read_json, uri)


# @pytest.mark.integration()
# def test_hive_style_reads_s3_parquet(public_storage_io_config):
#     uri = "s3://daft-public-data/test_fixtures/hive-style/test.parquet/**"
#     check_file(public_storage_io_config, daft.read_parquet, uri)
