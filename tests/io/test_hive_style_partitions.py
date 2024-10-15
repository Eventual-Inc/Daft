import pyarrow as pa
import pytest

import daft


@pytest.fixture(scope="session")
def public_storage_io_config() -> daft.io.IOConfig:
    return daft.io.IOConfig(
        azure=daft.io.AzureConfig(storage_account="dafttestdata", anonymous=True),
        s3=daft.io.S3Config(region_name="us-west-2", anonymous=True),
        gcs=daft.io.GCSConfig(anonymous=True),
    )


def check_file(public_storage_io_config, read_fn, uri):
    # These tables are partitioned on id1 (utf8) and id4 (int64).
    df = read_fn(uri, hive_partitioning=True, io_config=public_storage_io_config)
    column_names = df.schema().column_names()
    assert "id1" in column_names
    assert "id4" in column_names
    assert len(column_names) == 9
    assert df.count_rows() == 100000
    # Test schema inference on partition columns.
    pa_schema = df.schema().to_pyarrow_schema()
    assert pa_schema.field("id1").type == pa.large_string()
    assert pa_schema.field("id4").type == pa.int64()
    # Test that schema hints work on partition columns.
    df = read_fn(
        uri,
        hive_partitioning=True,
        io_config=public_storage_io_config,
        schema={
            "id4": daft.DataType.int32(),
        },
    )
    pa_schema = df.schema().to_pyarrow_schema()
    assert pa_schema.field("id1").type == pa.large_string()
    assert pa_schema.field("id4").type == pa.int32()

    # Test selects on a partition column and a non-partition columns.
    df_select = df.select("id2", "id1")
    column_names = df_select.schema().column_names()
    assert "id1" in column_names
    assert "id2" in column_names
    assert "id4" not in column_names
    assert len(column_names) == 2
    # TODO(desmond): .count_rows currently returns 0 if the first column in the Select is a
    #                partition column.
    assert df_select.count_rows() == 100000

    # Test filtering on partition columns.
    df_filter = df.where((daft.col("id1") == "id003") & (daft.col("id4") == 4) & (daft.col("id3") == "id0000000971"))
    column_names = df_filter.schema().column_names()
    assert "id1" in column_names
    assert "id4" in column_names
    assert len(column_names) == 9
    assert df_filter.count_rows() == 1


@pytest.mark.integration()
def test_hive_style_reads_csv(public_storage_io_config):
    uri = "s3://daft-public-data/test_fixtures/hive-style/test.csv/**"
    check_file(public_storage_io_config, daft.read_csv, uri)


@pytest.mark.integration()
def test_hive_style_reads_json(public_storage_io_config):
    uri = "s3://daft-public-data/test_fixtures/hive-style/test.json/**"
    check_file(public_storage_io_config, daft.read_json, uri)


@pytest.mark.integration()
def test_hive_style_reads_parquet(public_storage_io_config):
    uri = "s3://daft-public-data/test_fixtures/hive-style/test.parquet/**"
    check_file(public_storage_io_config, daft.read_parquet, uri)
