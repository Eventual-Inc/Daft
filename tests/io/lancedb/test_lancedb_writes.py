from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from tests.integration.io.conftest import minio_create_bucket

TABLE_NAME = "my_table"
data1 = {
    "vector": [[1.1, 1.2], [0.2, 1.8]],
    "lat": [45.5, 40.1],
    "long": [-122.7, -74.1],
}

data2 = {
    "vector": [[2.1, 2.2], [2.2, 2.8]],
    "lat": [46.5, 41.1],
    "long": [-123.7, -75.1],
}

PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LOWER_BOUND_SKIP, reason="lance not supported on old versions of pyarrow")


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance")
    yield str(tmp_dir)


def test_lancedb_roundtrip(lance_dataset_path):
    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    df1.write_lance(lance_dataset_path, mode="create")
    df2.write_lance(lance_dataset_path, mode="append")
    df_loaded = daft.read_lance(lance_dataset_path)

    assert df_loaded.to_pydict() == df1.concat(df2).to_pydict()


@pytest.mark.integration()
def test_lancedb_minio(minio_io_config):
    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    bucket_name = "lance"
    s3_path = f"s3://{bucket_name}/data"
    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        df1.write_lance(s3_path, mode="create", io_config=minio_io_config)
        df2.write_lance(s3_path, mode="append", io_config=minio_io_config)
        df_loaded = daft.read_lance(s3_path, io_config=minio_io_config)
        assert df_loaded.to_pydict() == df1.concat(df2).to_pydict()


def test_lancedb_write_with_schema(lance_dataset_path):
    """Writing a dataframe to lance with a user-provided schema with lance encodings."""
    data = {
        "vector": [1.1, 1.2],
        "compressible_strings": ["a" * 100, "b" * 100],
    }
    df = daft.from_pydict(data)

    pa_schema = pa.schema(
        [
            pa.field("vector", pa.float64()),
            pa.field("compressible_strings", pa.string(), metadata={"lance-encoding:compression": "zstd"}),
        ]
    )
    daft_schema = daft.schema.Schema.from_pyarrow_schema(pa_schema)
    print(f"\n daft schema:\n {daft_schema}; \n with metadata: \n{daft_schema.display_with_metadata(True)}")
    assert daft_schema.__repr__() == daft_schema.display_with_metadata(False) + "\n"
    df.write_lance(lance_dataset_path, schema=daft_schema, mode="create")

    df_loaded = daft.read_lance(lance_dataset_path)
    assert df_loaded.to_pydict() == df.to_pydict()

    import lance

    ds = lance.dataset(lance_dataset_path)
    written_pa_schema = ds.schema

    compress_field = written_pa_schema.field("compressible_strings")
    assert compress_field.type == pa.large_string()

    compress_field_metadata = compress_field.metadata
    assert compress_field_metadata is not None
    assert compress_field_metadata[b"lance-encoding:compression"] == b"zstd"


def test_lancedb_write_blob(lance_dataset_path):
    schema = pa.schema(
        [
            pa.field("blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}),
        ]
    )

    blobs_data = [b"foo", b"bar", b"baz"]
    df = daft.from_pydict({"blob": blobs_data})

    df.write_lance(lance_dataset_path, schema=daft.schema.Schema.from_pyarrow_schema(schema))

    import lance

    ds = lance.dataset(lance_dataset_path)
    written_pa_schema = ds.schema

    blob_field = written_pa_schema.field("blob")
    assert blob_field.type == pa.large_binary()

    blob_field_metadata = blob_field.metadata
    assert blob_field_metadata is not None
    assert blob_field_metadata[b"lance-encoding:blob"] == b"true"

    row_ids = ds.to_table(columns=[], with_row_id=True).column("_rowid").to_pylist()
    blobs = ds.take_blobs("blob", row_ids)
    for expected in blobs_data:
        with blobs.pop(0) as f:
            assert f.read() == expected


def test_lancedb_write_string(lance_dataset_path):
    import lance

    # Make lance dataset with a string column
    fields = [pa.field("name", pa.string(), nullable=True), pa.field("age", pa.int64(), nullable=True)]
    schema = pa.schema(fields)
    empty_table = pa.Table.from_pylist([], schema=schema)
    lance.write_dataset(empty_table, lance_dataset_path)

    # Write daft dataframe to lance dataset
    data = {"name": ["A" * 100], "age": [1]}
    df = daft.from_pydict(data)
    df.write_lance(lance_dataset_path, mode="append")

    # Read lance dataset back to daft dataframe and check if the data is written correctly
    df_loaded = daft.read_lance(lance_dataset_path)
    assert df_loaded.to_pydict() == data


def test_lancedb_write_incompatible_schema(lance_dataset_path):
    import lance

    # Make lance dataset with an int and a string column
    fields = [pa.field("name", pa.string(), nullable=True), pa.field("age", pa.int64(), nullable=True)]
    schema = pa.schema(fields)
    empty_table = pa.Table.from_pylist([], schema=schema)
    lance.write_dataset(empty_table, lance_dataset_path)

    # Write daft dataframe with incompatible schema to lance dataset
    data = {"name": ["A" * 100], "age": [[1, 2, 3]]}  # age is an int column, but data is a list column
    df = daft.from_pydict(data)

    with pytest.raises(ValueError, match="Schema of data does not match table schema"):
        df.write_lance(lance_dataset_path, mode="append")


def test_lancedb_morsel_size_config_fragment(lance_dataset_path):
    import os

    if os.environ.get("DAFT_FLOTILLA") == "false" or os.environ.get("DAFT_FLOTILLA") == "0":
        pytest.skip("Skipping test when DAFT_FLOTILLA=false")

    import lance

    daft.context.set_runner_ray(noop_if_initialized=True)

    num_rows = 50
    data_rows = []
    for i in range(num_rows):
        data_rows.append(
            {
                "id": i,
                "vector": [float(i), float(i + 1)],
                "data": f"task_{i}_" + "x" * 100,  # Make each row substantial
            }
        )

    dfs = []
    for row_data in data_rows:
        df_single = daft.from_pydict({k: [v] for k, v in row_data.items()})
        dfs.append(df_single)

    # Write each dataframe separately to create many fragments
    for i, df in enumerate(dfs):
        mode = "create" if i == 0 else "append"
        df.write_lance(lance_dataset_path, mode=mode, max_rows_per_file=1)

    df_loaded = daft.read_lance(lance_dataset_path)

    ds_before = lance.dataset(lance_dataset_path)
    fragments_before = len(ds_before.get_fragments())

    repartitioned_path = lance_dataset_path + "_repartitioned_default"
    df_repartitioned = df_loaded.repartition(1)
    df_repartitioned.write_lance(repartitioned_path, mode="create")

    #
    ds_after_default = lance.dataset(repartitioned_path)
    fragments_after_default = len(ds_after_default.get_fragments())

    daft.context.set_execution_config(morsel_size_lower_bound=5, morsel_size_upper_bound=20)

    df_loaded_config = daft.read_lance(lance_dataset_path)

    # Repartition to 1 partition with morsel size bounds
    repartitioned_config_path = lance_dataset_path + "_repartitioned_config"
    df_repartitioned_config = df_loaded_config.repartition(1)
    df_repartitioned_config.write_lance(repartitioned_config_path, mode="create")

    ds_after_config = lance.dataset(repartitioned_config_path)
    fragments_after_config = len(ds_after_config.get_fragments())

    assert fragments_before >= num_rows, f"Expected at least {num_rows} fragments initially, got {fragments_before}"
    assert fragments_after_config == 10
    assert (
        fragments_after_default == fragments_before
    ), f"Expected fragment count with config to be less than or equal to without config, got {fragments_after_config} and {fragments_after_default}."
