from __future__ import annotations

import os

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


def test_lancedb_write_with_create_append_mode(lance_dataset_path):
    import lance

    # Make lance dataset with a string column
    fields = [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("age", pa.int32(), nullable=True),
        pa.field("location", pa.large_string(), nullable=True),
    ]
    schema = pa.schema(fields)

    # Write daft dataframe to lance dataset
    data = {"id": [1], "name": ["A" * 100], "age": [1], "location": ["A" * 100]}
    df = daft.from_pydict(data)
    df.write_lance(lance_dataset_path, schema=schema, mode="create")

    # Read lance dataset back to daft dataframe and check if the data is written correctly
    df_loaded = daft.read_lance(lance_dataset_path)
    assert df_loaded.to_pydict() == data

    ds = lance.dataset(lance_dataset_path)
    assert ds.schema == schema


def test_create_mode_fails_when_target_is_file(lance_dataset_path):
    """When mode="create" and the target path points to a file, write_lance should fail fast."""
    # Create a file at the target path
    file_path = os.path.join(lance_dataset_path, "existing_file.txt")
    with open(file_path, "w") as f:
        f.write("test content")

    df = daft.from_pydict(data1)
    with pytest.raises(FileExistsError, match="Target path points to a file, cannot create a dataset here"):
        df.write_lance(file_path, mode="create")


def test_create_mode_fails_when_dataset_is_exists(lance_dataset_path):
    """When mode="create" and the target dataset already exists, write_lance should fail."""
    df = daft.from_pydict(data1)
    df.write_lance(lance_dataset_path, mode="create")

    with pytest.raises(ValueError, match="Cannot create a Lance dataset at a location where one already exists."):
        df.write_lance(lance_dataset_path, mode="create")


def test_create_mode_succeeds_when_directory_contains_file(lance_dataset_path):
    """When mode="create" and the target path exists and is non-empty, write_lance should succeed."""
    marker = os.path.join(lance_dataset_path, "marker")
    with open(marker, "w") as f:
        f.write("x")

    df = daft.from_pydict(data1)
    df.write_lance(lance_dataset_path, mode="create")

    df_loaded = daft.read_lance(lance_dataset_path)
    assert df_loaded.to_pydict() == data1


def test_create_mode_succeeds_when_directory_contains_subdirectory(lance_dataset_path):
    """When mode="create" and the target directory contains a subdirectory, write_lance should succeed."""
    subdir = os.path.join(lance_dataset_path, "subdir")
    os.makedirs(subdir)

    df = daft.from_pydict(data1)
    df.write_lance(lance_dataset_path, mode="create")

    df_loaded = daft.read_lance(lance_dataset_path)
    assert df_loaded.to_pydict() == data1


def test_create_mode_succeeds_on_missing_path(lance_dataset_path):
    """Create should succeed when the target path does not exist."""
    target_dir = os.path.join(lance_dataset_path, "new_table_missing")
    assert not os.path.exists(target_dir)

    df = daft.from_pydict(data1)
    df.write_lance(target_dir, mode="create")

    df_loaded = daft.read_lance(target_dir)
    assert df_loaded.to_pydict() == data1


def test_create_mode_succeeds_on_empty_directory(lance_dataset_path):
    """Create should succeed when the target is an empty directory."""
    target_dir = os.path.join(lance_dataset_path, "empty_dir")
    os.makedirs(target_dir)
    assert os.path.exists(target_dir) and len(os.listdir(target_dir)) == 0

    df = daft.from_pydict(data1)
    df.write_lance(target_dir, mode="create")

    df_loaded = daft.read_lance(target_dir)
    assert df_loaded.to_pydict() == data1


def test_append_mode_fails_when_dataset_does_not_exist(lance_dataset_path):
    """When mode="append" and the target dataset does not exist, write_lance should fail with appropriate error."""
    df = daft.from_pydict(data1)

    # Attempt to append to non-existent dataset should raise ValueError
    with pytest.raises(ValueError, match="Cannot append to non-existent Lance dataset"):
        df.write_lance(lance_dataset_path, mode="append")
