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
