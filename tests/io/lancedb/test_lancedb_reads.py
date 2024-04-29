import sys

import lance
import pyarrow as pa
import pytest

import daft

if sys.version_info[:2] < (3, 8):
    pytest.skip(allow_module_level=True, reason="LanceDB does not support Python 3.7 and below")

TABLE_NAME = "my_table"
data = {
    "vector": [[1.1, 1.2], [0.2, 1.8]],
    "lat": [45.5, 40.1],
    "long": [-122.7, -74.1],
}


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance")
    lance.write_dataset(pa.Table.from_pydict(data), tmp_dir)
    yield str(tmp_dir)


def test_lancedb_read(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path)
    assert df.to_pydict() == data


def test_lancedb_read_column_selection(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path)
    df = df.select("vector")
    assert df.to_pydict() == {"vector": data["vector"]}


def test_lancedb_read_filter(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path)
    df = df.where(df["lat"] > 45)
    df = df.select("vector")
    assert df.to_pydict() == {"vector": data["vector"][:1]}
