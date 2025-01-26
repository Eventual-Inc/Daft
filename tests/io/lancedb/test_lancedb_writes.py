import pyarrow as pa
import pytest

import daft
from tests.conftest import get_tests_daft_runner_name
from tests.integration.io.conftest import minio_create_bucket

TABLE_NAME = "my_table"
data = {
    "vector": [[1.1, 1.2], [0.2, 1.8]],
    "lat": [45.5, 40.1],
    "long": [-122.7, -74.1],
}

PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LOWER_BOUND_SKIP, reason="lance not supported on old versions of pyarrow")


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance")
    yield str(tmp_dir)


def test_lancedb_roundtrip(lance_dataset_path):
    df = daft.from_pydict(data)
    df.write_lance(lance_dataset_path)
    df = daft.read_lance(lance_dataset_path)
    assert df.to_pydict() == data


# TODO: re-enable test on Ray when fixed
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Lance fails to load credentials on Ray")
@pytest.mark.integration()
def test_lancedb_minio(minio_io_config):
    df = daft.from_pydict(data)
    bucket_name = "lance"
    s3_path = f"s3://{bucket_name}/data"
    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        df.write_lance(s3_path, io_config=minio_io_config)
        df = daft.read_lance(s3_path, io_config=minio_io_config)
        assert df.to_pydict() == data
