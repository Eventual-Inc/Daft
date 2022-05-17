import ray
import pytest
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from daft.datarepo.metadata_service import _LocalDatarepoMetadataService
from daft import Datarepo

DATAREPO_ID = "datarepo_foo"
FAKE_DATA = [{"foo": i} for i in range(10)]

@pytest.fixture()
def empty_metadata_service():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield _LocalDatarepoMetadataService(tmpdir)

@pytest.fixture()
def populated_metadata_service(empty_metadata_service: _LocalDatarepoMetadataService):
    path = empty_metadata_service.get_path(DATAREPO_ID)
    mock_tbl = pa.Table.from_pylist(FAKE_DATA)
    pq.write_table(mock_tbl, path)
    yield empty_metadata_service

@pytest.fixture(scope="module")
def ray_cluster():
    ray.init(num_cpus=2)
    yield
    ray.shutdown()

def test_get_datarepo_missing(ray_cluster: None, populated_metadata_service: _LocalDatarepoMetadataService):
    # TODO(jaychia): Change when we have better error types
    with pytest.raises(FileNotFoundError):
        Datarepo.get("SHOULD_NOT_EXIST", svc=populated_metadata_service)

def test_get_datarepo(ray_cluster: None, populated_metadata_service: _LocalDatarepoMetadataService):
    datarepo_id = populated_metadata_service.list_ids()[0]
    datarepo = Datarepo.get(datarepo_id, svc=populated_metadata_service)
    assert datarepo._id == DATAREPO_ID
    assert [row for row in datarepo._ray_dataset.iter_rows()] == FAKE_DATA
