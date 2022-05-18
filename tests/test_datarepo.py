import dataclasses
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from daft import Datarepo
from daft.datarepo.metadata_service import _LocalDatarepoMetadataService


@dataclasses.dataclass
class FakeDataclass:
    foo: int


@dataclasses.dataclass
class FakeNumpyDataclass:
    arr: np.ndarray


DATAREPO_ID = "datarepo_foo"
FAKE_DATA = [{"foo": i} for i in range(10)]
FAKE_DATACLASSES = [FakeDataclass(foo=d["foo"]) for d in FAKE_DATA]


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


@pytest.mark.skip
def test_get_datarepo(ray_cluster: None, populated_metadata_service: _LocalDatarepoMetadataService):
    datarepo_id = populated_metadata_service.list_ids()[0]
    datarepo = Datarepo.get(datarepo_id, svc=populated_metadata_service)
    assert datarepo._id == DATAREPO_ID

    # TODO(sammy): This will throw an error because .get does not yet deserialize the data correctly
    assert [row for row in datarepo._ray_dataset.iter_rows()] == FAKE_DATACLASSES


@pytest.mark.skip
def test_save_datarepo(ray_cluster: None, empty_metadata_service: _LocalDatarepoMetadataService):
    ds = ray.data.range(10).map(lambda i: FakeNumpyDataclass(arr=np.ones((4, 4)) * i))
    datarepo = Datarepo(datarepo_id=DATAREPO_ID, ray_dataset=ds)

    # TODO(sammy): This should fail as Ray cannot serialize multidimensional arrays to Arrow
    datarepo.save(DATAREPO_ID, svc=empty_metadata_service)
    ds_written = ray.data.read_parquet(empty_metadata_service.get_path(DATAREPO_ID))

    # TODO(sammy): Fill in the correct serialization for numpy on disk
    assert [row for row in ds_written.iter_rows()] == [{} for i in range(10)]


def test_datarepo_map(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i))
    datarepo = Datarepo(datarepo_id=DATAREPO_ID, ray_dataset=ds)
    mapped_repo = datarepo.map(lambda d: d.foo + 1)
    assert [row for row in mapped_repo._ray_dataset.iter_rows()] == [d["foo"] + 1 for d in FAKE_DATA]


def test_datarepo_map_batches(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i))
    datarepo = Datarepo(datarepo_id=DATAREPO_ID, ray_dataset=ds)
    mapped_repo = datarepo.map_batches(lambda dicts: [d.foo + 1 for d in dicts], batch_size=2)
    assert [row for row in mapped_repo._ray_dataset.iter_rows()] == [d["foo"] + 1 for d in FAKE_DATA]
