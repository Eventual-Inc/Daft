import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from daft import Datarepo
from daft.dataclasses import dataclass
from daft.datarepo.metadata_service import _LocalDatarepoMetadataService


@dataclass
class FakeDataclass:
    foo: int


@dataclass
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
    daft_schema = getattr(FakeDataclass, "_daft_schema")
    mock_tbl = daft_schema.serialize(FAKE_DATACLASSES)
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
        Datarepo.get("SHOULD_NOT_EXIST", data_type=FakeDataclass, svc=populated_metadata_service)


# @pytest.mark.skip
def test_get_datarepo(ray_cluster: None, populated_metadata_service: _LocalDatarepoMetadataService):
    datarepo_id = populated_metadata_service.list_ids()[0]
    datarepo = Datarepo.get(datarepo_id, data_type=FakeDataclass, svc=populated_metadata_service)
    assert datarepo._id == DATAREPO_ID

    # TODO(sammy): This will throw an error because .get does not yet deserialize the data correctly
    assert [row for row in datarepo._ray_dataset.iter_rows()] == FAKE_DATACLASSES


def test_save_datarepo(ray_cluster: None, empty_metadata_service: _LocalDatarepoMetadataService):
    ds = ray.data.range(10).map(lambda i: FakeNumpyDataclass(arr=np.ones((4, 4)) * i))
    datarepo = Datarepo(datarepo_id=DATAREPO_ID, ray_dataset=ds)

    datarepo.save(DATAREPO_ID, svc=empty_metadata_service)

    readback = Datarepo.get(DATAREPO_ID, data_type=FakeNumpyDataclass, svc=empty_metadata_service)

    original = list(datarepo._ray_dataset.iter_rows())

    to_verify = list(readback._ray_dataset.iter_rows())

    assert all([np.all(s.arr == t.arr) for s, t in zip(original, to_verify)])


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
