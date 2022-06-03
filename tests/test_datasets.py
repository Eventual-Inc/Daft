import os
import tempfile
import uuid
from typing import List

import numpy as np
import pyarrow.parquet as pq
import pytest
import ray

from daft import Dataset
from daft.dataclasses import dataclass
from daft.datarepo import DatarepoClient


@dataclass
class FakeDataclass:
    foo: int
    bar: int


@dataclass
class FakeNumpyDataclass:
    arr: np.ndarray


DATAREPO_ID = "datarepo_foo"
FAKE_DATA = [{"foo": i, "bar": i} for i in range(10)]
FAKE_DATACLASSES = [FakeDataclass(foo=d["foo"], bar=d["bar"]) for d in FAKE_DATA]


@pytest.fixture()
def empty_datarepo_client():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield DatarepoClient(f"file://{tmpdir}")


@pytest.fixture()
def populated_datarepo_client(empty_datarepo_client: DatarepoClient):
    path = f"{empty_datarepo_client._protocol}://{empty_datarepo_client._prefix}/{DATAREPO_ID}"
    empty_datarepo_client._fs.mkdir(path)
    daft_schema = getattr(FakeDataclass, "_daft_schema")
    mock_tbl = daft_schema.serialize(FAKE_DATACLASSES)
    pq.write_table(mock_tbl, os.path.join(path, str(uuid.uuid4()) + ".parquet"))
    yield empty_datarepo_client


@pytest.fixture(scope="module")
def ray_cluster():
    ray.init(num_cpus=2)
    yield
    ray.shutdown()


@pytest.mark.skip
def test_get_datarepo_missing(ray_cluster: None, populated_datarepo_client: DatarepoClient):
    # TODO(jaychia): Change when we have better error types
    with pytest.raises(FileNotFoundError):
        populated_datarepo_client.get_dataset("SHOULD_NOT_EXIST", data_type=FakeDataclass)


@pytest.mark.skip
def test_datarepo_from_datarepo_id(ray_cluster: None, populated_datarepo_client: DatarepoClient):
    datarepo_id = populated_datarepo_client.list_ids()[0]
    dataset = populated_datarepo_client.get_dataset(datarepo_id, data_type=FakeDataclass)
    assert dataset._id == DATAREPO_ID
    assert [row for row in dataset._ray_dataset.iter_rows()] == FAKE_DATACLASSES


@pytest.mark.skip
def test_datarepo_from_datarepo_id_load_column_subset(ray_cluster: None, populated_datarepo_client: DatarepoClient):
    datarepo_id = populated_datarepo_client.list_ids()[0]
    dataset = populated_datarepo_client.get_dataset(datarepo_id, data_type=FakeDataclass, columns=["foo"])
    assert dataset._id == DATAREPO_ID
    assert [row for row in dataset._ray_dataset.iter_rows()] == [
        FakeDataclass(foo=dc.foo, bar=None) for dc in FAKE_DATACLASSES  # type: ignore
    ]


@pytest.mark.skip
def test_save_datarepo(ray_cluster: None, empty_datarepo_client: DatarepoClient):
    def f(i: int) -> FakeNumpyDataclass:
        return FakeNumpyDataclass(arr=np.ones((4, 4)) * i)

    ds = ray.data.range(10).map(f)
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)

    empty_datarepo_client.save(DATAREPO_ID, dataset)

    readback = empty_datarepo_client.get_dataset(DATAREPO_ID, data_type=FakeNumpyDataclass)

    original = [item.arr for item in dataset._ray_dataset.iter_rows()]  # type: ignore

    to_verify = [item.arr for item in readback._ray_dataset.iter_rows()]  # type: ignore

    assert all([np.all(s == t) for s, t in zip(original, to_verify)])


def test_datarepo_map(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)

    def f(item: FakeDataclass) -> int:
        return item.foo + 1

    mapped_repo = dataset.map(f)
    assert [row for row in mapped_repo._ray_dataset.iter_rows()] == [d["foo"] + 1 for d in FAKE_DATA]


def test_datarepo_map_actor(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)

    class Actor:
        def __init__(self):
            pass

        def __call__(self, item: FakeDataclass):
            return item.foo + 1

    mapped_ds: Dataset[int] = dataset.map(Actor)

    # NOTE(jaychia): Use sets here because for some reason actors reverse the order of data
    assert {row for row in mapped_ds._ray_dataset.iter_rows()} == {d["foo"] + 1 for d in FAKE_DATA}


def test_datarepo_map_batches(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)

    def f(items: List[FakeDataclass]) -> List[int]:
        return [item.foo + 1 for item in items]

    mapped_ds: Dataset[int] = dataset.map_batches(f, batch_size=2)
    assert [row for row in mapped_ds._ray_dataset.iter_rows()] == [d["foo"] + 1 for d in FAKE_DATA]


def test_datarepo_map_batches_actor(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)

    class Actor:
        def __init__(self):
            pass

        def __call__(self, items: List[FakeDataclass]):
            return [item.foo + 1 for item in items]

    mapped_ds: Dataset[int] = dataset.map_batches(Actor, batch_size=2)
    # NOTE(jaychia): Use sets here because for some reason actors reverse the order of data
    assert {row for row in mapped_ds._ray_dataset.iter_rows()} == {d["foo"] + 1 for d in FAKE_DATA}


def test_datarepo_filter(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)

    def f(item: FakeDataclass) -> bool:
        return item.foo < 5

    mapped_ds: Dataset[int] = dataset.filter(f)
    assert [row for row in mapped_ds._ray_dataset.iter_rows()] == [FakeDataclass(foo=i, bar=i) for i in range(5)]


def test_datarepo_filter_actor(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)

    class Actor:
        def __init__(self):
            pass

        def __call__(self, item: FakeDataclass):
            return item.foo < 5

    mapped_ds: Dataset[FakeDataclass] = dataset.filter(Actor)
    # NOTE(jaychia): Use sets here because for some reason actors reverse the order of data
    assert {
        # This is always a FakeDataclass and not an ArrowRow as ._ray_dataset is not tabular
        row.foo  # type: ignore
        for row in mapped_ds._ray_dataset.iter_rows()
    } == {i for i in range(5)}


def test_datarepo_take(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)
    sample = dataset.take(5)
    assert sample == [FakeDataclass(foo=i, bar=i) for i in range(5)]


def test_datarepo_sample(ray_cluster: None):
    ds = ray.data.range(10).map(lambda i: FakeDataclass(foo=i, bar=i))
    dataset = Dataset(dataset_id=DATAREPO_ID, ray_dataset=ds)
    sample_repo = dataset.sample(5)
    assert [row for row in sample_repo._ray_dataset.iter_rows()] == [FakeDataclass(foo=i, bar=i) for i in range(5)]
