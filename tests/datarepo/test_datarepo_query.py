import tempfile
from typing import Iterator, List

import numpy as np
import pytest
import ray

from daft.dataclasses import dataclass
from daft.datarepo.datarepo import DataRepo
from daft.datarepo.query import functions as F

from .utils import create_test_catalog


@dataclass
class _TestDc:
    x: int
    arr: np.ndarray


@pytest.fixture(scope="function")
def populated_datarepo() -> Iterator[DataRepo]:
    with tempfile.TemporaryDirectory() as td:
        catalog = create_test_catalog(td)
        dr = DataRepo.create(catalog, "test_dc", _TestDc)
        ds = ray.data.range(100)
        ds = ds.map(lambda x: _TestDc(x, np.ones(1)))
        dr.append(ds, rows_per_partition=10)

        yield dr


def test_query_all(populated_datarepo):
    ds = populated_datarepo.query(_TestDc).execute()
    assert sorted([row for row in ds.iter_rows()], key=lambda dc: dc.x) == [_TestDc(i, np.ones(1)) for i in range(100)]


def test_query_limit(populated_datarepo):
    limit = 5
    ds = populated_datarepo.query(_TestDc).limit(limit).execute()
    results = sorted([row for row in ds.iter_rows()], key=lambda dc: dc.x)
    assert len(results) == limit


@F.func
def add_x_to_arr_decorator(arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
    return arr + x_kwarg


@F.func(return_type=np.ndarray)
def add_x_to_arr_decorater_kwarg(arr: np.ndarray, x_kwarg: int = 100):
    return arr + x_kwarg


def add_x_to_arr(arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
    return arr + x_kwarg


add_x_to_arr_func = F.func(add_x_to_arr, return_type=np.ndarray)


@F.func
class AddXToArrDecorator:
    def __init__(self):
        self.offset = 0

    def __call__(self, arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
        retarr: np.ndarray = arr + x_kwarg + self.offset
        return retarr


@F.func(return_type=np.ndarray)
class AddXToArrDecoratorKwarg:
    def __init__(self):
        self.offset = 0

    def __call__(self, arr: np.ndarray, x_kwarg: int = 100):
        return arr + x_kwarg + self.offset


class AddXToArr:
    def __init__(self):
        self.offset = 0

    def __call__(self, arr: np.ndarray, x_kwarg: int = 100):
        return arr + x_kwarg + self.offset


AddXToArrFunc = F.func(AddXToArr, return_type=np.ndarray)


@pytest.mark.parametrize(
    "f",
    [
        add_x_to_arr_func,
        add_x_to_arr_decorator,
        add_x_to_arr_decorater_kwarg,
        AddXToArrDecorator,
        AddXToArrDecoratorKwarg,
        AddXToArrFunc,
    ],
)
def test_query_with_column_decorator(populated_datarepo, f):
    ds = populated_datarepo.query(_TestDc).with_column("arr_added", f("arr", x_kwarg="x")).execute()
    rows = sorted([row for row in ds.iter_rows()], key=lambda dc: dc.x)
    assert [row.arr_added for row in rows] == [np.ones(1) * i for i in range(1, 100 + 1)]


@F.batch_func
def add_x_to_arr_batch_decorator(arr_batch: List[np.ndarray], x_batch: List[int]) -> List[np.ndarray]:
    return [arr + x for arr, x in zip(arr_batch, x_batch)]


@F.batch_func(return_type=List[np.ndarray])
def add_x_to_arr_batch_decorator_kwarg(arr_batch: List[np.ndarray], x_batch: List[int]):
    return [arr + x for arr, x in zip(arr_batch, x_batch)]


def add_x_to_arr_batch(arr_batch: List[np.ndarray], x_batch: List[int]):
    return [arr + x for arr, x in zip(arr_batch, x_batch)]


add_x_to_arr_batch_func = F.batch_func(add_x_to_arr_batch, return_type=List[np.ndarray])


@F.batch_func
class AddXToArrBatchDecorator:
    def __init__(self):
        self.offset = 0

    def __call__(self, arr_batch: np.ndarray, x_batch: List[int]) -> List[np.ndarray]:
        return [arr + x for arr, x in zip(arr_batch, x_batch)]


@F.batch_func(return_type=List[np.ndarray])
class AddXToArrBatchDecoratorKwarg:
    def __init__(self):
        self.offset = 0

    def __call__(self, arr_batch: np.ndarray, x_batch: List[int]) -> List[np.ndarray]:
        return [arr + x for arr, x in zip(arr_batch, x_batch)]


class AddXToArrBatch:
    def __init__(self):
        self.offset = 0

    def __call__(self, arr_batch: np.ndarray, x_batch: List[int]) -> List[np.ndarray]:
        return [arr + x for arr, x in zip(arr_batch, x_batch)]


AddXToArrBatchFunc = F.batch_func(AddXToArrBatch, return_type=List[np.ndarray])


@pytest.mark.parametrize(
    "f",
    [
        add_x_to_arr_batch_func,
        add_x_to_arr_batch_decorator,
        add_x_to_arr_batch_decorator_kwarg,
        AddXToArrBatchDecorator,
        AddXToArrBatchDecoratorKwarg,
        AddXToArrBatchFunc,
    ],
)
def test_query_with_column_batch(populated_datarepo, f):
    ds = populated_datarepo.query(_TestDc).with_column("arr_added", f("arr", "x")).execute()
    rows = sorted([row for row in ds.iter_rows()], key=lambda dc: dc.x)
    assert [row.arr_added for row in rows] == [np.ones(1) * i for i in range(1, 100 + 1)]


def test_query_where(populated_datarepo):
    ds = populated_datarepo.query(_TestDc).where("x", "<", 10).execute()
    results = sorted([row for row in ds.iter_rows()], key=lambda dc: dc.x)
    assert results == [_TestDc(i, np.ones(1)) for i in range(10)]
