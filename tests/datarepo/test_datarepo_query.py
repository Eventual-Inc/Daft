import os
import tempfile
import uuid

import numpy as np
import pyarrow.parquet as pq
import pytest
import ray

from daft.dataclasses import dataclass
from daft.datarepo.datarepo import DataRepo
from daft.datarepo.query.definitions import QueryColumn
from daft.datarepo.query import functions as F

from typing import Iterator


@dataclass
class TestDc:
    x: int
    arr: np.ndarray


@pytest.fixture(scope="function")
def populated_datarepo(ray_cluster) -> Iterator[DataRepo]:
    with tempfile.TemporaryDirectory() as td:
        dr = DataRepo.create(f"file://{td}", "test_dc", TestDc)
        ds = ray.data.range(100)
        ds = ds.map(lambda x: TestDc(x, np.ones(1)))
        dr.append(ds, rows_per_partition=10)

        yield dr


def test_query_all(populated_datarepo):
    ds = populated_datarepo.query(TestDc).execute()
    assert sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x) == [
        TestDc(i, np.ones(1)) for i in range(100)
    ]


def test_query_limit(populated_datarepo):
    limit = 5
    ds = populated_datarepo.query(TestDc).limit(limit).execute()
    results = sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x)
    assert len(results) == limit


def test_query_apply(populated_datarepo):
    def add_x_to_arr(arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
        return arr + x_kwarg

    ds = populated_datarepo.query(TestDc).apply(add_x_to_arr, "arr", x_kwarg="x").execute()
    assert sorted([row for row in ds._ray_dataset.iter_rows()]) == [np.ones(1) * i for i in range(1, 100 + 1)]


def test_query_with_column_decorator(populated_datarepo):
    @F.func
    def add_x_to_arr(arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
        return arr + x_kwarg

    ds = populated_datarepo.query(TestDc).with_column("arr_added", add_x_to_arr("arr", x_kwarg="x")).execute()
    rows = sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x)
    assert [row.arr_added for row in rows] == [np.ones(1) * i for i in range(1, 100 + 1)]


def test_query_with_column(populated_datarepo):
    def add_x_to_arr(arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
        return arr + x_kwarg

    add_x_to_arr = F.func(add_x_to_arr, return_type=np.ndarray)

    ds = populated_datarepo.query(TestDc).with_column("arr_added", add_x_to_arr("arr", x_kwarg="x")).execute()
    rows = sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x)
    assert [row.arr_added for row in rows] == [np.ones(1) * i for i in range(1, 100 + 1)]


def test_query_where(populated_datarepo):
    ds = populated_datarepo.query(TestDc).where("x", "<", 10).execute()
    results = sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x)
    assert results == [TestDc(i, np.ones(1)) for i in range(10)]
