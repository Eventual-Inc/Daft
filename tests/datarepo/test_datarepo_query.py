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
    ds = populated_datarepo.query(TestDc).to_daft_dataset()
    assert sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x) == [
        TestDc(i, np.ones(1)) for i in range(100)
    ]


def test_query_limit(populated_datarepo):
    limit = 5
    ds = populated_datarepo.query(TestDc).limit(limit).to_daft_dataset()
    results = sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x)
    assert len(results) == limit


def test_query_apply(populated_datarepo):
    def add_x_to_arr(arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
        return arr + x_kwarg

    ds = (
        populated_datarepo.query(TestDc)
        .apply(add_x_to_arr, QueryColumn(name="arr"), x_kwarg=QueryColumn(name="x"))
        .to_daft_dataset()
    )
    assert sorted([row for row in ds._ray_dataset.iter_rows()]) == [np.ones(1) * i for i in range(1, 100 + 1)]


def test_query_where(populated_datarepo):
    ds = populated_datarepo.query(TestDc).where(QueryColumn("x"), "<", 10).to_daft_dataset()
    results = sorted([row for row in ds._ray_dataset.iter_rows()], key=lambda dc: dc.x)
    assert results == [TestDc(i, np.ones(1)) for i in range(10)]
