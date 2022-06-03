import os
import tempfile
import uuid

import numpy as np
import pyarrow.parquet as pq
import pytest
import ray

from daft.dataclasses import dataclass
from daft.datarepo.datarepo import DataRepo
from daft.datarepo.query.definitions import FilterPredicate, QueryColumn

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


def test_query_to_df(populated_datarepo):
    limit = 5

    def add_x_to_arr(arr: np.ndarray, x_kwarg: int = 100) -> np.ndarray:
        return arr + x_kwarg

    ds = (
        populated_datarepo.query(TestDc)
        .filter(FilterPredicate(left="x", comparator="<", right=10))
        .apply(add_x_to_arr, QueryColumn(name="arr"), x_kwarg=QueryColumn(name="x"))
        .limit(limit)
        .to_daft_dataset()
    )
    assert [row for row in ds._ray_dataset.iter_rows()] == [np.ones(1) * i for i in range(1, limit + 1)]
