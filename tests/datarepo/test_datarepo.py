import tempfile

import numpy as np
from ray import data

from daft.dataclasses import dataclass
from daft.datarepo.datarepo import DataRepo
from .utils import create_test_catalog


def test_datarepo_load() -> None:
    @dataclass
    class TestDc:
        x: int
        arr: np.ndarray

    with tempfile.TemporaryDirectory() as td:
        catalog = create_test_catalog(td)
        dr = DataRepo.create(catalog, "test_dc", TestDc)
        ds = data.range(100)
        ds = ds.map(lambda x: TestDc(x, np.ones(1)))
        dr.append(ds, rows_per_partition=10)

        read_back_ds = dr.to_dataset(TestDc)
        assert ds.sort(lambda v: v.x).take_all() == read_back_ds.sort(lambda v: v.x).take_all()

def test_datarepo_overwrite() -> None:
    @dataclass
    class TestDc:
        x: int
        arr: np.ndarray

    with tempfile.TemporaryDirectory() as td:
        catalog = create_test_catalog(td)
        dr = DataRepo.create(catalog, "test_dc", TestDc)
        ds = data.range(100)
        ds = ds.map(lambda x: TestDc(x, np.ones(1)))
        dr.append(ds, rows_per_partition=10)

        read_back_ds = dr.to_dataset(TestDc)
        assert ds.sort(lambda v: v.x).take_all() == read_back_ds.sort(lambda v: v.x).take_all()

        ds2 = data.range(100).map(lambda x: x + 100)
        ds2 = ds2.map(lambda x: TestDc(x, np.ones(1)))
        dr.overwrite(ds2, rows_per_partition=10)
        read_back_ds = dr.to_dataset(TestDc)

        assert ds2.sort(lambda v: v.x).take_all() == read_back_ds.sort(lambda v: v.x).take_all()


