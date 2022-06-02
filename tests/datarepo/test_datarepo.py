import tempfile

import numpy as np
from ray import data

from daft.dataclasses import dataclass
from daft.datarepo.datarepo import DataRepo


def test_datarepo_load() -> None:
    @dataclass
    class TestDc:
        x: int
        arr: np.ndarray

    with tempfile.TemporaryDirectory() as td:
        dr = DataRepo.create(f"file://{td}", "test_dc", TestDc)
        ds = data.range(100)
        ds = ds.map(lambda x: TestDc(x, np.ones(1)))
        dr.append(ds, rows_per_partition=10)

        read_back_ds = dr.to_dataset(TestDc)
        assert ds.sort(lambda v: v.x).take_all() == read_back_ds.sort(lambda v: v.x).take_all()
