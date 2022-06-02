
from daft.dataclasses import dataclass
from daft.datarepo.datarepo import DataRepo
from ray import data
import numpy as np

def test_datarepo_load() -> None:
    @dataclass
    class TestDc:
        x: int
        arr: np.ndarray

    dr = DataRepo.create('memory://test', 'name', TestDc)
    ds = data.range(100)
    ds = ds.map(lambda x: TestDc(x, np.ones(10)))

    dr.append(ds, rows_per_partition=10)
    print(dr.history())