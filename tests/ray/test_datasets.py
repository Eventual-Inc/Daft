from __future__ import annotations

from typing import Any

import pytest

from daft import DataFrame
from daft.context import get_context


class MyObj:
    def __init__(self, x: int):
        self._x = x

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, MyObj) and self._x == other._x


DATA = {
    "intcol": [1, 2, 3],
    "strcol": ["a", "b", "c"],
}


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_ray_dataset_all_arrow(n_partitions: int):
    df = DataFrame.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("floatcol", df["intcol"].cast(float))
    ds = df.to_ray_dataset()

    rows = [row for row in ds.iter_rows()]
    assert rows == [
        {"intcol": 1, "strcol": "a", "floatcol": 1.0},
        {"intcol": 2, "strcol": "b", "floatcol": 2.0},
        {"intcol": 3, "strcol": "c", "floatcol": 3.0},
    ]


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_ray_dataset_with_py(n_partitions: int):
    df = DataFrame.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("pycol", df["intcol"].apply(lambda x: MyObj(x)))
    ds = df.to_ray_dataset()

    rows = [row for row in ds.iter_rows()]
    assert rows == [
        {"intcol": 1, "strcol": "a", "pycol": MyObj(1)},
        {"intcol": 2, "strcol": "b", "pycol": MyObj(2)},
        {"intcol": 3, "strcol": "c", "pycol": MyObj(3)},
    ]
