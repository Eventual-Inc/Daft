from __future__ import annotations

from typing import Any

import numpy as np
import pytest
import ray.data

from daft import DataFrame
from daft.context import get_context

RAY_VERSION = tuple(int(s) for s in ray.__version__.split("."))


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

    if RAY_VERSION >= (2, 2, 0):
        assert ds.dataset_format() == "arrow", "Ray Dataset format should be arrow"
    elif RAY_VERSION >= (2, 0, 0):
        assert ds._dataset_format() == "arrow", "Ray Dataset format should be arrow"

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

    if RAY_VERSION >= (2, 2, 0):
        assert ds.dataset_format() == "simple", "Ray Dataset format should be simple because it has Python objects"
    elif RAY_VERSION >= (2, 0, 0):
        assert ds._dataset_format() == "simple", "Ray Dataset format should be simple because it has Python objects"

    rows = [row for row in ds.iter_rows()]
    assert rows == [
        {"intcol": 1, "strcol": "a", "pycol": MyObj(1)},
        {"intcol": 2, "strcol": "b", "pycol": MyObj(2)},
        {"intcol": 3, "strcol": "c", "pycol": MyObj(3)},
    ]


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_ray_dataset_with_numpy(n_partitions: int):
    df = DataFrame.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("npcol", df["intcol"].apply(lambda x: np.ones((x, 3))))
    ds = df.to_ray_dataset()

    if RAY_VERSION >= (2, 2, 0):
        assert (
            ds.dataset_format() == "arrow"
        ), "Ray Dataset format should be arrow because it uses a Tensor extension type"
    elif RAY_VERSION >= (2, 0, 0):
        assert (
            ds._dataset_format() == "simple"
        ), "In old versions of Ray, we drop down to `simple` format because ArrowTensorType is not compatible with ragged tensors"

    rows = [dict(row) for row in ds.iter_rows()]
    np.testing.assert_equal(
        rows,
        [
            {"intcol": 1, "strcol": "a", "npcol": np.ones((1, 3))},
            {"intcol": 2, "strcol": "b", "npcol": np.ones((2, 3))},
            {"intcol": 3, "strcol": "c", "npcol": np.ones((3, 3))},
        ],
    )


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_ray_dataset(n_partitions: int):
    ds = ray.data.range(8)
    ds = ds.map(lambda i: {"int": i, "np": np.ones((3, 3))}).repartition(n_partitions)

    df = DataFrame.from_ray_dataset(ds)
    np.testing.assert_equal(
        df.to_pydict(),
        {
            "int": list(range(8)),
            "np": [np.ones((3, 3)) for i in range(8)],
        },
    )
