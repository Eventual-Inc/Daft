from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pac
import pytest
import ray

import daft
from daft import DataType
from daft.context import get_context

RAY_VERSION = tuple(int(s) for s in ray.__version__.split(".")[0:3])


class MyObj:
    def __init__(self, x: int):
        self._x = x

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, MyObj) and self._x == other._x


DATA = {
    "intcol": [1, 2, 3],
    "strcol": ["a", "b", "c"],
}


def _row_to_pydict(row: ray.data.row.TableRow | dict) -> dict:
    if isinstance(row, dict):
        return row
    return row.as_pydict()


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_ray_dataset_all_arrow(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("floatcol", df["intcol"].cast(DataType.float64()))
    ds = df.to_ray_dataset()

    if RAY_VERSION < (2, 4, 0):
        if RAY_VERSION >= (2, 2, 0):
            assert ds.dataset_format() == "arrow", "Ray Dataset format should be arrow"
        elif RAY_VERSION >= (2, 0, 0):
            assert ds._dataset_format() == "arrow", "Ray Dataset format should be arrow"

    rows = sorted([_row_to_pydict(row) for row in ds.iter_rows()], key=lambda r: r["intcol"])
    assert rows == sorted(
        [
            {"intcol": 1, "strcol": "a", "floatcol": 1.0},
            {"intcol": 2, "strcol": "b", "floatcol": 2.0},
            {"intcol": 3, "strcol": "c", "floatcol": 3.0},
        ],
        key=lambda r: r["intcol"],
    )


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.skipif(
    RAY_VERSION >= (2, 5, 0), reason="Ray Datasets versions >= 2.5.0 no longer support Python objects as rows"
)
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_ray_dataset_with_py(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("pycol", df["intcol"].apply(lambda x: MyObj(x), DataType.python()))
    ds = df.to_ray_dataset()

    if RAY_VERSION < (2, 4, 0):
        if RAY_VERSION >= (2, 2, 0):
            assert ds.dataset_format() == "simple", "Ray Dataset format should be simple because it has Python objects"
        elif RAY_VERSION >= (2, 0, 0):
            assert ds._dataset_format() == "simple", "Ray Dataset format should be simple because it has Python objects"

    rows = sorted([row for row in ds.iter_rows()], key=lambda r: r["intcol"])
    assert rows == sorted(
        [
            {"intcol": 1, "strcol": "a", "pycol": MyObj(1)},
            {"intcol": 2, "strcol": "b", "pycol": MyObj(2)},
            {"intcol": 3, "strcol": "c", "pycol": MyObj(3)},
        ],
        key=lambda r: r["intcol"],
    )


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_ray_dataset_with_numpy(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    shape = (3, 3)
    df = df.with_column("npcol", df["intcol"].apply(lambda _: np.ones(shape), DataType.tensor(DataType.int64(), shape)))
    ds = df.to_ray_dataset()

    if RAY_VERSION < (2, 4, 0):
        if RAY_VERSION >= (2, 2, 0):
            assert (
                ds.dataset_format() == "arrow"
            ), "Ray Dataset format should be arrow because it uses a Tensor extension type"
        elif RAY_VERSION >= (2, 0, 0):
            assert (
                ds._dataset_format() == "arrow"
            ), "Ray Dataset format should be arrow because it uses a Tensor extension type"

    rows = sorted([_row_to_pydict(row) for row in ds.iter_rows()], key=lambda r: r["intcol"])
    np.testing.assert_equal(
        rows,
        sorted(
            [
                {"intcol": 1, "strcol": "a", "npcol": np.ones((3, 3))},
                {"intcol": 2, "strcol": "b", "npcol": np.ones((3, 3))},
                {"intcol": 3, "strcol": "c", "npcol": np.ones((3, 3))},
            ],
            key=lambda r: r["intcol"],
        ),
    )


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.skipif(RAY_VERSION < (2, 2, 0), reason="Variable-shaped tensor columns not supported in Ray < 2.1.0")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_ray_dataset_with_numpy_variable_shaped(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("npcol", df["intcol"].apply(lambda x: np.ones((x, 3)), DataType.tensor(DataType.int64())))
    ds = df.to_ray_dataset()

    if RAY_VERSION < (2, 4, 0):
        if RAY_VERSION >= (2, 2, 0):
            assert (
                ds.dataset_format() == "arrow"
            ), "Ray Dataset format should be arrow because it uses a Tensor extension type"
        elif RAY_VERSION >= (2, 0, 0):
            assert (
                ds._dataset_format() == "simple"
            ), "In old versions of Ray, we drop down to `simple` format because ArrowTensorType is not compatible with ragged tensors"

    rows = sorted([_row_to_pydict(row) for row in ds.iter_rows()], key=lambda r: r["intcol"])
    np.testing.assert_equal(
        rows,
        sorted(
            [
                {"intcol": 1, "strcol": "a", "npcol": np.ones((1, 3))},
                {"intcol": 2, "strcol": "b", "npcol": np.ones((2, 3))},
                {"intcol": 3, "strcol": "c", "npcol": np.ones((3, 3))},
            ],
            key=lambda r: r["intcol"],
        ),
    )


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_ray_dataset_all_arrow(n_partitions: int):
    def add_float(table: pa.Table) -> pa.Table:
        return table.append_column("floatcol", table["intcol"].cast(pa.float64()))

    table = pa.table(DATA)
    ds = ray.data.from_arrow(table).map_batches(add_float, batch_format="pyarrow").repartition(n_partitions)

    if RAY_VERSION < (2, 4, 0):
        if RAY_VERSION >= (2, 2, 0):
            assert ds.dataset_format() == "arrow", "Ray Dataset format should be arrow"
        elif RAY_VERSION >= (2, 0, 0):
            assert ds._dataset_format() == "arrow", "Ray Dataset format should be arrow"

    df = daft.from_ray_dataset(ds)
    # Sort data since partition ordering in Datasets is not deterministic.
    out_table = df.to_arrow()
    out_table = out_table.take(pac.sort_indices(out_table, sort_keys=[("intcol", "ascending")]))
    expected_table = add_float(table).cast(
        pa.schema(
            [
                ("intcol", pa.int64()),
                ("strcol", pa.large_string()),
                ("floatcol", pa.float64()),
            ]
        )
    )
    assert out_table.equals(expected_table), (out_table, expected_table)


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_ray_dataset_simple(n_partitions: int):
    ds = ray.data.range(8, parallelism=n_partitions)

    df = daft.from_ray_dataset(ds)
    # Sort data since partition ordering in Datasets is not deterministic.
    out = df.to_pydict()
    key = "id" if RAY_VERSION >= (2, 5, 0) else "value"
    assert list(out.keys()) == [key]
    assert sorted(out[key]) == list(range(8))


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_ray_dataset_tensor(n_partitions: int):
    ds = ray.data.range(8)
    ds = (
        ds.map(lambda d: {"int": d["id"], "np": np.ones((3, 3))})
        if RAY_VERSION >= (2, 5, 0)
        else ds.map(lambda i: {"int": i, "np": np.ones((3, 3))})
    )
    ds = ds.repartition(n_partitions)

    df = daft.from_ray_dataset(ds)
    out = df.to_pydict()
    assert out.keys() == {"int", "np"}
    # Sort data since partition ordering in Datasets is not deterministic.
    out_sorted_rows = sorted(list(zip(out["int"], out["np"])), key=lambda row: row[0])
    int_col, np_col = zip(*out_sorted_rows)
    out_sorted = {"int": int_col, "np": np_col}
    expected = {
        "int": list(range(8)),
        "np": [np.ones((3, 3)) for i in range(8)],
    }
    np.testing.assert_equal(out_sorted, expected)


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_ray_dataset_pandas(n_partitions: int):
    def add_float(df: pd.DataFrame) -> pd.DataFrame:
        df["floatcol"] = df["intcol"].astype(float)
        return df

    pd_df = pd.DataFrame(DATA)

    ds = ray.data.from_pandas(pd_df).map_batches(add_float).repartition(n_partitions)

    if RAY_VERSION < (2, 4, 0):
        if RAY_VERSION >= (2, 2, 0):
            assert ds.dataset_format() == "pandas", "Ray Dataset format should be pandas"
        elif RAY_VERSION >= (2, 0, 0):
            assert ds._dataset_format() == "pandas", "Ray Dataset format should be pandas"

    df = daft.from_ray_dataset(ds)
    expected_df = add_float(pd_df)
    pd.testing.assert_frame_equal(df.to_pandas(), expected_df)


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_ray_dataset_preview(n_partitions: int):
    ds = ray.data.range(3, parallelism=n_partitions)

    df = daft.from_ray_dataset(ds)
    assert len(df) == 3
    assert len(df._preview.preview_partition) == 3


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_ray_dataset_data_longer_than_preview(n_partitions: int):
    ds = ray.data.range(10, parallelism=n_partitions)

    df = daft.from_ray_dataset(ds)
    assert len(df) == 10
    assert len(df._preview.preview_partition) == 8
