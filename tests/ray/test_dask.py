from __future__ import annotations

from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

import daft
from daft import DataType
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
def test_to_dask_dataframe_all_arrow(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("floatcol", df["intcol"].cast(DataType.float64()))
    ddf = df.to_dask_dataframe()

    rows = sorted(ddf.compute().to_dict("records"), key=lambda r: r["intcol"])
    assert rows == sorted(
        [
            {"intcol": 1, "strcol": "a", "floatcol": 1.0},
            {"intcol": 2, "strcol": "b", "floatcol": 2.0},
            {"intcol": 3, "strcol": "c", "floatcol": 3.0},
        ],
        key=lambda r: r["intcol"],
    )


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_dask_dataframe_all_arrow_with_schema(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("floatcol", df["intcol"].cast(DataType.float64()))
    ddf = df.to_dask_dataframe({"intcol": np.int64, "strcol": np.str_, "floatcol": np.float64})

    rows = sorted(ddf.compute().to_dict("records"), key=lambda r: r["intcol"])
    assert rows == sorted(
        [
            {"intcol": 1, "strcol": "a", "floatcol": 1.0},
            {"intcol": 2, "strcol": "b", "floatcol": 2.0},
            {"intcol": 3, "strcol": "c", "floatcol": 3.0},
        ],
        key=lambda r: r["intcol"],
    )


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_dask_dataframe_with_py(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("pycol", df["intcol"].apply(lambda x: MyObj(x), DataType.python()))
    ddf = df.to_dask_dataframe()

    rows = sorted(ddf.compute().to_dict("records"), key=lambda r: r["intcol"])
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
def test_to_dask_dataframe_with_numpy(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("npcol", df["intcol"].apply(lambda _: np.ones((3, 3)), DataType.python()))
    ddf = df.to_dask_dataframe()

    rows = sorted(ddf.compute().to_dict("records"), key=lambda r: r["intcol"])
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
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_to_dask_dataframe_with_numpy_variable_shaped(n_partitions: int):
    df = daft.from_pydict(DATA).repartition(n_partitions)
    df = df.with_column("npcol", df["intcol"].apply(lambda x: np.ones((x, 3)), DataType.python()))
    ddf = df.to_dask_dataframe()

    rows = sorted(ddf.compute().to_dict("records"), key=lambda r: r["intcol"])
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
def test_from_dask_dataframe_all_arrow(n_partitions: int):
    df = pd.DataFrame(DATA)
    df["floatcol"] = df["intcol"].astype(float)
    ddf = dd.from_pandas(df, npartitions=n_partitions)

    daft_df = daft.from_dask_dataframe(ddf)
    out_df = daft_df.to_pandas()
    pd.testing.assert_frame_equal(out_df, df)


# @pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.skip()  # dask doesn't seem to work with object types anymore
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_dask_dataframe_tensor(n_partitions: int):
    df = pd.DataFrame(DATA)
    df["tensor"] = pd.Series([np.ones((2, 2)) for _ in range(len(df))], dtype=object)
    ddf = dd.from_pandas(df, npartitions=n_partitions)

    daft_df = daft.from_dask_dataframe(ddf)
    out_df = daft_df.to_pandas()
    pd.testing.assert_frame_equal(out_df, df)


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_dask_dataframe_preview(n_partitions: int):
    df = pd.DataFrame(DATA)
    ddf = dd.from_pandas(df, npartitions=n_partitions)

    daft_df = daft.from_dask_dataframe(ddf)
    assert len(daft_df) == 3
    assert len(daft_df._preview.preview_partition) == 3


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="Needs to run on Ray runner")
@pytest.mark.parametrize("n_partitions", [1, 2])
def test_from_dask_dataframe_data_longer_than_preview(n_partitions: int):
    df = pd.DataFrame(
        {
            "intcol": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "strcol": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=n_partitions)

    daft_df = daft.from_dask_dataframe(ddf)
    assert len(daft_df) == 10
    assert len(daft_df._preview.preview_partition) == 8
