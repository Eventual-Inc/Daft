from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from daft.runners.blocks import ArrowDataBlock, PyListDataBlock
from daft.runners.partitioning import PartitionSet
from daft.types import ExpressionType


def pytest_addoption(parser):
    parser.addoption("--run_conda", action="store_true", default=False, help="run tests that require conda")
    parser.addoption("--run_docker", action="store_true", default=False, help="run tests that require docker")


def pytest_configure(config):
    config.addinivalue_line("markers", "conda: mark test as requiring conda to run")
    config.addinivalue_line("markers", "docker: mark test as requiring docker to run")


def pytest_collection_modifyitems(config, items):
    marks = {
        "conda": pytest.mark.skip(reason="need --run_conda option to run"),
        "docker": pytest.mark.skip(reason="need --run_docker option to run"),
    }
    for item in items:
        for keyword in marks:
            if keyword in item.keywords and not config.getoption(f"--run_{keyword}"):
                item.add_marker(marks[keyword])


def assert_df_column_type(
    partition_set: PartitionSet,
    colname: str,
    type_: type,
):
    """Asserts that all tiles for a given column is of the implementation, given a type"""
    et = ExpressionType.python(type_)
    vpart = partition_set._get_merged_vpartition()
    blocks = [tile.block for tile in vpart.columns.values() if tile.column_name == colname]
    assert len(blocks) == 1, f"cannot find block with provided colname {colname}"
    block = blocks[0]

    if et._is_python_type():
        assert isinstance(block, PyListDataBlock)
    else:
        assert isinstance(block, ArrowDataBlock)
        assert isinstance(block.data, pa.ChunkedArray)
        assert block.data.type == et.to_arrow_type()


def assert_pydict_equals(
    daft_pydict: dict[str, list],
    expected_pydict: dict[str, list],
    sort_key: str | list[str] = "Unique Key",
    assert_ordering: bool = False,
):
    """Asserts the equality of two python dictionaries containing Dataframe data as Python objects.

    Args:
        daft_pydict (dict[str, list]): Pydict retrieved from a Daft DataFrame.to_pydict()
        expected_pydict (dict[str, list]): expected data
        sort_key (str | list[str], optional): column to sort data by. Defaults to "Unique Key".
        assert_ordering (bool, optional): whether to check sorting order. Defaults to False.
    """
    if len(daft_pydict) == 0 and len(expected_pydict) == 0:
        return
    elif len(daft_pydict[list(daft_pydict.keys())[0]]) == 0:
        assert len(expected_pydict[list(expected_pydict.keys())[0]]) == 0
        return

    sort_key = [sort_key] if isinstance(sort_key, str) else sort_key
    daft_clean_pydict, expected_clean_pydict = (daft_pydict, expected_pydict)

    if not assert_ordering:
        sorted_pydicts = []
        for pydict in (daft_clean_pydict, expected_clean_pydict):
            pydict_sort_keys = list(zip(*[pydict[key] for key in sort_key]))
            pydict_sort_indices = sorted(
                range(len(pydict_sort_keys)),
                key=lambda x: (tuple(obj is not None for obj in pydict_sort_keys[x]), pydict_sort_keys[x]),
            )
            sorted_pydict = {k: [data[pydict_sort_indices[i]] for i in range(len(data))] for k, data in pydict.items()}
            sorted_pydicts.append(sorted_pydict)
        daft_clean_pydict, expected_clean_pydict = sorted_pydicts

    # Replace float("nan") with a sentinel value that will be equal to itself
    class SentinelNaN:
        def __repr__(self) -> str:
            return "NaN"

    float_nan = SentinelNaN()
    replaced_nan_pydicts = []
    for pydict in (daft_clean_pydict, expected_clean_pydict):
        replaced_nan_pydict = {
            k: [float_nan if isinstance(v, float) and np.isnan(v) else v for v in data] for k, data in pydict.items()
        }
        replaced_nan_pydicts.append(replaced_nan_pydict)
    daft_clean_pydict, expected_clean_pydict = replaced_nan_pydicts

    assert daft_clean_pydict == expected_clean_pydict


def assert_df_equals(
    daft_df: pd.DataFrame,
    pd_df: pd.DataFrame,
    sort_key: str | list[str] = "Unique Key",
    assert_ordering: bool = False,
):
    """Asserts that a Daft Dataframe is equal to a Pandas Dataframe.

    By default, we do not assert that the ordering is equal and will sort dataframes according to `sort_key`.
    However, if asserting on ordering is intended behavior, set `assert_ordering=True` and this function will
    no longer run sorting before running the equality comparison.
    """
    daft_pd_df = daft_df.reset_index(drop=True).reindex(sorted(daft_df.columns), axis=1)
    pd_df = pd_df.reset_index(drop=True).reindex(sorted(pd_df.columns), axis=1)

    # If we are not asserting on the ordering being equal, we run a sort operation on both dataframes using the provided sort key
    if not assert_ordering:
        sort_key_list: list[str] = [sort_key] if isinstance(sort_key, str) else sort_key
        for key in sort_key_list:
            assert key in daft_pd_df.columns, (
                f"DaFt Dataframe missing key: {key}\nNOTE: This doesn't necessarily mean your code is "
                "breaking, but our testing utilities require sorting on this key in order to compare your "
                "Dataframe against the expected Pandas Dataframe."
            )
            assert key in pd_df.columns, (
                f"Pandas Dataframe missing key: {key}\nNOTE: This doesn't necessarily mean your code is "
                "breaking, but our testing utilities require sorting on this key in order to compare your "
                "Dataframe against the expected Pandas Dataframe."
            )
        daft_pd_df = daft_pd_df.sort_values(by=sort_key_list).reset_index(drop=True)
        pd_df = pd_df.sort_values(by=sort_key_list).reset_index(drop=True)

    assert sorted(daft_pd_df.columns) == sorted(pd_df.columns), f"Found {daft_pd_df.columns} expected {pd_df.columns}"
    for col in pd_df.columns:
        df_series = daft_pd_df[col]
        pd_series = pd_df[col]

        try:
            pd.testing.assert_series_equal(df_series, pd_series)
        except AssertionError:
            print(f"Failed assertion for col: {col}")
            raise
