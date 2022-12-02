from __future__ import annotations

import argparse
import os
from itertools import chain

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pac
import pytest

from daft.context import get_context
from daft.runners.blocks import ArrowDataBlock, PyListDataBlock
from daft.runners.partitioning import PartitionSet
from daft.types import ExpressionType


@pytest.fixture(scope="session", autouse=True)
def sentry_telemetry():
    if os.getenv("CI"):
        import sentry_sdk

        sentry_sdk.init(
            dsn="https://7e05ae17fdad482a82cd2e79e94d9f51@o1383722.ingest.sentry.io/6701254",
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            # We recommend adjusting this value in production.
            traces_sample_rate=1.0,
            # traces_sampler=True,
        )
        sentry_sdk.set_tag("CI", os.environ["CI"])
        sentry_sdk.set_tag("DAFT_RUNNER", get_context().runner_config.name)
    else:
        ...
    yield


def pytest_addoption(parser):
    parser.addoption("--run_conda", action="store_true", default=False, help="run tests that require conda")
    parser.addoption("--run_docker", action="store_true", default=False, help="run tests that require docker")
    parser.addoption(
        "--run_tdd", action="store_true", default=False, help="run tests that are marked for Test Driven Development"
    )
    parser.addoption(
        "--run_tdd_all",
        action="store_true",
        default=False,
        help="run tests that are marked for Test Driven Development (including low priority)",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "conda: mark test as requiring conda to run")
    config.addinivalue_line("markers", "docker: mark test as requiring docker to run")
    config.addinivalue_line("markers", "tdd: mark test as for TDD in active development")
    config.addinivalue_line("markers", "tdd_all: mark test as for TDD but not in active development")
    config.addinivalue_line("markers", "tpch: mark as a tpch test")


def pytest_collection_modifyitems(config, items):
    marks = {
        "conda": pytest.mark.skip(reason="need --run_conda option to run"),
        "docker": pytest.mark.skip(reason="need --run_docker option to run"),
        "tdd": pytest.mark.skip(reason="need --run_tdd option to run"),
        "tdd_all": pytest.mark.skip(reason="need --run_tdd_all option to run"),
    }
    for item in items:
        for keyword in marks:
            if keyword in item.keywords and not config.getoption(f"--run_{keyword}"):
                item.add_marker(marks[keyword])


def run_tdd():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_tdd", action="store_true")
    args, _ = parser.parse_known_args()
    return args.run_tdd


def assert_df_column_type(
    partition_set: PartitionSet,
    colname: str,
    type_: type,
):
    """Asserts that all tiles for a given column is of the implementation, given a type"""
    et = ExpressionType.from_py_type(type_)
    vpart = partition_set._get_merged_vpartition()
    blocks = [tile.block for tile in vpart.columns.values() if tile.column_name == colname]
    assert len(blocks) == 1, f"cannot find block with provided colname {colname}"
    block = blocks[0]

    if ExpressionType.is_py(et):
        assert isinstance(block, PyListDataBlock)
    elif ExpressionType.is_primitive(et):
        assert isinstance(block, ArrowDataBlock)
        assert isinstance(block.data, pa.ChunkedArray)
        assert block.data.type == et.to_arrow_type()
    else:
        raise NotImplementedError(f"assert_df_column_type not implemented for {et}")


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


def assert_list_columns_equal(
    daft_columns: dict[str, list[list]],
    expected_columns: dict[str, list[list]],
    daft_col_keys: list | None = None,
    assert_list_element_ordering: bool = False,
) -> None:
    """Assert that two dictionaries of columns with list elements are equal.

    Special cased because:
        1. list element column implementation is currently not in Arrow;
        2. individual elements need to be sorted for the equality check.
    """
    if not assert_list_element_ordering:
        for col in chain(daft_columns.values(), expected_columns.values()):
            for elem in col:
                elem.sort(key=lambda x: (x is None, x))

    if daft_col_keys:
        for key in daft_columns:
            daft_columns[key] = [elem for key, elem in sorted(zip(daft_col_keys, daft_columns[key]))]

    assert daft_columns == expected_columns


def assert_arrow_equals(
    daft_columns: dict[str, pa.Array | pa.ChunkedArray],
    expected_columns: dict[str, pa.Array | pa.ChunkedArray],
    sort_key: str | list[str] = "id",
    assert_ordering: bool = False,
):
    """Asserts that two dictionaries of columns are equal.
    By default, we do not assert that the ordering is equal and will sort the columns according to `sort_key`.
    However, if asserting on ordering is intended behavior, set `assert_ordering=True` and this function will
    no longer run sorting before running the equality comparison.
    """
    daft_sort_indices: pa.Array | None = None
    expected_sort_indices: pa.Array | None = None
    if not assert_ordering:
        sort_key_list: list[str] = [sort_key] if isinstance(sort_key, str) else sort_key
        for key in sort_key_list:
            assert key in daft_columns, (
                f"DaFt Data missing key: {key}\nNOTE: This doesn't necessarily mean your code is "
                "breaking, but our testing utilities require sorting on this key in order to compare your "
                "Dataframe against the expected Data."
            )
            assert key in expected_columns, (
                f"Expectred Data missing key: {key}\nNOTE: This doesn't necessarily mean your code is "
                "breaking, but our testing utilities require sorting on this key in order to compare your "
                "Dataframe against the expected Data."
            )
        daft_sort_indices = pac.sort_indices(
            pa.Table.from_pydict(daft_columns), sort_keys=[(k, "ascending") for k in sort_key_list]
        )
        expected_sort_indices = pac.sort_indices(
            pa.Table.from_pydict(expected_columns), sort_keys=[(k, "ascending") for k in sort_key_list]
        )

    assert sorted(daft_columns.keys()) == sorted(
        expected_columns.keys()
    ), f"Found {daft_columns.keys()} expected {expected_columns.keys()}"
    columns = list(daft_columns.keys())

    for coldict, sort_indices in ((daft_columns, daft_sort_indices), (expected_columns, expected_sort_indices)):
        for col in columns:
            arr: pa.Array
            if isinstance(coldict[col], pa.ChunkedArray) and coldict[col].num_chunks > 0:
                arr = coldict[col].combine_chunks()
            elif isinstance(coldict[col], pa.ChunkedArray):
                arr = pa.array([], type=coldict[col].type)
            else:
                arr = coldict[col]
            arr = pac.array_take(arr, sort_indices) if sort_indices is not None else arr
            coldict[col] = arr

    for col in columns:
        daft_arr = daft_columns[col]
        expected_arr = expected_columns[col]

        assert len(daft_arr) == len(expected_arr), f"{col} failed length check: {len(daft_arr)} vs {len(expected_arr)}"

        # Float types NaNs do not equal each other, so we special-case floating arrays by checking that they are NaN in
        # the same indices, and then filtering out the NaN indices when checking equality
        assert expected_arr.type == daft_arr.type, f"{col} failed type check: {daft_arr.type} vs {expected_arr.type}"
        nan_indices = None
        if pa.types.is_floating(expected_arr.type):
            assert pac.all(
                pac.equal(pac.is_nan(daft_arr), pac.is_nan(expected_arr))
            ), f"{col} failed NaN check: {daft_arr} vs {expected_arr}"
            nan_indices = pac.is_nan(daft_arr)

        # pac.equal not implemented for null arrays
        arr_eq = (
            pac.equal(daft_arr, expected_arr)
            if not pa.types.is_null(daft_arr.type)
            else pa.array([True] * len(daft_arr))
        )

        if nan_indices is not None:
            arr_eq = pac.array_filter(arr_eq, pac.invert(nan_indices))

        # No elements to compare
        if pac.all(arr_eq).as_py() is None:
            continue

        assert pac.all(
            arr_eq
        ).as_py(), f"{col} failed equality check: {daft_arr} vs {expected_arr} (equality array={arr_eq})"
