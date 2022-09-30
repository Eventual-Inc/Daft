import argparse
import os
from typing import List, Type, Union

import pandas as pd
import pyarrow as pa
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
    type_: Type,
):
    """Asserts that all tiles for a given column is of the implementation, given a type"""
    et = ExpressionType.from_py_type(type_)
    vpartitions = partition_set._get_all_vpartitions()
    for vpart in vpartitions:
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
    sort_key: Union[str, List[str]] = "Unique Key",
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
        sort_key_list: List[str] = [sort_key] if isinstance(sort_key, str) else sort_key
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
