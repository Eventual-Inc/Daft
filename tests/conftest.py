from __future__ import annotations

import uuid

import pandas as pd
import pyarrow as pa
import pytest

import daft
from daft.table import MicroPartition

# import all conftest
from tests.integration.io.conftest import *


def pytest_addoption(parser):
    parser.addoption(
        "--credentials",
        action="store_true",
        help="Whether or not the current environment has access to remote storage credentials",
    )


@pytest.fixture(scope="session", autouse=True)
def set_execution_configs():
    """Sets global Daft config for testing"""
    daft.set_execution_config(
        # Disables merging of ScanTasks
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test that runs with external dependencies"
    )


class UuidType(pa.ExtensionType):
    NAME = "daft.uuid"

    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(), self.NAME)

    def __arrow_ext_serialize__(self):
        return b""

    def __reduce__(self):
        return UuidType, ()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


@pytest.fixture
def uuid_ext_type() -> UuidType:
    ext_type = UuidType()
    pa.register_extension_type(ext_type)
    yield ext_type
    pa.unregister_extension_type(ext_type.NAME)


@pytest.fixture(
    scope="function",
    params=[
        # Convert the data into Arrow and then load as in-memory Arrow data
        "arrow",
        # Dump the data as Parquet and load it as Parquet (will trigger "Unloaded" MicroPartitions)
        "parquet",
    ],
)
def data_source(request):
    return request.param


@pytest.fixture(scope="function")
def join_strategy(request):
    # Modifies the join strategy parametrization to toggle a specialized presorting path for sort-merge joins, where
    # each side of the join is sorted such that their boundaries will align.
    if request.param != "sort_merge_aligned_boundaries":
        yield request.param
    else:
        old_execution_config = daft.context.get_context().daft_execution_config
        try:
            daft.set_execution_config(
                sort_merge_join_sort_with_aligned_boundaries=True,
            )
            yield "sort_merge"
        finally:
            daft.set_execution_config(old_execution_config)


@pytest.fixture(scope="function")
def make_df(data_source, tmp_path) -> daft.Dataframe:
    """Makes a dataframe when provided with data"""

    def _make_df(
        data: pa.Table | dict | list,
        repartition: int = 1,
        repartition_columns: list[str] = [],
    ) -> daft.DataFrame:
        pa_table: pa.Table
        if isinstance(data, pa.Table):
            pa_table = data
        elif isinstance(data, dict):
            pa_table = pa.table(data)
        elif isinstance(data, list):
            data = {k: [d[k] for d in data] for k in data[0].keys()}
            pa_table = pa.table(data)
        else:
            raise NotImplementedError(f"make_df not implemented for input type: {type(data)}")

        variant = data_source
        if variant == "arrow":
            df = daft.from_arrow(pa_table)
            if repartition != 1:
                return df.repartition(repartition, *repartition_columns)
            return df
        elif variant == "parquet":
            import pyarrow.parquet as papq

            name = str(uuid.uuid4())
            daft_table = MicroPartition.from_arrow(pa_table)
            partitioned_tables = (
                daft_table.partition_by_random(repartition, 0)
                if len(repartition_columns) == 0
                else daft_table.partition_by_hash([daft.col(c) for c in repartition_columns], repartition)
            )
            for i, tbl in enumerate(partitioned_tables):
                tmp_file = tmp_path / (name + f"-{i}")
                papq.write_table(tbl.to_arrow(), str(tmp_file))
            return daft.read_parquet(str(tmp_path) + f"/{name}-*")
        else:
            raise NotImplementedError(f"make_df not implemented for: {variant}")

    yield _make_df


def assert_df_equals(
    daft_df: pd.DataFrame,
    pd_df: pd.DataFrame,
    sort_key: str | list[str] = "Unique Key",
    assert_ordering: bool = False,
    check_dtype: bool = True,
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
            pd.testing.assert_series_equal(df_series, pd_series, check_dtype=check_dtype)
        except AssertionError:
            print(f"Failed assertion for col: {col}")
            raise
