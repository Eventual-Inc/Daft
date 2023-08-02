from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest


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
