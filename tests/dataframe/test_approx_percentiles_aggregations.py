from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

from daft import col


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_approx_percentiles_global(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "id": [1, 2, 3],
            "values": [1, 2, 3],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.agg(
        [
            col("values").approx_percentiles(0.5).alias("percentiles"),
        ]
    )
    daft_cols = daft_df.to_pydict()
    pd.testing.assert_series_equal(pd.Series(daft_cols["percentiles"]), pd.Series([2.0]), check_exact=False, rtol=0.02)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_approx_percentiles_global_with_nulls(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "id": [1, 2, 3, 4],
            "values": [1, None, 2, 3],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.agg(
        [
            col("values").approx_percentiles(0.5).alias("percentiles"),
        ]
    )
    daft_cols = daft_df.to_pydict()
    pd.testing.assert_series_equal(pd.Series(daft_cols["percentiles"]), pd.Series([2.0]), check_exact=False, rtol=0.02)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_approx_percentiles_global_all_nulls(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "id": [1, 2, 3],
            "values": pa.array([None, None, None], type=pa.int64()),
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.agg(
        [
            col("values").approx_percentiles(0.5).alias("percentiles"),
        ]
    )
    daft_cols = daft_df.to_pydict()
    pd.testing.assert_series_equal(pd.Series(daft_cols["percentiles"]), pd.Series([None]))


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_approx_percentiles_groupby(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "id": [1, 1, 1, 2],
            "values": [1, 2, 3, 2],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby("id").agg(
        [
            col("values").approx_percentiles(0.5).alias("percentiles"),
        ]
    )
    daft_cols = daft_df.to_pydict()
    pd.testing.assert_series_equal(
        pd.Series(daft_cols["percentiles"]), pd.Series([2.0, 2.0]), check_exact=False, rtol=0.02
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 7])
def test_approx_percentiles_groupby_with_nulls(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "id": [1, 1, 1, 2, 2, 3],
            "values": [1, 2, 3, 2, None, None],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby("id").agg(
        [
            col("values").approx_percentiles(0.5).alias("percentiles"),
        ]
    )
    daft_cols = daft_df.to_pandas()
    pd.testing.assert_series_equal(
        daft_cols.sort_values("id")["percentiles"],
        pd.Series([2.0, 2.0, None]),
        check_exact=False,
        rtol=0.02,
        check_index=False,
        check_names=False,
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_approx_percentiles_groupby_all_nulls(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "id": [1, 1, 2],
            "values": pa.array([None, None, None], type=pa.int64()),
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby("id").agg(
        [
            col("values").approx_percentiles(0.5).alias("percentiles"),
        ]
    )
    daft_cols = daft_df.to_pydict()
    assert daft_cols["percentiles"] == [None, None]
