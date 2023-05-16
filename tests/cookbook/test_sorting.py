from __future__ import annotations

import numpy as np
import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals


def test_sorted_by_expr(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Sort by a column that undergoes an expression"""
    daft_df = daft_df.repartition(repartition_nparts)
    daft_sorted_df = daft_df.sort(((col("Unique Key") % 2) == 0).if_else(col("Unique Key"), col("Unique Key") * -1))
    daft_sorted_pd_df = daft_sorted_df.to_pandas()

    service_requests_csv_pd_df["tmp"] = service_requests_csv_pd_df["Unique Key"]
    service_requests_csv_pd_df["tmp"] = np.where(
        service_requests_csv_pd_df["tmp"] % 2 == 0,
        service_requests_csv_pd_df["tmp"],
        service_requests_csv_pd_df["tmp"] * -1,
    )
    service_requests_csv_pd_df = service_requests_csv_pd_df.sort_values("tmp", ascending=True)
    service_requests_csv_pd_df = service_requests_csv_pd_df.drop(["tmp"], axis=1)

    assert_df_equals(
        daft_sorted_pd_df,
        service_requests_csv_pd_df,
        assert_ordering=True,
    )


@pytest.mark.parametrize(
    "sort_keys",
    [
        pytest.param(["Unique Key"], id="NumSortKeys:1"),
        pytest.param(["Borough", "Unique Key"], id="NumSortKeys:2"),
    ],
)
def test_get_sorted(daft_df, service_requests_csv_pd_df, repartition_nparts, sort_keys):
    """Sort by a column"""
    daft_df = daft_df.repartition(repartition_nparts)
    daft_sorted_df = daft_df.sort([col(k) for k in sort_keys], desc=True)
    daft_sorted_pd_df = daft_sorted_df.to_pandas()
    assert_df_equals(
        daft_sorted_pd_df,
        service_requests_csv_pd_df.sort_values(by=sort_keys, ascending=False),
        assert_ordering=True,
    )


@pytest.mark.parametrize(
    "sort_keys",
    [
        pytest.param(["Unique Key"], id="NumSortKeys:1"),
        pytest.param(["Borough", "Unique Key"], id="NumSortKeys:2"),
    ],
)
def test_get_sorted_top_n(daft_df, service_requests_csv_pd_df, repartition_nparts, sort_keys):
    """Sort by a column"""
    daft_df = daft_df.repartition(repartition_nparts)
    daft_sorted_df = daft_df.sort([col(k) for k in sort_keys], desc=True).limit(100)
    daft_sorted_pd_df = daft_sorted_df.to_pandas()

    assert_df_equals(
        daft_sorted_pd_df,
        service_requests_csv_pd_df.sort_values(by=sort_keys, ascending=False).head(100),
        assert_ordering=True,
    )


@pytest.mark.parametrize(
    "sort_keys",
    [
        pytest.param(["Borough", "Unique Key"], id="NumSortKeys:2"),
    ],
)
def test_get_sorted_top_n_flipped_desc(daft_df, service_requests_csv_pd_df, repartition_nparts, sort_keys):
    """Sort by a column"""
    daft_df = daft_df.repartition(repartition_nparts)
    desc_list = [True]
    for i in range(len(sort_keys) - 1):
        desc_list.append(not desc_list[-1])
    daft_sorted_df = daft_df.sort([col(k) for k in sort_keys], desc=desc_list).limit(100)
    daft_sorted_pd_df = daft_sorted_df.to_pandas()

    assert_df_equals(
        daft_sorted_pd_df,
        service_requests_csv_pd_df.sort_values(by=sort_keys, ascending=[not b for b in desc_list]).head(100),
        assert_ordering=True,
    )


@pytest.mark.parametrize(
    "daft_df_ops",
    [
        pytest.param(
            lambda daft_df: daft_df.sort(col("Unique Key"), desc=True).select(col("Unique Key"), col("Complaint Type")),
            id="sort..select",
        ),
        pytest.param(
            lambda daft_df: daft_df.select(col("Unique Key"), col("Complaint Type")).sort(col("Unique Key"), desc=True),
            id="select..sort",
        ),
    ],
)
def test_get_sorted_top_n_projected(daft_df_ops, daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Sort by a column and retrieve specific columns from the top N results"""
    daft_df = daft_df.repartition(repartition_nparts)
    expected = service_requests_csv_pd_df.sort_values(by="Unique Key", ascending=False)[
        ["Unique Key", "Complaint Type"]
    ]
    daft_df = daft_df_ops(daft_df)
    daft_pd_pf = daft_df.to_pandas()
    assert_df_equals(
        daft_pd_pf,
        expected,
        assert_ordering=True,
    )
