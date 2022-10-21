from __future__ import annotations

import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_repartition,
    parametrize_sort_desc,
)


@parametrize_service_requests_csv_repartition
@parametrize_sort_desc("sort_desc")
@pytest.mark.parametrize(
    "sort_keys",
    [
        pytest.param(["Unique Key"], id="NumSortKeys:1"),
        pytest.param(["Borough", "Unique Key"], id="NumSortKeys:2"),
    ],
)
def test_get_sorted(sort_desc, daft_df, service_requests_csv_pd_df, repartition_nparts, sort_keys):
    """Sort by a column"""
    daft_df = daft_df.repartition(repartition_nparts)
    daft_sorted_df = daft_df.sort([col(k) for k in sort_keys], desc=sort_desc)

    daft_sorted_pd_df = daft_sorted_df.to_pandas()
    assert_df_equals(
        daft_sorted_pd_df,
        service_requests_csv_pd_df.sort_values(by=sort_keys, ascending=not sort_desc),
        assert_ordering=True,
    )


@parametrize_service_requests_csv_repartition
@parametrize_sort_desc("sort_desc")
@pytest.mark.parametrize(
    "sort_keys",
    [
        pytest.param(["Unique Key"], id="NumSortKeys:1"),
        pytest.param(["Borough", "Unique Key"], id="NumSortKeys:2"),
    ],
)
def test_get_sorted_top_n(sort_desc, daft_df, service_requests_csv_pd_df, repartition_nparts, sort_keys):
    """Sort by a column"""
    daft_df = daft_df.repartition(repartition_nparts)
    daft_sorted_df = daft_df.sort([col(k) for k in sort_keys], desc=sort_desc).limit(100)
    daft_sorted_pd_df = daft_sorted_df.to_pandas()

    assert_df_equals(
        daft_sorted_pd_df,
        service_requests_csv_pd_df.sort_values(by=sort_keys, ascending=not sort_desc).head(100),
        assert_ordering=True,
    )


@parametrize_service_requests_csv_repartition
@parametrize_sort_desc("sort_desc")
@pytest.mark.parametrize(
    "sort_keys",
    [
        pytest.param(["Borough", "Unique Key"], id="NumSortKeys:2"),
    ],
)
def test_get_sorted_top_n_flipped_desc(sort_desc, daft_df, service_requests_csv_pd_df, repartition_nparts, sort_keys):
    """Sort by a column"""
    daft_df = daft_df.repartition(repartition_nparts)
    desc_list = [sort_desc]
    for i in range(len(sort_keys) - 1):
        desc_list.append(not desc_list[-1])
    daft_sorted_df = daft_df.sort([col(k) for k in sort_keys], desc=desc_list).limit(100)
    daft_sorted_pd_df = daft_sorted_df.to_pandas()

    assert_df_equals(
        daft_sorted_pd_df,
        service_requests_csv_pd_df.sort_values(by=sort_keys, ascending=[not b for b in desc_list]).head(100),
        assert_ordering=True,
    )


@parametrize_service_requests_csv_repartition
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        pytest.param(
            lambda daft_df, sort_desc: daft_df.sort(col("Unique Key"), desc=sort_desc).select(
                col("Unique Key"), col("Complaint Type")
            ),
            id="sort..select",
        ),
        pytest.param(
            lambda daft_df, sort_desc: daft_df.select(col("Unique Key"), col("Complaint Type")).sort(
                col("Unique Key"), desc=sort_desc
            ),
            id="select..sort",
        ),
    ],
)
@parametrize_sort_desc("sort_desc")
def test_get_sorted_top_n_projected(daft_df_ops, sort_desc, daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Sort by a column and retrieve specific columns from the top N results"""
    daft_df = daft_df.repartition(repartition_nparts)
    expected = service_requests_csv_pd_df.sort_values(by="Unique Key", ascending=not sort_desc)[
        ["Unique Key", "Complaint Type"]
    ]
    daft_df = daft_df_ops(daft_df, sort_desc)
    daft_pd_pf = daft_df.to_pandas()
    assert_df_equals(
        daft_pd_pf,
        expected,
        assert_ordering=True,
    )
