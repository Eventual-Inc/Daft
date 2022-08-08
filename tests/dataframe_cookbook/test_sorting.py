import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_partitioned_daft_df,
    parametrize_sort_desc,
)


@parametrize_partitioned_daft_df()
@parametrize_sort_desc("sort_desc")
def test_get_sorted(sort_desc, daft_df, pd_df):
    """Sort by a column"""
    daft_sorted_df = daft_df.sort(col("Unique Key"), desc=sort_desc)

    assert_df_equals(
        daft_sorted_df,
        pd_df.sort_values(by="Unique Key", ascending=not sort_desc),
        assert_ordering=True,
    )


@parametrize_partitioned_daft_df()
@parametrize_sort_desc("sort_desc")
def test_get_sorted_top_n(sort_desc, daft_df, pd_df):
    """Sort by a column"""
    daft_sorted_df = daft_df.sort(col("Unique Key"), desc=sort_desc).limit(100)

    assert_df_equals(
        daft_sorted_df,
        pd_df.sort_values(by="Unique Key", ascending=not sort_desc).head(100),
        assert_ordering=True,
    )


@parametrize_partitioned_daft_df()
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
def test_get_sorted_top_n_projected(daft_df_ops, sort_desc, daft_df, pd_df):
    """Sort by a column and retrieve specific columns from the top N results"""
    expected = pd_df.sort_values(by="Unique Key", ascending=not sort_desc)[["Unique Key", "Complaint Type"]]
    assert_df_equals(
        daft_df_ops(daft_df, sort_desc),
        expected,
        assert_ordering=True,
    )
