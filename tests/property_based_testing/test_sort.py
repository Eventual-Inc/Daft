from __future__ import annotations

from hypothesis import given

from tests.property_based_testing.strategies import dataframe, hashable_dtypes


@given(dataframe(columns={"A": hashable_dtypes}))
def test_sort_simple(df):
    sorted_df = df.sort("A")
    data = sorted_df.to_pydict()

    # Ensure that data is sorted correctly
    data_list = data["A"].to_pylist()
    nones, non_nones = [el for el in data_list if el is None], [el for el in data_list if el is not None]
    sorted_data = sorted(non_nones) + nones
    assert data_list == sorted_data
