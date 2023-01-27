from __future__ import annotations

from hypothesis import given

from daft import DataFrame
from tests.property_based_testing.strategies import (
    columns_dict,
    row_nums_column,
    total_order_dtypes,
)


@given(
    columns_dict(
        generate_columns_with_type={"A": total_order_dtypes},
        generate_columns_with_strategy={"row_num": row_nums_column},
    )
)
def test_sort_single_column(columns_dict_data):
    df = DataFrame.from_pydict(columns_dict_data)
    sorted_df = df.sort("A")
    sorted_data = sorted_df.to_pydict()

    # Ensure that key column is sorted correctly
    sorted_keys = sorted_data["A"].to_pylist()
    nones, non_nones = [el for el in sorted_keys if el is None], [el for el in sorted_keys if el is not None]
    manually_sorted_data = sorted(non_nones) + nones
    assert sorted_keys == manually_sorted_data

    # Ensure that row numbers were not mangled during sort
    original_keys = columns_dict_data["A"].to_pylist()
    original_row_to_index_mapping = {row_num: i for i, row_num in enumerate(sorted_data["row_num"].to_pylist())}
    unsorted_keys = [sorted_keys[original_row_to_index_mapping[i]] for i in range(len(sorted_keys))]
    assert unsorted_keys == original_keys
