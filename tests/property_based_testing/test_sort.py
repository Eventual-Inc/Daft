from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import data, integers

from daft import DataFrame
from tests.property_based_testing.strategies import (
    columns_dict,
    row_nums_column,
    total_order_dtypes,
)


@given(data(), integers(min_value=1, max_value=3))
def test_sort_multi_column(data, num_sort_cols):
    # Generate N number of sort key columns, and one "row_num" column which enumerates the original row number
    sort_key_columns = [f"key_{i}" for i in range(num_sort_cols)]
    columns_dict_data = data.draw(
        columns_dict(
            generate_columns_with_type={
                sort_key_col_name: total_order_dtypes for sort_key_col_name in sort_key_columns
            },
            generate_columns_with_strategy={"row_num": row_nums_column},
        )
    )

    df = DataFrame.from_pydict(columns_dict_data)
    sorted_df = df.sort(sort_key_columns)
    sorted_data = sorted_df.to_pydict()
    sorted_keys: list[tuple[Any, ...]] = list(zip(*[sorted_data[k].to_pylist() for k in sort_key_columns]))

    # Ensure that key column(s) are sorted correctly
    original_keys = list(zip(*[columns_dict_data[k].to_pylist() for k in sort_key_columns]))
    manually_sorted_keys = sorted(
        original_keys,
        # Use (item is None, item) to handle sorting of None values
        key=lambda tup: tuple((item is None, item) for item in tup),
    )
    assert sorted_keys == manually_sorted_keys

    # Ensure that row numbers were not mangled during sort
    original_row_to_index_mapping = {row_num: i for i, row_num in enumerate(sorted_data["row_num"].to_pylist())}
    unsorted_keys = [sorted_keys[original_row_to_index_mapping[i]] for i in range(len(sorted_keys))]
    assert unsorted_keys == original_keys
