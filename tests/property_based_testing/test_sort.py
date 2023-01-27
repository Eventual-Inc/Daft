from __future__ import annotations

from hypothesis.stateful import Bundle, RuleBasedStateMachine, precondition, rule
from hypothesis.strategies import data, integers

from daft import DataFrame
from tests.property_based_testing.strategies import (
    columns_dict,
    row_nums_column,
    total_order_dtypes,
)


class DataframeSortStateMachine(RuleBasedStateMachine):
    """Tests sorts in the face of various other operations such as filters, projections etc"""

    Dataframes = Bundle("dataframes")

    def __init__(self):
        super().__init__()
        self.df = None
        self.sort_keys = None
        self.row_num_col_name = "row_num"

    @rule(data=data(), num_sort_cols=integers(min_value=1, max_value=3))
    @precondition(lambda self: self.df is None)
    def newdataframe(self, data, num_sort_cols):
        """Start node of the state machine, creates an initial dataframe"""
        self.sort_keys = [f"sort_key_{i}" for i in range(num_sort_cols)]
        # Generate N number of sort key columns, and one "row_num" column which enumerates the original row number
        columns_dict_data = data.draw(
            columns_dict(
                generate_columns_with_type={
                    sort_key_col_name: total_order_dtypes for sort_key_col_name in self.sort_keys
                },
                generate_columns_with_strategy={self.row_num_col_name: row_nums_column},
            )
        )
        df = DataFrame.from_pydict(columns_dict_data)
        self.df = df

    @rule()
    @precondition(lambda self: self.df is not None)
    def run_and_check_sort(self):
        """'assert' step of this state machine which runs a sort on the accumulated dataframe plan and checks that the sort was executed correctly."""
        unsorted_data = self.df.to_pydict()
        self.df._clear_cache()
        self.df = self.df.sort(self.sort_keys)
        sorted_data = self.df.to_pydict()
        self.df._clear_cache()

        sorted_keys = list(zip(*[sorted_data[k].to_pylist() for k in self.sort_keys]))
        original_keys = list(zip(*[unsorted_data[k].to_pylist() for k in self.sort_keys]))

        # Ensure that key column(s) are sorted correctly
        manually_sorted_keys = sorted(
            original_keys,
            # Use (item is None, item) to handle sorting of None values
            key=lambda tup: tuple((item is None, item) for item in tup),
        )
        assert sorted_keys == manually_sorted_keys

        # Ensure that rows were not mangled during sort
        row_num_to_sorted_idx_mapping = {
            row_num: idx for idx, row_num in enumerate(sorted_data[self.row_num_col_name].to_pylist())
        }
        original_row_num = unsorted_data[self.row_num_col_name].to_pylist()
        unsorted_keys = []
        for idx in range(len(sorted_keys)):
            row_num = original_row_num[idx]
            sorted_idx = row_num_to_sorted_idx_mapping[row_num]
            key = sorted_keys[sorted_idx]
            unsorted_keys.append(key)

        assert unsorted_keys == original_keys


TestDataframeSortStateMachine = DataframeSortStateMachine.TestCase
