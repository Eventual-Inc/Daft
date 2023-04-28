from __future__ import annotations

import math
import os
from typing import Any

import pandas as pd
from hypothesis import note, reject, settings
from hypothesis.stateful import Bundle, RuleBasedStateMachine, precondition, rule
from hypothesis.strategies import (
    booleans,
    data,
    integers,
    lists,
    permutations,
    sampled_from,
)

import daft
from daft import DataFrame
from daft.datatype import DataType
from tests.property_based_testing.strategies import (
    columns_dict,
    generate_data,
    total_order_dtypes,
)

# Maximum number of sort columns to generate
MAX_NUM_SORT_COLS = 3


def _is_nan(obj: Any) -> bool:
    """Checks if an object is a float NaN"""
    return isinstance(obj, float) and math.isnan(obj)


@settings(max_examples=int(os.getenv("HYPOTHESIS_MAX_EXAMPLES", 100)), stateful_step_count=8, deadline=None)
class DataframeSortStateMachine(RuleBasedStateMachine):
    """Tests sorts in the face of various other operations such as filters, projections etc

    Creates N number of sort key columns named "sort_key_{i}", and one "row_num" column which enumerates the original row number.

    Intermediate steps can consist of repartitions, filters, projections, sorts etc.

    The sort steps will additionally pull data down to check sort correctness.
    """

    Dataframes = Bundle("dataframes")

    def __init__(self):
        super().__init__()
        self.df: DataFrame | None = None
        self.sort_keys: list[str] = None
        self.num_rows_strategy = integers(min_value=8, max_value=8)
        self.repartition_num_partitions_strategy = sampled_from([1, 4, 5, 9])
        self.sorted_on: list[tuple[str, bool]] | None = None

    @rule(data=data(), num_sort_cols=integers(min_value=1, max_value=MAX_NUM_SORT_COLS))
    @precondition(lambda self: self.df is None)
    def newdataframe(self, data, num_sort_cols):
        """Start node of the state machine, creates an initial dataframe"""
        self.sort_keys = [f"sort_key_{i}" for i in range(num_sort_cols)]

        columns_dict_data = data.draw(
            columns_dict(
                generate_columns_with_type={
                    sort_key_col_name: total_order_dtypes for sort_key_col_name in self.sort_keys
                },
                num_rows_strategy=self.num_rows_strategy,
            )
        )
        df = daft.from_pydict(columns_dict_data)
        self.df = df

    @rule(data=data())
    @precondition(lambda self: self.df is not None)
    def run_sort(self, data):
        """Run a sort on the accumulated dataframe plan"""
        sort_on = data.draw(permutations(self.sort_keys))
        descending = data.draw(lists(min_size=len(self.sort_keys), max_size=len(self.sort_keys), elements=booleans()))
        self.df = self.df.sort(sort_on, desc=descending)
        self.sorted_on = list(zip(sort_on, descending))

    @rule()
    @precondition(lambda self: self.sorted_on is not None)
    def collect_after_sort(self):
        """Optionally runs after any sort step to check that sort is maintained"""
        sorted_data = self.df.to_pydict()
        sorted_on_cols = [c for c, _ in self.sorted_on]
        sorted_on_desc = [d for _, d in self.sorted_on]

        # Ensure that key column(s) are sorted correctly
        data = zip(*[sorted_data[k] for k in sorted_on_cols])

        try:
            current_tup = next(data)
        except StopIteration:
            # Trivial case, no rows to check that were sorted.
            self.sorted_on = None
            return

        try:
            for next_tup in data:
                note(f"Comparing {current_tup} and {next_tup} for desc={sorted_on_desc}")

                for current_val, next_val, desc in zip(current_tup, next_tup, sorted_on_desc):
                    a, b = (current_val, next_val) if desc else (next_val, current_val)

                    # Assert that a >= b, where the ordering is defined as: None > NaN > other values
                    # `continue` checking lex sort if values are equal, but `break` if they are not equal
                    if a is None and b is None:
                        continue
                    elif _is_nan(a) and _is_nan(b):
                        continue
                    elif a is None and _is_nan(b):
                        break
                    elif _is_nan(a) and b is None:
                        raise AssertionError(
                            f"current_val={current_val} vs next_val={next_val} is an invalid sort order for desc={desc}"
                        )
                    elif a is None or _is_nan(a):
                        break
                    elif b is None or _is_nan(b):
                        raise AssertionError(
                            f"current_val={current_val} vs next_val={next_val} is an invalid sort order for desc={desc}"
                        )

                    # Invariant here: all cases handled for None and NaN values
                    assert a is not None and not _is_nan(a)
                    assert b is not None and not _is_nan(b)

                    assert (
                        a >= b
                    ), f"current_val={current_val} vs next_val={next_val} is an invalid sort order for desc={desc}"
                    if a != b:
                        break

                current_tup = next_tup
        except AssertionError:
            sorted_keys_df = pd.DataFrame({k: pd.Series(v, dtype="object") for k, v in sorted_data.items()})
            note(f"Received sorted keys:\n{sorted_keys_df[sorted_on_cols]}")
            raise

        # Ensure that we reset self.sorted_on so that we won't try to collect again
        self.sorted_on = None

    ###
    # Intermediate fuzzing steps - these steps perform actions that should not affect the final sort result
    # Some steps are skipped because they encounter bugs that need to be fixed.
    ###

    @rule(data=data())
    @precondition(lambda self: self.df is not None)
    def repartition_df(self, data):
        """Runs a repartitioning step"""
        num_partitions = data.draw(
            self.repartition_num_partitions_strategy, label="Number of partitions for repartitioning"
        )
        self.df = self.df.repartition(num_partitions)

        # Repartitioning changes the ordering of the data, so we cannot sort after this step
        self.sorted_on = None

    @rule(data=data())
    @precondition(lambda self: self.df is not None)
    def filter_df(self, data):
        """Runs a filter on a simple equality predicate on a random column"""
        assert self.df is not None
        col_name_to_filter = data.draw(sampled_from(self.df.schema().column_names()), label="Column to filter on")
        col_daft_type = self.df.schema()[col_name_to_filter].dtype

        # Logical types do not accept equality operators, but can be filtered on by themselves
        if col_daft_type == DataType.bool():
            self.df = self.df.where(self.df[col_name_to_filter])
        # Reject if filtering on a null column - not a meaningful operation
        elif col_daft_type == DataType.null():
            reject()
        # Reject for binary types because they are not comparable yet (TODO: https://github.com/Eventual-Inc/Daft/issues/688)
        elif col_daft_type == DataType.binary():
            reject()
        else:
            filter_value = data.draw(generate_data(col_daft_type), label="Filter value")
            self.df = self.df.where(self.df[col_name_to_filter] == filter_value)

    @rule(data=data())
    @precondition(lambda self: self.df is not None)
    def project_df(self, data):
        """Runs a projection on a random column, replacing it"""
        assert self.df is not None
        column_name = data.draw(sampled_from(self.df.schema().column_names()), label="Column to filter on")
        column_daft_type = self.df.schema()[column_name].dtype
        type_to_op_mapping = {
            DataType.string(): lambda e, other: e + other,
            DataType.int64(): lambda e, other: e + other,
            DataType.int32(): lambda e, other: e + other,
            DataType.float64(): lambda e, other: e + other,
            DataType.bool(): lambda e, other: e | other,
            # No meaningful binary operations supported for these yet
            DataType.date(): lambda e, other: e.dt.year(),
            DataType.binary(): lambda e, other: e,
            DataType.null(): lambda e, other: e,
        }
        op = type_to_op_mapping[column_daft_type]
        other_binary_value = data.draw(generate_data(column_daft_type), label="Binary *other* value")
        self.df = self.df.with_column(column_name, op(self.df[column_name], other_binary_value))

        # Some of the projections change the ordering of the data, so data is no longer sorted
        self.sorted_on = None


TestDataframeSortStateMachine = DataframeSortStateMachine.TestCase
