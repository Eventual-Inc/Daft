from __future__ import annotations

import datetime

import pyarrow as pa
from hypothesis.strategies import (
    SearchStrategy,
    binary,
    booleans,
    composite,
    dates,
    floats,
    integers,
    lists,
    none,
    one_of,
    sampled_from,
    text,
)

from daft.types import ExpressionType


class UserObject:
    def __init__(self, x: str, y: int):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"UserObject(x={self.x}, y={self.y})"

    def add(self, other: UserObject) -> UserObject:
        return UserObject(x=self.x + other.x, y=self.y + other.y)


@composite
def user_object(draw) -> UserObject:
    return UserObject(x=draw(text(), label="UserObject.x"), y=draw(integers(), "UserObject.y"))


###
# Various strategies for dtypes and their corresponding Daft type
###

_strat_allstr = text()
_strat_int64 = integers(min_value=-(2**63), max_value=(2**63) - 1)
_strat_double = floats()
_strat_boolean = booleans()
_strat_byte = binary()
_strat_date = dates(min_value=datetime.date(2000, 1, 1), max_value=datetime.date(2100, 1, 1))
_strat_user_object = user_object()

_default_strategies = {
    ExpressionType.string(): _strat_allstr,
    ExpressionType.integer(): _strat_int64,
    ExpressionType.float(): _strat_double,
    ExpressionType.logical(): _strat_boolean,
    ExpressionType.bytes(): _strat_byte,
    ExpressionType.date(): _strat_date,
    ExpressionType._infer_from_py_type(UserObject): _strat_user_object,
    ExpressionType.null(): none(),
}


@composite
def generate_data(
    draw, daft_type: ExpressionType, strategies: dict[ExpressionType, SearchStrategy] = _default_strategies
):
    """Helper to generate data when given a daft_type"""
    if daft_type not in strategies:
        raise NotImplementedError(f"Strategy for type {daft_type} not implemented")
    return draw(strategies[daft_type], label=f"Generated data for type {daft_type}")


# All available dtypes
all_dtypes = sampled_from(
    [
        ExpressionType.string(),
        ExpressionType.integer(),
        ExpressionType.float(),
        ExpressionType.logical(),
        ExpressionType.bytes(),
        ExpressionType.date(),
        ExpressionType._infer_from_py_type(UserObject),
    ]
)

# Dtypes that have a total ordering
total_order_dtypes = sampled_from(
    [
        ExpressionType.string(),
        ExpressionType.integer(),
        ExpressionType.float(),
        # ExpressionType.logical(),
        # ExpressionType.bytes(),
        ExpressionType.date(),
    ]
)

ColumnData = "list[Any] | pa.Array"


###
# Strategies for creation of column data
###


@composite
def column(
    draw,
    length: int = 64,
    dtypes: SearchStrategy[ExpressionType] = all_dtypes,
    strategies: dict[ExpressionType, SearchStrategy] = _default_strategies,
) -> ColumnData:
    """Generate a column of data

    Args:
        draw: Hypothesis draw function
        length: length of column
        dtypes: strategy for generating daft_type
        strategies: strategies for generating data for each daft_type, defaults to `_default_strategies`
    """
    daft_type = draw(dtypes, label="Column dtype")
    dtype_strategy = strategies[daft_type]
    nullable_dtype_strategy = one_of(dtype_strategy, none())
    col_data = draw(
        one_of(
            # All nulls
            lists(none(), min_size=length, max_size=length),
            # No nulls
            lists(dtype_strategy, min_size=length, max_size=length),
            # Some nulls
            lists(nullable_dtype_strategy, min_size=length, max_size=length),
        ),
        label=f"Column data",
    )

    # Convert sampled data to appropriate underlying format
    if daft_type._is_python_type():
        return col_data
    return pa.array(col_data, type=daft_type.to_arrow_type())


@composite
def row_nums_column(draw, length: int = 64) -> ColumnData:
    return pa.array([i for i in range(length)], type=pa.int64())


@composite
def columns_dict(
    draw,
    generate_columns_with_type: dict[str, SearchStrategy[ExpressionType]] = {},
    generate_columns_with_strategy: dict[str, SearchStrategy[ColumnData]] = {},
    num_rows_strategy: SearchStrategy[int] = integers(min_value=0, max_value=8),
    num_other_generated_columns_strategy: SearchStrategy[int] = integers(min_value=0, max_value=2),
) -> dict[str, ColumnData]:
    """Hypothesis composite strategy for generating in-memory Daft DataFrames.

    Args:
        draw: Hypothesis draw function
        num_rows_strategy: strategy for generating number of rows
        generate_columns_with_type: {col_name: column_strategy} for specific strategies (e.g. hashable columns, numeric columns etc)
        num_other_generated_columns_strategy: generate N more columns with random names and data according to this strategy
    """
    df_len = draw(num_rows_strategy, label="Number of rows")

    # Generate requested columns according to requested types
    requested_columns = {
        col_name: draw(column(length=df_len, dtypes=daft_type_strategy), label=f"Requested column {col_name} by type")
        for col_name, daft_type_strategy in generate_columns_with_type.items()
    }

    # Generate requested columns according to requested strategies
    requested_strategy_columns = {
        col_name: draw(col_strategy(length=df_len), label=f"Requested column {col_name} by strategy")
        for col_name, col_strategy in generate_columns_with_strategy.items()
    }

    # Generate additional columns with random types and names
    num_cols = draw(num_other_generated_columns_strategy, label="Number of additional columns")
    additional_column_names = draw(
        lists(
            text().filter(
                lambda name: name not in requested_columns.keys() and name not in requested_strategy_columns.keys()
            ),
            min_size=num_cols,
            max_size=num_cols,
            unique=True,
        ),
        label="Additional column names",
    )
    additional_columns = {
        col_name: draw(column(length=df_len), label=f"Additional column {col_name}")
        for col_name in additional_column_names
    }

    return {**requested_columns, **requested_strategy_columns, **additional_columns}
