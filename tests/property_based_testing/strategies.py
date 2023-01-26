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

from daft import DataFrame
from daft.types import ExpressionType


class UserObject:
    def __init__(self, x: str, y: int):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"UserObject(x={self.x}, y={self.y})"


@composite
def user_object(draw) -> UserObject:
    return UserObject(x=draw(text(), label="UserObject.x"), y=draw(integers(), "UserObject.y"))


###
# Various strategies for dtypes and their corresponding Daft type
###

allstr_dtype = (text(), str)
int64_dtype = (integers(min_value=-(2**63), max_value=(2**63) - 1), int)
double_dtype = (floats(), float)
boolean_dtype = (booleans(), bool)
byte_dtype = (binary(), bytes)
date_dtype = (dates(), datetime.date)
user_object_dtype = (user_object(), UserObject)

# All available dtypes
all_dtypes = sampled_from(
    [
        allstr_dtype,
        int64_dtype,
        double_dtype,
        boolean_dtype,
        byte_dtype,
        date_dtype,
        user_object_dtype,
    ]
)

# Dtypes that are numeric
numeric_dtypes = sampled_from(
    [
        int64_dtype,
        double_dtype,
    ]
)

# Dtypes that are hashable and can be used for sorts, joins, repartitions etc
hashable_dtypes = sampled_from(
    [
        allstr_dtype,
        int64_dtype,
        boolean_dtype,
        byte_dtype,
        date_dtype,
    ]
)

# Dtypes that are not hashable, and should throw an error if used for sorts, joins, repartitions etc
non_hashable_dtypes = sampled_from(
    [
        double_dtype,
        user_object_dtype,
    ]
)

ColumnData = "list[Any] | pa.Array"


@composite
def column(draw, length: int = 64, dtypes: SearchStrategy = all_dtypes) -> ColumnData:
    dtype_strategy, dtype = draw(dtypes, label="Column dtype strategy")
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
    daft_type = ExpressionType.from_py_type(dtype)
    if ExpressionType.is_py(daft_type):
        return col_data
    return pa.array(col_data, type=daft_type.to_arrow_type())


@composite
def dataframe(
    draw,
    columns: dict[str, SearchStrategy[tuple[SearchStrategy, type]]] = {},
    num_rows_strategy: SearchStrategy[int] = integers(min_value=0, max_value=8),
    num_other_generated_columns_strategy: SearchStrategy[int] = integers(min_value=0, max_value=2),
) -> DataFrame:
    """Hypothesis composite strategy for generating in-memory Daft DataFrames.

    Args:
        draw: Hypothesis draw function
        num_rows_strategy: strategy for generating number of rows
        columns: {col_name: column_strategy} for specific strategies (e.g. hashable columns, numeric columns etc)
        num_other_generated_columns_strategy: generate N more columns with random names and data according to this strategy
    """
    df_len = draw(num_rows_strategy, label="Number of rows")

    # Generate requested columns according to requested types
    requested_columns = {
        col_name: draw(column(length=df_len, dtypes=col_type_strategy), label=f"Requested column {col_name}")
        for col_name, col_type_strategy in columns.items()
    }

    # Generate additional columns with random types and names
    num_cols = draw(num_other_generated_columns_strategy, label="Number of additional columns")
    additional_column_names = draw(
        lists(text().filter(lambda name: name in requested_columns), min_size=num_cols, max_size=num_cols, unique=True),
        label="Additional column names",
    )
    additional_columns = {
        col_name: draw(column(length=df_len), label=f"Additional column {col_name}")
        for col_name in additional_column_names
    }

    return DataFrame.from_pydict({**requested_columns, **additional_columns})
