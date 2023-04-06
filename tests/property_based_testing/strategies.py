from __future__ import annotations

import datetime

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

from daft.datatype import DataType
from daft.series import Series

###
# Various strategies for dtypes and their corresponding Daft type
###

_strat_allstr = text()
_strat_int64 = integers(min_value=-(2**63), max_value=(2**63) - 1)
_strat_int32 = integers(min_value=-(2**31), max_value=(2**31) - 1)
_strat_double = floats()
_strat_boolean = booleans()
_strat_byte = binary()
_strat_date = dates(min_value=datetime.date(2000, 1, 1), max_value=datetime.date(2100, 1, 1))

_default_strategies = {
    DataType.string(): _strat_allstr,
    DataType.int64(): _strat_int64,
    DataType.int32(): _strat_int32,
    DataType.float64(): _strat_double,
    DataType.bool(): _strat_boolean,
    DataType.binary(): _strat_byte,
    DataType.date(): _strat_date,
    DataType.null(): none(),
}


@composite
def generate_data(draw, daft_type: DataType, strategies: dict[DataType, SearchStrategy] = _default_strategies):
    """Helper to generate data when given a daft_type"""
    if daft_type not in strategies:
        raise NotImplementedError(f"Strategy for type {daft_type} not implemented")
    return draw(strategies[daft_type], label=f"Generated data for type {daft_type}")


# All available dtypes
all_dtypes = sampled_from(
    [
        DataType.string(),
        DataType.int64(),
        DataType.float64(),
        DataType.bool(),
        DataType.binary(),
        DataType.date(),
        DataType.null(),
    ]
)

# Dtypes that have a total ordering
total_order_dtypes = sampled_from(
    [
        DataType.string(),
        DataType.int64(),
        DataType.float64(),
        # DataType.bool(),
        # DataType.binary(),
        DataType.date(),
    ]
)


###
# Strategies for creation of column data
###


@composite
def series(
    draw,
    length: int = 64,
    dtypes: SearchStrategy[DataType] = all_dtypes,
    strategies: dict[DataType, SearchStrategy] = _default_strategies,
) -> Series:
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
    return Series.from_pylist(col_data).cast(daft_type)


@composite
def columns_dict(
    draw,
    generate_columns_with_type: dict[str, SearchStrategy[DataType]] = {},
    num_rows_strategy: SearchStrategy[int] = integers(min_value=0, max_value=8),
) -> dict[str, Series]:
    """Hypothesis composite strategy for generating in-memory Daft DataFrames.

    Args:
        draw: Hypothesis draw function
        num_rows_strategy: strategy for generating number of rows
        generate_columns_with_type: {col_name: column_strategy} for specific strategies (e.g. hashable columns, numeric columns etc)
    """
    df_len = draw(num_rows_strategy, label="Number of rows")

    # Generate requested columns according to requested types
    requested_columns = {
        col_name: draw(series(length=df_len, dtypes=daft_type_strategy), label=f"Requested column {col_name} by type")
        for col_name, daft_type_strategy in generate_columns_with_type.items()
    }

    # Generate additional columns with random types and names
    num_cols = draw(integers(min_value=0, max_value=2), label="Number of additional columns")
    additional_column_names = draw(
        lists(
            text().filter(lambda name: name not in requested_columns.keys()),
            min_size=num_cols,
            max_size=num_cols,
            unique=True,
        ),
        label="Additional column names",
    )
    additional_columns = {
        col_name: draw(series(length=df_len), label=f"Additional column {col_name}")
        for col_name in additional_column_names
    }

    return {**requested_columns, **additional_columns}
