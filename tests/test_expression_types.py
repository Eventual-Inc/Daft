from __future__ import annotations

import datetime

import numpy as np
import pyarrow as pa
import pytest

from daft import DataFrame, col
from daft.execution.operators import OperatorEnum
from daft.types import ExpressionType


class MyObj:
    pass


ALL_TYPES = {
    ExpressionType.from_py_type(str): ["a", "b", "c", None],
    ExpressionType.from_py_type(bytes): [b"a", b"b", b"c", None],
    ExpressionType.from_py_type(bool): [True, True, False, None],
    ExpressionType.from_py_type(datetime.date): [
        datetime.date(1994, 1, 1),
        datetime.date(1994, 1, 2),
        datetime.date(1994, 1, 3),
        None,
    ],
    ExpressionType.from_py_type(int): [1, 2, 3, None],
    ExpressionType.from_py_type(float): [1.0, 2.0, 3.0, None],
    ExpressionType.from_py_type(object): [MyObj(), MyObj(), MyObj(), None],
    ExpressionType.from_py_type(type(None)): [None, None, None, None],
}

# Mapping between the OperatorEnum name and a lambda that runs it on input expressions
OPS = {
    "NEGATE": lambda ex: -ex,
    "POSITIVE": lambda ex: +ex,
    "ABS": lambda ex: abs(ex),
    "INVERT": lambda ex: ~ex,
    "STR_CONCAT": lambda ex1, ex2: ex1.str.concat(ex2),
    "STR_CONTAINS": lambda ex, _: ex.str.contains("foo"),
    "STR_ENDSWITH": lambda ex, _: ex.str.endswith("foo"),
    "STR_STARTSWITH": lambda ex, _: ex.str.startswith("foo"),
    "STR_LENGTH": lambda ex: ex.str.length(),
    "IS_NULL": lambda ex: ex.is_null(),
    "IS_NAN": lambda ex: ex.is_nan(),
    "DT_DAY": lambda ex: ex.dt.day(),
    "DT_MONTH": lambda ex: ex.dt.month(),
    "DT_YEAR": lambda ex: ex.dt.year(),
    "DT_DAY_OF_WEEK": lambda ex: ex.dt.day_of_week(),
    "ADD": lambda ex1, ex2: ex1 + ex2,
    "SUB": lambda ex1, ex2: ex1 - ex2,
    "MUL": lambda ex1, ex2: ex1 * ex2,
    "FLOORDIV": lambda ex1, ex2: ex1 // ex2,
    "TRUEDIV": lambda ex1, ex2: ex1 / ex2,
    "POW": lambda ex1, ex2: ex1**ex2,
    "MOD": lambda ex1, ex2: ex1 % ex2,
    "AND": lambda ex1, ex2: ex1 & ex2,
    "OR": lambda ex1, ex2: ex1 | ex2,
    "LT": lambda ex1, ex2: ex1 < ex2,
    "LE": lambda ex1, ex2: ex1 <= ex2,
    "EQ": lambda ex1, ex2: ex1 == ex2,
    "NEQ": lambda ex1, ex2: ex1 != ex2,
    "GT": lambda ex1, ex2: ex1 > ex2,
    "GE": lambda ex1, ex2: ex1 >= ex2,
    "IF_ELSE": lambda ex1, ex2, ex3: ex1.if_else(ex2, ex3),
}

# These ops are not evaluated via expression evaluation and are exluded from this test
EXCLUDE_OPS = {
    "SUM",
    "MEAN",
    "MIN",
    "MAX",
    "COUNT",
    "LIST",
    "CONCAT",
    "EXPLODE",
    # Tested in test_cast.py
    "CAST_INT",
    "CAST_FLOAT",
    "CAST_STRING",
    "CAST_LOGICAL",
    "CAST_BYTES",
    "CAST_DATE",
}

# Mapping between ExpressionTypes and (expected_numpy_dtypes, expected_python_types)
CHECK_TYPE_FUNC: dict[ExpressionType, tuple[set[np.dtype], type]] = {
    ExpressionType.from_py_type(str): lambda arr: pa.types.is_string(arr.type),
    ExpressionType.from_py_type(bytes): lambda arr: pa.types.is_binary(arr.type),
    ExpressionType.from_py_type(bool): lambda arr: pa.types.is_boolean(arr.type),
    ExpressionType.from_py_type(datetime.date): lambda arr: pa.types.is_date(arr.type),
    ExpressionType.from_py_type(int): lambda arr: pa.types.is_integer(arr.type),
    ExpressionType.from_py_type(float): lambda arr: pa.types.is_floating(arr.type),
    ExpressionType.from_py_type(object): lambda arr: isinstance(arr, list),
    # None of the operations return Null types
    # ExpressionType.from_py_type(type(None)): ({np.dtype("object")}, type(None)),
}


@pytest.mark.parametrize(
    ["op_name", "arg_types", "ret_type"],
    # Test that all the defined types in type matrix work as intended
    [
        pytest.param(op_name, arg_types, ret_type, id=f"{op_name}-{'-'.join(map(str, arg_types))}-{ret_type}")
        for op_name, op_enum in OperatorEnum.__members__.items()
        for arg_types, ret_type in op_enum.value.type_matrix
        if op_name not in EXCLUDE_OPS
    ],
)
def test_type_matrix_execution(op_name: str, arg_types: tuple[ExpressionType, ...], ret_type: ExpressionType):
    df = DataFrame.from_pydict({str(et): ALL_TYPES[et] for et in ALL_TYPES})
    op = OPS[op_name]
    df = df.with_column("bar", op(*[col(str(et)) for et in arg_types]))

    assert df.schema()["bar"].daft_type == ret_type

    df.collect()
    collected_columns = df.to_pydict()

    check_type_func = CHECK_TYPE_FUNC[ret_type]
    assert check_type_func(
        collected_columns["bar"]
    ), f"Expected type {ret_type} but received array {collected_columns['bar']} with type {collected_columns['bar'].type}"
