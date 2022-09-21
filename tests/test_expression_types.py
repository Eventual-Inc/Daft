import datetime
from typing import Dict, Set, Tuple, Type

import numpy as np
import polars as pl
import pytest

from daft import DataFrame, col
from daft.execution.operators import ExpressionType, OperatorEnum


class MyObj:
    pass


ALL_TYPES = {
    ExpressionType.from_py_type(str): ["a", "b", "c"],
    ExpressionType.from_py_type(bytes): [b"a", b"b", b"c"],
    ExpressionType.from_py_type(bool): [True, True, False],
    ExpressionType.from_py_type(datetime.date): [
        datetime.date(1994, 1, 1),
        datetime.date(1994, 1, 2),
        datetime.date(1994, 1, 3),
    ],
    ExpressionType.from_py_type(int): [1, 2, 3],
    ExpressionType.from_py_type(float): [1.0, 2.0, 3.0],
    ExpressionType.from_py_type(object): [MyObj(), MyObj(), MyObj()],
    ExpressionType.from_py_type(type(None)): [None, None, None],
}

# Mapping between the OperatorEnum name and a lambda that runs it on input expressions
OPS = {
    "NEGATE": lambda ex: -ex,
    "POSITIVE": lambda ex: +ex,
    "ABS": lambda ex: abs(ex),
    "INVERT": lambda ex: ~ex,
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

# These ops do not have Expression methods and are exluded from this test
EXCLUDE_OPS = {
    "SUM",
    "MEAN",
    "MIN",
    "MAX",
    "COUNT",
}

# Mapping between ExpressionTypes and (expected_numpy_dtypes, expected_python_types)
NP_AND_PY_TYPE: Dict[ExpressionType, Tuple[Set[np.dtype], Type]] = {
    ExpressionType.from_py_type(str): ({pl.Utf8}, str),
    ExpressionType.from_py_type(bytes): ({pl.List(pl.UInt8)}, bytes),
    ExpressionType.from_py_type(bool): ({pl.Boolean}, bool),
    ExpressionType.from_py_type(datetime.date): ({pl.Date}, datetime.date),
    ExpressionType.from_py_type(int): ({pl.Int32, pl.Int64}, int),
    ExpressionType.from_py_type(float): ({pl.Float64}, float),
    ExpressionType.from_py_type(object): ({pl.Object}, MyObj),
    # None of the operations return Null types
    # ExpressionType.from_py_type(type(None)): ({np.dtype("object")}, type(None)),
}


@pytest.mark.parametrize(
    ["op_name", "arg_types", "ret_type"],
    [
        pytest.param(op_name, arg_types, ret_type, id=f"{op_name}-{'-'.join(map(str, arg_types))}-{ret_type}")
        for op_name, op_enum in OperatorEnum.__members__.items()
        for arg_types, ret_type in op_enum.value.type_matrix
        if op_name not in EXCLUDE_OPS
    ],
)
def test_type_matrix_execution(op_name: str, arg_types: Tuple[ExpressionType, ...], ret_type: ExpressionType):
    df = DataFrame.from_pydict({str(et): ALL_TYPES[et] for et in ALL_TYPES})
    op = OPS[op_name]
    df = df.with_column("bar", op(*[col(str(et)) for et in arg_types]))

    expected_polars_type, expected_python_type = NP_AND_PY_TYPE[ret_type]

    assert df.schema()["bar"].daft_type == ret_type
    assert df.to_polars()["bar"].dtype in expected_polars_type
    for result_item in df.to_polars()["bar"].to_list():

        # HACK: corner-case here for bytes, as polars returns List[UInt8] instead
        if expected_python_type == bytes:
            assert isinstance(result_item, list)
            continue

        assert isinstance(result_item, expected_python_type), f"{result_item} != {expected_python_type}"
