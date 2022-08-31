import datetime

import pytest

from daft.dataframe import DataFrame
from daft.execution.operators import ExpressionType
from daft.expressions import col, lit
from daft.udf import udf

COLS = ["floatcol", "intcol", "stringcol", "boolcol", "datecol", "myobjcol"]


class MyObj:
    pass


@pytest.fixture(scope="function")
def daft_df():
    daft_df = DataFrame.from_csv("tests/assets/iris.csv")
    daft_df = daft_df.select(
        col("sepal.length").alias("floatcol"),
        lit(100).alias("intcol"),
        col("variety").alias("stringcol"),
        (col("sepal.length") < 5.0).alias("boolcol"),
        lit(datetime.date(1994, 1, 1)).alias("datecol"),
        lit(MyObj()).alias("myobjcol"),
    )

    schema_fields = daft_df.schema()
    assert set(schema_fields.column_names()) == set(COLS)
    assert schema_fields["floatcol"].daft_type == ExpressionType.from_py_type(float)
    assert schema_fields["intcol"].daft_type == ExpressionType.from_py_type(int)
    assert schema_fields["stringcol"].daft_type == ExpressionType.from_py_type(str)
    assert schema_fields["boolcol"].daft_type == ExpressionType.from_py_type(bool)
    assert schema_fields["datecol"].daft_type == ExpressionType.from_py_type(datetime.date)
    return daft_df


generic_python_object_unary = {"myobjcol": ExpressionType.python_object()}


UNARY_OPS_RESULT_TYPE_MAPPING = {
    "__abs__": {
        "floatcol": ExpressionType.from_py_type(float),
        "intcol": ExpressionType.from_py_type(int),
        **generic_python_object_unary,
    },
    "__neg__": {
        "floatcol": ExpressionType.from_py_type(float),
        "intcol": ExpressionType.from_py_type(int),
        **generic_python_object_unary,
    },
    "__pos__": {
        "floatcol": ExpressionType.from_py_type(float),
        "intcol": ExpressionType.from_py_type(int),
        **generic_python_object_unary,
    },
    "__invert__": {
        "boolcol": ExpressionType.from_py_type(bool),
        **generic_python_object_unary,
    },
    # TODO: add when ready
    # "count",
    # "sum",
    # "mean",
    # "min",
    # "max",
}


@pytest.mark.parametrize(
    ["colname", "op", "expected_result_type"],
    [
        pytest.param(
            colname,
            op,
            UNARY_OPS_RESULT_TYPE_MAPPING[op].get(colname, None),
            id=f"Op:{op}-Col:{colname}",
        )
        for op in UNARY_OPS_RESULT_TYPE_MAPPING
        for colname in COLS
    ],
)
def test_unary_ops_select_types(daft_df, colname, op, expected_result_type):
    if expected_result_type is None:
        with pytest.raises(TypeError):
            df = daft_df.select(getattr(col(colname), op)())
        return

    df = daft_df.select(getattr(col(colname), op)())
    fields = df.schema()
    assert len(fields) == 1
    field = fields[colname]
    assert field.name == colname
    assert field.daft_type == expected_result_type


number_number_number = {
    ("intcol", "intcol"): ExpressionType.from_py_type(int),
    ("intcol", "floatcol"): ExpressionType.from_py_type(float),
    ("floatcol", "floatcol"): ExpressionType.from_py_type(float),
    ("floatcol", "intcol"): ExpressionType.from_py_type(float),
}
bool_bool_logical = {("boolcol", "boolcol"): ExpressionType.from_py_type(bool)}
number_number_logical = {
    ("intcol", "intcol"): ExpressionType.from_py_type(bool),
    ("floatcol", "floatcol"): ExpressionType.from_py_type(bool),
    ("floatcol", "intcol"): ExpressionType.from_py_type(bool),
    ("intcol", "floatcol"): ExpressionType.from_py_type(bool),
}
string_string_logical = {("stringcol", "stringcol"): ExpressionType.from_py_type(bool)}
date_date_logical = {("datecol", "datecol"): ExpressionType.from_py_type(bool)}
generic_python_object_binary = {
    **{("myobjcol", c): ExpressionType.python_object() for c in COLS},
    **{(c, "myobjcol"): ExpressionType.python_object() for c in COLS},
}

BINARY_OPS_RESULT_TYPE_MAPPING = {
    "__add__": {**number_number_number, **generic_python_object_binary},
    "__sub__": {**number_number_number, **generic_python_object_binary},
    "__mul__": {**number_number_number, **generic_python_object_binary},
    "__floordiv__": {**number_number_number, **generic_python_object_binary},
    "__truediv__": {**number_number_number, **generic_python_object_binary},
    "__pow__": {**number_number_number, **generic_python_object_binary},
    "__mod__": {**number_number_number, **generic_python_object_binary},
    "__and__": {**bool_bool_logical, **generic_python_object_binary},
    "__or__": {**bool_bool_logical, **generic_python_object_binary},
    "__lt__": {**number_number_logical, **string_string_logical, **date_date_logical, **generic_python_object_binary},
    "__le__": {**number_number_logical, **string_string_logical, **date_date_logical, **generic_python_object_binary},
    "__eq__": {**number_number_logical, **string_string_logical, **date_date_logical, **generic_python_object_binary},
    "__ne__": {**number_number_logical, **string_string_logical, **date_date_logical, **generic_python_object_binary},
    "__gt__": {**number_number_logical, **string_string_logical, **date_date_logical, **generic_python_object_binary},
    "__ge__": {**number_number_logical, **string_string_logical, **date_date_logical, **generic_python_object_binary},
}


@pytest.mark.parametrize(
    ["col1", "col2", "op", "expected_result_type"],
    [
        pytest.param(
            col1,
            col2,
            op,
            BINARY_OPS_RESULT_TYPE_MAPPING[op].get((col1, col2), None),
            id=f"Op:{op}-Cols:{(col1, col2)}",
        )
        for op in BINARY_OPS_RESULT_TYPE_MAPPING
        for col1 in COLS
        for col2 in COLS
    ],
)
def test_binary_ops_select_types(daft_df, col1, col2, op, expected_result_type):
    if expected_result_type is None:
        with pytest.raises(TypeError):
            df = daft_df.select(getattr(col(col1), op)(col(col2)))
        return

    df = daft_df.select(getattr(col(col1), op)(col(col2)))
    fields = df.schema()
    assert len(fields) == 1
    field = fields[col1]
    assert field.name == col1
    assert field.daft_type == expected_result_type


def test_udf(daft_df):
    @udf(return_type=str)
    def my_udf(x, y):
        pass

    df = daft_df.select(my_udf(col("floatcol"), col("boolcol")))

    fields = df.schema()
    assert len(fields) == 1
    field = fields["floatcol"]
    assert field.name == "floatcol"
    assert field.daft_type == ExpressionType.from_py_type(str)
