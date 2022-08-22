import pytest

from daft.dataframe import DataFrame
from daft.execution.operators import ExpressionType
from daft.expressions import col, udf

COLS = ["floatcol", "intcol", "stringcol", "boolcol"]


@pytest.fixture(scope="function")
def daft_df():
    daft_df = DataFrame.from_csv("tests/assets/iris.csv")
    daft_df = daft_df.select(
        col("sepal.length").alias("floatcol"),
        (col("sepal.length") // 1).alias("intcol"),
        col("variety").alias("stringcol"),
        (col("sepal.length") < 5.0).alias("boolcol"),
    )

    schema_fields = {e.name: e for e in daft_df.schema()}
    assert schema_fields.keys() == {"floatcol", "intcol", "stringcol", "boolcol"}
    assert schema_fields["floatcol"].daft_type == ExpressionType.from_py_type(float)
    assert schema_fields["intcol"].daft_type == ExpressionType.from_py_type(int)
    assert schema_fields["stringcol"].daft_type == ExpressionType.from_py_type(str)
    assert schema_fields["boolcol"].daft_type == ExpressionType.from_py_type(bool)
    return daft_df


UNARY_OPS_RESULT_TYPE_MAPPING = {
    "__abs__": {
        "floatcol": ExpressionType.from_py_type(float),
        "intcol": ExpressionType.from_py_type(int),
    },
    "__neg__": {
        "floatcol": ExpressionType.from_py_type(float),
        "intcol": ExpressionType.from_py_type(int),
    },
    "__pos__": {
        "floatcol": ExpressionType.from_py_type(float),
        "intcol": ExpressionType.from_py_type(int),
    },
    "__invert__": {
        "boolcol": ExpressionType.from_py_type(bool),
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
            UNARY_OPS_RESULT_TYPE_MAPPING[op].get(colname, ExpressionType.unknown()),
            id=f"Op:{op}-Col:{colname}",
        )
        for op in UNARY_OPS_RESULT_TYPE_MAPPING
        for colname in COLS
    ],
)
def test_unary_ops_select_types(daft_df, colname, op, expected_result_type):
    df = daft_df.select(getattr(col(colname), op)())

    fields = [field for field in df.schema()]
    assert len(fields) == 1
    field = fields[0]
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

BINARY_OPS_RESULT_TYPE_MAPPING = {
    "__add__": number_number_number,
    "__sub__": number_number_number,
    "__mul__": number_number_number,
    "__floordiv__": {k: ExpressionType.from_py_type(int) for k in number_number_number},
    "__truediv__": number_number_number,
    "__pow__": number_number_number,
    "__mod__": number_number_number,
    "__and__": bool_bool_logical,
    "__or__": bool_bool_logical,
    "__lt__": {**number_number_logical, **string_string_logical},
    "__le__": {**number_number_logical, **string_string_logical},
    "__eq__": {**number_number_logical, **string_string_logical},
    "__ne__": {**number_number_logical, **string_string_logical},
    "__gt__": {**number_number_logical, **string_string_logical},
    "__ge__": {**number_number_logical, **string_string_logical},
}


@pytest.mark.parametrize(
    ["col1", "col2", "op", "expected_result_type"],
    [
        pytest.param(
            col1,
            col2,
            op,
            BINARY_OPS_RESULT_TYPE_MAPPING[op].get((col1, col2), ExpressionType.unknown()),
            id=f"Op:{op}:Cols:{(col1, col2)}",
        )
        for op in BINARY_OPS_RESULT_TYPE_MAPPING
        for col1 in COLS
        for col2 in COLS
    ],
)
def test_unary_ops_select_types(daft_df, col1, col2, op, expected_result_type):
    df = daft_df.select(getattr(col(col1), op)(col(col2)))

    fields = [field for field in df.schema()]
    assert len(fields) == 1
    field = fields[0]
    assert field.name == col1
    assert field.daft_type == expected_result_type


def test_udf(daft_df):
    @udf(return_type=str)
    def my_udf(x, y):
        pass

    df = daft_df.select(my_udf(col("floatcol"), col("boolcol")))

    fields = [field for field in df.schema()]
    assert len(fields) == 1
    field = fields[0]
    assert field.name == "floatcol"
    assert field.daft_type == ExpressionType.from_py_type(str)


@pytest.mark.skip(reason="multi-return UDF not implemented")
def test_multi_return_udf(daft_df):
    @udf(return_type=[str, str])
    def my_udf(x, y):
        pass

    c0, c1 = my_udf(col("floatcol"), col("boolcol"))
    df = daft_df.select(c0.alias("c0"), c1.alias("c1"))
    fields = [field for field in df.schema()]
    assert len(fields) == 2

    for i, field in enumerate(fields):
        assert field.name == f"c{i}"
        assert field.daft_type == ExpressionType.from_py_type(str)
