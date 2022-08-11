import pytest

from daft.dataframe import DataFrame
from daft.execution.operators import ExpressionType
from daft.expressions import col

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

    schema_fields = {e.name(): e for e in daft_df.schema()}
    assert schema_fields.keys() == {"floatcol", "intcol", "stringcol", "boolcol"}
    assert schema_fields["floatcol"].resolved_type() == ExpressionType.NUMBER
    assert schema_fields["intcol"].resolved_type() == ExpressionType.NUMBER
    assert schema_fields["stringcol"].resolved_type() == ExpressionType.STRING
    assert schema_fields["boolcol"].resolved_type() == ExpressionType.LOGICAL
    return daft_df


UNARY_OPS_RESULT_TYPE_MAPPING = {
    "__abs__": {
        "floatcol": ExpressionType.NUMBER,
        "intcol": ExpressionType.NUMBER,
    },
    "__neg__": {
        "floatcol": ExpressionType.NUMBER,
        "intcol": ExpressionType.NUMBER,
    },
    "__pos__": {
        "floatcol": ExpressionType.NUMBER,
        "intcol": ExpressionType.NUMBER,
    },
    "__invert__": {
        "boolcol": ExpressionType.LOGICAL,
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
            UNARY_OPS_RESULT_TYPE_MAPPING[op].get(colname, ExpressionType.UNKNOWN),
            id=f"Op:{op},Col:{colname}",
        )
        for op in UNARY_OPS_RESULT_TYPE_MAPPING
        for colname in COLS
    ],
)
def test_unary_ops_select_types(daft_df, colname, op, expected_result_type):
    df = daft_df.select(getattr(col(colname), op)())

    exprs = [e for e in df.schema()]
    assert len(exprs) == 1
    expr = exprs[0]
    assert expr.name() == colname
    assert expr.resolved_type() == expected_result_type


number_cols = ["floatcol", "intcol"]
number_number_number = {(col1, col2): ExpressionType.NUMBER for col1 in number_cols for col2 in number_cols}
bool_bool_logical = {("boolcol", "boolcol"): ExpressionType.LOGICAL}
number_number_logical = {(col1, col2): ExpressionType.LOGICAL for col1 in number_cols for col2 in number_cols}
string_string_logical = {("stringcol", "stringcol"): ExpressionType.LOGICAL}

BINARY_OPS_RESULT_TYPE_MAPPING = {
    "__add__": number_number_number,
    "__sub__": number_number_number,
    "__mul__": number_number_number,
    "__floordiv__": number_number_number,
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
            BINARY_OPS_RESULT_TYPE_MAPPING[op].get((col1, col2), ExpressionType.UNKNOWN),
            id=f"Op:{op},Cols:{(col1, col2)}",
        )
        for op in BINARY_OPS_RESULT_TYPE_MAPPING
        for col1 in COLS
        for col2 in COLS
    ],
)
def test_unary_ops_select_types(daft_df, col1, col2, op, expected_result_type):
    df = daft_df.select(getattr(col(col1), op)(col(col2)))

    exprs = [e for e in df.schema()]
    assert len(exprs) == 1
    expr = exprs[0]
    assert expr.name() == col1
    assert expr.resolved_type() == expected_result_type
