from __future__ import annotations

import itertools
import math
import operator as ops

import numpy as np
import pyarrow as pa
import pytest

from daft import col, lit
from daft.recordbatch import MicroPartition
from tests.recordbatch import daft_numeric_types

OPS = [
    ops.add,
    ops.sub,
    ops.mul,
    ops.truediv,
    ops.floordiv,
    ops.mod,
    ops.lt,
    ops.le,
    ops.eq,
    ops.ne,
    ops.ge,
    ops.gt,
]


def lists_close_with_nones(a, b):
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if x is None and y is None:
            continue
        if x is not None and y is not None:
            np.testing.assert_allclose([x], [y])
        else:
            return False
    return True


@pytest.mark.parametrize("data_dtype, op", itertools.product(daft_numeric_types, OPS))
def test_table_numeric_expressions(data_dtype, op) -> None:
    a, b = [5, 6, 7, 8], [1, 2, 3, 4]
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    daft_recordbatch = MicroPartition.from_arrow(pa_table)
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [op(col("a").cast(data_dtype), col("b").cast(data_dtype)).alias("result")]
    )

    assert len(daft_recordbatch) == 4
    assert daft_recordbatch.column_names() == ["result"]
    pyresult = [op(left, right) for left, right in zip(a, b)]
    assert daft_recordbatch.get_column_by_name("result").to_pylist() == pyresult


@pytest.mark.parametrize("data_dtype, op", itertools.product(daft_numeric_types, OPS))
def test_table_numeric_expressions_with_nulls(data_dtype, op) -> None:
    a, b = [5, 6, None, 8, None], [1, 2, 3, None, None]
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    daft_recordbatch = MicroPartition.from_arrow(pa_table)
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [op(col("a").cast(data_dtype), col("b").cast(data_dtype)).alias("result")]
    )

    assert len(daft_recordbatch) == 5
    assert daft_recordbatch.column_names() == ["result"]
    pyresult = [op(left, right) for left, right in zip(a[:2], b[:2])]
    assert daft_recordbatch.get_column_by_name("result").to_pylist()[:2] == pyresult

    assert daft_recordbatch.get_column_by_name("result").to_pylist()[2:] == [None, None, None]


def test_table_numeric_abs() -> None:
    table = MicroPartition.from_pydict({"a": [None, -1.0, 0, 2, 3, None], "b": [-1, -2, 3, 4, None, None]})

    abs_table = table.eval_expression_list([abs(col("a")), col("b").abs()])

    assert [
        abs(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()
    ] == abs_table.get_column_by_name("a").to_pylist()
    assert [
        abs(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()
    ] == abs_table.get_column_by_name("b").to_pylist()


def test_table_abs_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to abs to be numeric"):
        table.eval_expression_list([abs(col("a"))])


def test_table_numeric_ceil() -> None:
    table = MicroPartition.from_pydict(
        {
            "a": [None, -1.0, -0.5, 0, 0.5, 2, None],
            "b": [-1.7, -1.5, -1.3, 0.3, 0.7, None, None],
        }
    )

    ceil_table = table.eval_expression_list([col("a").ceil(), col("b").ceil()])

    assert [
        math.ceil(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()
    ] == ceil_table.get_column_by_name("a").to_pylist()
    assert [
        math.ceil(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()
    ] == ceil_table.get_column_by_name("b").to_pylist()


def test_table_ceil_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to ceil to be numeric"):
        table.eval_expression_list([col("a").ceil()])


def test_table_numeric_floor() -> None:
    table = MicroPartition.from_pydict(
        {
            "a": [None, -1.0, -0.5, 0.0, 0.5, 2, None],
            "b": [-1.7, -1.5, -1.3, 0.3, 0.7, None, None],
        }
    )

    floor_table = table.eval_expression_list([col("a").floor(), col("b").floor()])

    assert [
        math.floor(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()
    ] == floor_table.get_column_by_name("a").to_pylist()
    assert [
        math.floor(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()
    ] == floor_table.get_column_by_name("b").to_pylist()


def test_table_floor_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to floor to be numeric"):
        table.eval_expression_list([col("a").floor()])


@pytest.mark.parametrize(("fun"), ["sign", "signum"])
def test_table_numeric_sign(fun: str) -> None:
    table = MicroPartition.from_pydict(
        {
            "a": [None, -1, -5, 0, 5, 2, None],
            "b": [-1.7, -1.5, -1.3, 0.3, 0.7, None, None],
        }
    )
    my_schema = pa.schema([pa.field("uint8", pa.uint8())])
    table_Unsign = MicroPartition.from_arrow(pa.Table.from_arrays([pa.array([None, 0, 1, 2, 3])], schema=my_schema))

    sign_table = table.eval_expression_list([getattr(col("a"), fun)(), getattr(col("b"), fun)()])
    unsign_sign_table = table_Unsign.eval_expression_list([getattr(col("uint8"), fun)()])

    def checkSign(val):
        if val < 0:
            return -1
        if val > 0:
            return 1
        return 0

    assert [
        checkSign(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()
    ] == sign_table.get_column_by_name("a").to_pylist()
    assert [
        checkSign(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()
    ] == sign_table.get_column_by_name("b").to_pylist()
    assert [
        checkSign(v) if v is not None else v for v in table_Unsign.get_column_by_name("uint8").to_pylist()
    ] == unsign_sign_table.get_column_by_name("uint8").to_pylist()


@pytest.mark.parametrize(
    ("fun"),
    [
        "sign",
        "signum",
        "negate",
        "negative",
    ],
)
def test_table_sign_bad_input(fun: str) -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    mapping = {"negate": "negative", "signum": "sign"}
    true_fun = mapping[fun] if fun in mapping else fun
    with pytest.raises(ValueError, match=f"Expected input to {true_fun} to be numeric"):
        table.eval_expression_list([getattr(col("a"), fun)()])


@pytest.mark.parametrize(
    ("fun"),
    [
        ("negate"),
        ("negative"),
    ],
)
def test_table_numeric_negative(fun: str) -> None:
    table = MicroPartition.from_pydict(
        {
            "a": [None, -1, -5, 0, 5, 2, None],
            "b": [-1.7, -1.5, -1.3, 0.3, 0.7, None, None],
        }
    )
    sign_table = table.eval_expression_list([getattr(col("a"), fun)(), getattr(col("b"), fun)()])

    # Check signed integers
    a_result = sign_table.to_pydict()["a"]
    expected_a = [None, 1, 5, 0, -5, -2, None]
    assert a_result == expected_a

    # Check floating point
    b_result = sign_table.to_pydict()["b"]
    expected_b = [1.7, 1.5, 1.3, -0.3, -0.7, None, None]
    assert b_result == expected_b


@pytest.mark.parametrize(
    ("fun", "is_arc", "is_co"),
    [
        ("sin", False, False),
        ("cos", False, False),
        ("tan", False, False),
        ("sinh", False, False),
        ("cosh", False, False),
        ("arcsin", True, False),
        ("arccos", True, False),
        ("arctan", True, False),
        ("radians", False, False),
        ("degrees", False, False),
        ("csc", False, True),
        ("sec", False, True),
        ("cot", False, True),
    ],
)
def test_table_numeric_trigonometry(fun: str, is_arc: bool, is_co: bool) -> None:
    np_fun = fun
    if not is_arc:
        table = MicroPartition.from_pydict({"a": [0.0, math.pi, math.pi / 2, math.nan]})
        if is_co:
            reciprocal = {"cot": "tan", "csc": "sin", "sec": "cos"}
            np_fun = reciprocal[fun]
    else:
        table = MicroPartition.from_pydict({"a": [0.0, 1, 0.5, math.nan]})
    s = table.to_pandas()["a"]
    np_result = getattr(np, np_fun)(s)
    if is_co:
        np_result = 1 / np_result

    trigonometry_table = table.eval_expression_list([getattr(col("a"), fun)()])
    assert (
        all(
            x == pytest.approx(y, rel=1e-9) or (math.isnan(x) and math.isnan(y))
            for x, y in zip(trigonometry_table.get_column_by_name("a").to_pylist(), np_result.to_list())
        )
        is True
    )


def test_table_numeric_tanh() -> None:
    table = MicroPartition.from_pydict({"a": [0.0, math.pi, math.pi / 2, math.nan]})
    s = table.to_pandas()["a"]
    np_result = np.tanh(s)
    arct = table.eval_expression_list([col("a").tanh()])
    assert (
        all(
            x - y < 1.0e-10
            or (x is None and y is None)
            or (math.isnan(x) and math.isnan(y) or math.isinf(x) and math.isinf(y))
            for x, y in zip(arct.get_column_by_name("a").to_pylist(), np_result.to_list())
        )
        is True
    )


def test_table_numeric_arc_trigonometry_oor() -> None:
    table = MicroPartition.from_pydict({"a": [math.pi, 2]})
    cot_table = table.eval_expression_list([col("a").arcsin(), col("a").arccos().alias("b")])
    assert all(math.isnan(x) for x in cot_table.get_column_by_name("a").to_pylist())
    assert all(math.isnan(x) for x in cot_table.get_column_by_name("b").to_pylist())


def test_table_numeric_cot() -> None:
    table = MicroPartition.from_pydict({"a": [0, None, math.nan]})
    cot_table = table.eval_expression_list([col("a").cot()])
    expected = [math.inf, None, math.nan]
    assert (
        all(
            x == y or (math.isnan(x) and math.isnan(y)) or (math.isinf(x) and math.isinf(y))
            for x, y in zip(cot_table.get_column_by_name("a").to_pylist(), expected)
        )
        is True
    )


def test_table_numeric_atan2() -> None:
    # cartesian product of y and x tables
    table = MicroPartition.from_pydict(
        {
            "y": [0.0, 1.0, 0.5, -0.5, -0.0, math.nan, 0.0, math.nan],
            "x": [0.0, 0.0, 0.5, 0.5, -10.0, math.nan, math.nan, 1.0],
        }
    )
    pds = table.to_pandas()
    np_result = np.arctan2(pds["y"], pds["x"])

    atan2_table = table.eval_expression_list([col("y").arctan2(col("x"))])
    assert (
        all(
            a == b or (a is None and b is None) or (math.isnan(a) and math.isnan(b))
            for a, b in zip(atan2_table.get_column_by_name("y").to_pylist(), np_result.to_list())
        )
        is True
    )


def test_table_numeric_atan2_literals() -> None:
    table = MicroPartition.from_pydict({"y": [0.0, 1.0, -1.0, math.nan]})
    pds = table.to_pandas()
    literals = [0.0, 1.0, -1.0, math.nan]
    # lhs has value, rhs has literal
    for litv in literals:
        np_result = np.arctan2(pds["y"], np.repeat(litv, len(pds)))
        atan2_table = table.eval_expression_list([col("y").arctan2(lit(litv))])
        assert (
            all(
                a == b or (a is None and b is None) or (math.isnan(a) and math.isnan(b))
                for a, b in zip(atan2_table.get_column_by_name("y").to_pylist(), np_result.to_list())
            )
            is True
        )

    # lhs has literal, rhs has value
    for litv in literals:
        np_result = np.arctan2(np.repeat(litv, len(pds)), pds["y"])
        atan2_table = table.eval_expression_list([lit(litv).arctan2(col("y"))])
        assert (
            all(
                a == b or (a is None and b is None) or (math.isnan(a) and math.isnan(b))
                for a, b in zip(atan2_table.get_column_by_name("literal").to_pylist(), np_result.to_list())
            )
            is True
        )


def test_table_numeric_arctanh() -> None:
    table = MicroPartition.from_pydict({"a": [0.0, 0.5, 0.9, -0.9, -0.5, -0.0, 1, -1.3, math.nan]})
    s = table.to_pandas()["a"]
    np_result = np.arctanh(s)

    arct = table.eval_expression_list([col("a").arctanh()])
    assert (
        all(
            x - y < 1.0e-10
            or (x is None and y is None)
            or (math.isnan(x) and math.isnan(y) or math.isinf(x) and math.isinf(y))
            for x, y in zip(arct.get_column_by_name("a").to_pylist(), np_result.to_list())
        )
        is True
    )


def test_table_numeric_arcsinh() -> None:
    table = MicroPartition.from_pydict({"a": [0.0, 1.0, 0.5, -0.5, -0.0, math.nan]})
    s = table.to_pandas()["a"]
    np_result = np.arcsinh(s)

    arcs = table.eval_expression_list([col("a").arcsinh()])
    assert (
        all(
            x - y < 1.0e-10 or (x is None and y is None) or (math.isnan(x) and math.isnan(y))
            for x, y in zip(arcs.get_column_by_name("a").to_pylist(), np_result.to_list())
        )
        is True
    )


def test_table_numeric_arccosh() -> None:
    table = MicroPartition.from_pydict({"a": [1.0, 2.0, 1.5, 0.5, math.nan]})
    s = table.to_pandas()["a"]
    np_result = np.arccosh(s)

    arcc = table.eval_expression_list([col("a").arccosh()])
    assert (
        all(
            x - y < 1.0e-10 or (x is None and y is None) or (math.isnan(x) and math.isnan(y))
            for x, y in zip(arcc.get_column_by_name("a").to_pylist(), np_result.to_list())
        )
        is True
    )


def test_table_numeric_round() -> None:
    from decimal import ROUND_HALF_UP, Decimal

    table = MicroPartition.from_pydict(
        {
            "a": [None, -1, -5, 0, 5, 2, None],
            "b": [-1.765, -1.565, -1.321, 0.399, 0.781, None, None],
        }
    )
    round_table = table.eval_expression_list([col("a").round(0), col("b").round(2)])
    assert [
        Decimal(v).to_integral_value(rounding=ROUND_HALF_UP) if v is not None else v
        for v in table.get_column_by_name("a").to_pylist()
    ] == round_table.get_column_by_name("a").to_pylist()
    assert [
        (float(Decimal(str(v)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)) if v is not None else v)
        for v in table.get_column_by_name("b").to_pylist()
    ] == round_table.get_column_by_name("b").to_pylist()


def test_table_round_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to be numeric"):
        table.eval_expression_list([col("a").round()])

    table = MicroPartition.from_pydict({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="failed to cast"):
        table.eval_expression_list([col("a").round(-2)])


def test_clip_one_sided_bounding():
    table = MicroPartition.from_pydict({"a": [1, 2, 3, 4, 5]})
    clip_table = table.eval_expression_list([col("a").clip(None, 3)])
    expected = [1, 2, 3, 3, 3]
    assert clip_table.get_column_by_name("a").to_pylist() == expected

    clip_table = table.eval_expression_list([col("a").clip(3, None)])
    expected = [3, 3, 3, 4, 5]
    assert clip_table.get_column_by_name("a").to_pylist() == expected


def test_clip_integer_float_promotion():
    table = MicroPartition.from_pydict({"a": [1, 2, 3, 4, 5]})
    clip_table = table.eval_expression_list([col("a").clip(2.5, 4.5)])
    expected = [2.5, 2.5, 3, 4, 4.5]
    assert clip_table.get_column_by_name("a").to_pylist() == expected


def test_clip_zero_handling():
    table = MicroPartition.from_pydict({"a": [-0.0, 0.0, 1.0, -1.0]})
    clip_table = table.eval_expression_list([col("a").clip(-0.5, 0.5)])
    expected = [-0.0, 0.0, 0.5, -0.5]
    assert clip_table.get_column_by_name("a").to_pylist() == expected


def test_clip_empty_array():
    table = MicroPartition.from_pydict({"a": []})
    with pytest.raises(ValueError):
        table.eval_expression_list([col("a").clip(0, 1)])


def test_clip_all_within_bounds():
    table = MicroPartition.from_pydict({"a": [2, 3, 4]})
    clip_table = table.eval_expression_list([col("a").clip(1, 5)])
    expected = [2, 3, 4]
    assert clip_table.get_column_by_name("a").to_pylist() == expected


def test_clip_all_out_of_bounds():
    table = MicroPartition.from_pydict({"a": [0, 1, 2, 3, 4, 5]})
    clip_table = table.eval_expression_list([col("a").clip(2, 3)])
    expected = [2, 2, 2, 3, 3, 3]
    assert clip_table.get_column_by_name("a").to_pylist() == expected


def test_clip_nan_handling():
    table = MicroPartition.from_pydict({"a": [1, 2, np.nan, 4, 5]})
    clip_table = table.eval_expression_list([col("a").clip(2, 4)])
    expected = [2, 2, np.nan, 4, 4]
    actual = clip_table.get_column_by_name("a").to_pylist()
    assert all((a == b or (np.isnan(a) and np.isnan(b))) for a, b in zip(actual, expected))


@pytest.mark.parametrize(
    "lower_bound, upper_bound, expected",
    [
        # Test case 1: Column lower bound with scalar upper bound
        ("lower_bound", 5, [1.0, 2.5, None, 4.7, 5.0, float("nan")]),
        # Test case 2: Scalar lower bound with column upper bound
        (2.0, "upper_bound", [2.0, 2.5, None, 4.7, 5.0, float("nan")]),
        # Test case 3: Column lower and upper bounds
        ("lower_bound", "upper_bound", [1.0, 2.5, None, 4.7, 5.0, float("nan")]),
        # Test case 4: Infinite bounds
        (float("-inf"), float("inf"), [1.0, 2.5, None, 4.7, 5.0, float("nan")]),
        # Test case 5: None bounds
        (None, None, [1.0, 2.5, None, 4.7, 5.0, float("nan")]),
        # Test case 6: Scalar bounds
        (2.0, 5.0, [2.0, 2.5, None, 4.7, 5.0, float("nan")]),
    ],
)
def test_clip(lower_bound, upper_bound, expected):
    """Test clipping a column with various combinations of scalar and column bounds."""
    # Initialize test data
    table = MicroPartition.from_pydict(
        {
            "data": [1.0, 2.5, None, 4.7, 5.0, float("nan")],
            "lower_bound": [0.5, 2.0, 1.0, None, 4.0, 0.0],
            "upper_bound": [2.0, 3.0, 5.0, None, None, float("inf")],
        }
    )

    # Prepare the clip expression
    lower = col(lower_bound) if isinstance(lower_bound, str) else lower_bound
    upper = col(upper_bound) if isinstance(upper_bound, str) else upper_bound

    # Perform the clip operation
    clip_table = table.eval_expression_list([col("data").clip(lower, upper)])
    actual = clip_table.get_column_by_name("data").to_pylist()

    # Verify results
    assert all(
        (a == b) or (a is None and b is None) or (math.isnan(a) and math.isnan(b)) for a, b in zip(actual, expected)
    ), f"Expected {expected}, got {actual}"


def test_clip_incompatible_lengths():
    table1 = MicroPartition.from_pydict({"data": [1, 2, 3, 4, 5]})
    table2 = MicroPartition.from_pydict({"data": [1, 2, 3]})  # Shorter array
    table3 = MicroPartition.from_pydict({"data": [1, 2, 3, 4, 5, 6]})  # Longer array

    # Test shorter lower bound
    with pytest.raises(ValueError):
        table1.eval_expression_list([col("data").clip(table2.get_column_by_name("data"), lit(5))])

    # Test longer upper bound
    with pytest.raises(ValueError):
        table1.eval_expression_list([col("data").clip(lit(0), table3.get_column_by_name("data"))])

    # Test both bounds with different lengths
    with pytest.raises(ValueError):
        table1.eval_expression_list(
            [col("data").clip(table2.get_column_by_name("data"), table3.get_column_by_name("data"))]
        )


def test_clip_unsupported_types():
    # Test with string data
    table = MicroPartition.from_pydict({"strings": ["a", "b", "c"], "numbers": [1, 2, 3]})

    # Test string column as data
    with pytest.raises(ValueError):
        table.eval_expression_list([col("strings").clip(0, 5)])

    # Test string column as lower bound
    with pytest.raises(ValueError):
        table.eval_expression_list([col("numbers").clip(col("strings"), 5)])

    # Test string column as upper bound
    with pytest.raises(ValueError):
        table.eval_expression_list([col("numbers").clip(0, col("strings"))])

    # Test string literals as bounds
    with pytest.raises(ValueError):
        table.eval_expression_list([col("numbers").clip("a", "z")])


def test_table_numeric_log2() -> None:
    table = MicroPartition.from_pydict({"a": [0.1, 0.01, 1.5, None], "b": [1, 10, None, None]})
    log2_table = table.eval_expression_list([col("a").log2(), col("b").log2()])
    assert [
        math.log2(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()
    ] == log2_table.get_column_by_name("a").to_pylist()
    assert [
        math.log2(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()
    ] == log2_table.get_column_by_name("b").to_pylist()


def test_table_log2_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to log to be numeric"):
        table.eval_expression_list([col("a").log2()])


def test_table_numeric_log10() -> None:
    table = MicroPartition.from_pydict({"a": [0.1, 0.01, 1.5, None], "b": [1, 10, None, None]})
    log10_table = table.eval_expression_list([col("a").log10(), col("b").log10()])
    assert [
        math.log10(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()
    ] == log10_table.get_column_by_name("a").to_pylist()
    assert [
        math.log10(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()
    ] == log10_table.get_column_by_name("b").to_pylist()


def test_table_log10_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to log to be numeric"):
        table.eval_expression_list([col("a").log10()])


@pytest.mark.parametrize(
    ("base"),
    [2, 10, 100, math.e],
)
def test_table_numeric_log(base: float) -> None:
    table = MicroPartition.from_pydict({"a": [0.1, 0.01, 1.5, None], "b": [1, 10, None, None]})
    log_table = table.eval_expression_list([col("a").log(base), col("b").log(base)])

    assert lists_close_with_nones(
        log_table.get_column_by_name("a").to_pylist(),
        [math.log(v, base) if v is not None else None for v in table.get_column_by_name("a").to_pylist()],
    )

    assert lists_close_with_nones(
        log_table.get_column_by_name("b").to_pylist(),
        [math.log(v, base) if v is not None else None for v in table.get_column_by_name("b").to_pylist()],
    )


def test_table_log_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to log to be numeric"):
        table.eval_expression_list([col("a").log(base=10)])


def test_table_numeric_ln() -> None:
    table = MicroPartition.from_pydict({"a": [0.1, 0.01, 1.5, None], "b": [1, 10, None, None]})
    ln_table = table.eval_expression_list([col("a").ln(), col("b").ln()])
    assert lists_close_with_nones(
        [math.log(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()],
        ln_table.get_column_by_name("a").to_pylist(),
    )
    assert lists_close_with_nones(
        [math.log(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()],
        ln_table.get_column_by_name("b").to_pylist(),
    )


def test_table_ln_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to log to be numeric"):
        table.eval_expression_list([col("a").ln()])


def test_table_numeric_log1p() -> None:
    table = MicroPartition.from_pydict({"a": [0.1, 0.01, 1.5, None], "b": [1, 10, None, None]})
    log1p_table = table.eval_expression_list([col("a").log1p(), col("b").log1p()])
    assert lists_close_with_nones(
        [math.log1p(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()],
        log1p_table.get_column_by_name("a").to_pylist(),
    )
    assert lists_close_with_nones(
        [math.log1p(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()],
        log1p_table.get_column_by_name("b").to_pylist(),
    )


def test_table_log1p_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to log to be numeric"):
        table.eval_expression_list([col("a").log1p()])


def test_table_expm1() -> None:
    table = MicroPartition.from_pydict({"a": [0.1, 0.01, None], "b": [1, 10, None]})
    expm1_table = table.eval_expression_list([col("a").expm1(), col("b").expm1()])
    assert lists_close_with_nones(
        [0.10517091807564763, 0.010050167084168058, None],
        expm1_table.get_column_by_name("a").to_pylist(),
    )
    assert lists_close_with_nones(
        [1.718281828459045, 22025.465794806718, None],
        expm1_table.get_column_by_name("b").to_pylist(),
    )


def test_table_exp() -> None:
    table = MicroPartition.from_pydict({"a": [0.1, 0.01, None], "b": [1, 10, None]})
    exp_table = table.eval_expression_list([col("a").exp(), col("b").exp()])
    assert lists_close_with_nones(
        [1.1051709180756477, 1.010050167084168, None],
        exp_table.get_column_by_name("a").to_pylist(),
    )
    assert lists_close_with_nones(
        [2.718281828459045, 22026.465794806718, None],
        exp_table.get_column_by_name("b").to_pylist(),
    )


def test_table_numeric_sqrt() -> None:
    table = MicroPartition.from_pydict({"a": [4, 9, None, 16, 25, None], "b": [2.25, 0.81, None, 1, 10.24, None]})
    sqrt_table = table.eval_expression_list([col("a").sqrt(), col("b").sqrt()])
    assert [
        math.sqrt(v) if v is not None else v for v in table.get_column_by_name("a").to_pylist()
    ] == sqrt_table.get_column_by_name("a").to_pylist()
    assert [
        math.sqrt(v) if v is not None else v for v in table.get_column_by_name("b").to_pylist()
    ] == sqrt_table.get_column_by_name("b").to_pylist()


@pytest.mark.parametrize(
    "left, right",
    [
        pytest.param([1, 2, 3], [4, 5, 6], id="NoNulls"),
        pytest.param([1, 2, None], [4, None, 6], id="WithNulls"),
    ],
)
def test_table_shift_left(left, right) -> None:
    table = MicroPartition.from_pydict({"a": left, "b": right})
    shift_left_table = table.eval_expression_list([col("a") << (col("b"))])
    assert shift_left_table.get_column_by_name("a").to_pylist() == [
        i << j if i is not None and j is not None else None for i, j in zip(left, right)
    ]


def test_table_shift_left_with_scalar() -> None:
    table = MicroPartition.from_pydict({"a": [1, 2, 4]})
    shift_left_table = table.eval_expression_list([col("a") << (1)])
    assert [1 << 1, 2 << 1, 4 << 1] == shift_left_table.get_column_by_name("a").to_pylist()


def test_table_shift_left_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Cannot operate shift left on types: Utf8, Utf8"):
        table.eval_expression_list([col("a") << (col("a"))])


def test_table_shift_left_bad_shift() -> None:
    table = MicroPartition.from_pydict({"a": [1, 2, 4], "b": [3, 2, 1]})

    with pytest.raises(ValueError, match="Cannot operate shift left on types: Int64, Utf8"):
        table.eval_expression_list([col("a") << (lit("a"))])


def test_table_shift_left_negative_bits() -> None:
    table = MicroPartition.from_pydict({"a": [1, 2, 4], "b": [3, 2, -1]})

    with pytest.raises(ValueError, match="Cannot shift left by a negative number"):
        table.eval_expression_list([col("a") << (col("b"))])


def test_table_shift_left_syntactic_sugar() -> None:
    table = MicroPartition.from_pydict({"a": [1, 2, 4]})
    shift_table = table.eval_expression_list([col("a").shift_left(1)])
    assert [1 << 1, 2 << 1, 4 << 1] == shift_table.get_column_by_name("a").to_pylist()


@pytest.mark.parametrize(
    "left, right",
    [
        pytest.param([1, 2, 3], [4, 5, 6], id="NoNulls"),
        pytest.param([1, 2, None], [4, None, 6], id="WithNulls"),
    ],
)
def test_table_shift_right(left, right) -> None:
    table = MicroPartition.from_pydict({"a": left, "b": right})
    shift_right_table = table.eval_expression_list([col("a") >> (col("b"))])
    assert shift_right_table.get_column_by_name("a").to_pylist() == [
        i >> j if i is not None and j is not None else None for i, j in zip(left, right)
    ]


def test_table_shift_right_with_scalar() -> None:
    table = MicroPartition.from_pydict({"a": [1, 2, 4]})
    shift_right_table = table.eval_expression_list([col("a") >> (1)])
    assert [1 >> 1, 2 >> 1, 4 >> 1] == shift_right_table.get_column_by_name("a").to_pylist()


def test_table_shift_right_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Cannot operate shift right on types: Utf8, Utf8"):
        table.eval_expression_list([col("a") >> (col("a"))])


def test_table_shift_right_bad_shift() -> None:
    table = MicroPartition.from_pydict({"a": [8, 4, 2], "b": [3, 2, 1]})

    with pytest.raises(ValueError, match="Cannot operate shift right on types: Int64, Utf8"):
        table.eval_expression_list([col("a") >> (lit("a"))])


def test_table_shift_right_negative_bits() -> None:
    table = MicroPartition.from_pydict({"a": [8, 4, 2], "b": [3, 2, -1]})

    with pytest.raises(ValueError, match="Cannot shift right by a negative number"):
        table.eval_expression_list([col("a") >> (col("b"))])


def test_table_shift_right_syntactic_sugar() -> None:
    table = MicroPartition.from_pydict({"a": [1, 2, 4]})
    shift_table = table.eval_expression_list([col("a").shift_right(1)])
    assert [1 >> 1, 2 >> 1, 4 >> 1] == shift_table.get_column_by_name("a").to_pylist()
