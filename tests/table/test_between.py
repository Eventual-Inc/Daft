import datetime

import pytest

from daft import col
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "value,lower,upper,expected",
    [
        pytest.param([1, 2, 3, 4], 1, 2, [True, True, False, False], id="IntIntInt"),
        pytest.param([1, 2, 3, 4], 1.0, 2.0, [True, True, False, False], id="IntFloatFloat"),
        pytest.param([1, 2, 3, 4], 1, 2.0, [True, True, False, False], id="IntIntFloat"),
        pytest.param([1.0, 2.0, 3.0, 4.0], 1.0, 2.0, [True, True, False, False], id="FloatFloatFloat"),
        pytest.param([1.0, 2.0, 3.0, 4.0], 1, 2, [True, True, False, False], id="FloatIntInt"),
        pytest.param([1.0, 2.0, 3.0, 4.0], 1, 2.0, [True, True, False, False], id="FloatIntFloat"),
        pytest.param([1.0, 2.0, 3.0, 4.0], None, 1, [None, None, None, None], id="FloatNullInt"),
        pytest.param([1.0, 2.0, 3.0, 4.0], 1, None, [None, None, None, None], id="FloatIntNull"),
        pytest.param([1.0, 2.0, 3.0, 4.0], None, None, [None, None, None, None], id="FloatNullNull"),
        pytest.param([None, None, None, None], None, None, [None, None, None, None], id="NullNullNull"),
        pytest.param([None, None, None, None], 1, 1, [None, None, None, None], id="NullIntInt"),
        pytest.param(
            [datetime.datetime(2023, 1, 1), datetime.datetime(2022, 1, 1)],
            datetime.datetime(2022, 12, 30),
            datetime.datetime(2023, 1, 2),
            [True, False],
            id="Datetime",
        ),
    ],
)
def test_table_expr_between_scalars(value, lower, upper, expected) -> None:
    daft_table = MicroPartition.from_pydict({"value": value})
    daft_table = daft_table.eval_expression_list([col("value").between(lower, upper)])
    pydict = daft_table.to_pydict()
    assert pydict["value"] == expected


@pytest.mark.parametrize(
    "value,lower,upper,expected",
    [
        pytest.param([1, 2, 3, 4], [1, 1, 1, 1], [2, 2, 2, 2], [True, True, False, False], id="IntIntInt"),
        pytest.param(
            [1, 2, 3, 4], [1.0, 1.0, 1.0, 1.0], [2.0, 2.0, 2.0, 2.0], [True, True, False, False], id="IntFloatFloat"
        ),
        pytest.param([1, 2, 3, 4], [1, 1, 1, 1], [2.0, 2.0, 2.0, 2.0], [True, True, False, False], id="IntIntFloat"),
        pytest.param(
            [None, None, None, None],
            [2.0, 2.0, 2.0, 2.0],
            [2.0, 2.0, 2.0, 2.0],
            [None, None, None, None],
            id="NullFloatFloat",
        ),
        pytest.param(
            [None, None, None, None],
            [None, None, None, None],
            [2.0, 2.0, 2.0, 2.0],
            [None, None, None, None],
            id="NullNullFloat",
        ),
        pytest.param(
            [None, None, None, None],
            [2.0, 2.0, 2.0, 2.0],
            [None, None, None, None],
            [None, None, None, None],
            id="NullFloatNull",
        ),
        pytest.param(
            [None, None, None, None],
            [None, None, None, None],
            [None, None, None, None],
            [None, None, None, None],
            id="NullNullNull",
        ),
        pytest.param(
            [1.0, 2.0, 3.0, 4.0],
            [1.0, 1.0, 1.0, 1.0],
            [2.0, 2.0, 2.0, 2.0],
            [True, True, False, False],
            id="FloatFloatFloat",
        ),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1, 1, 1, 1], [2, 2, 2, 2], [True, True, False, False], id="FloatIntInt"),
        pytest.param(
            [1.0, 2.0, 3.0, 4.0], [1, 1, 1, 1], [2.0, 2.0, 2.0, 2.0], [True, True, False, False], id="FloatIntFloat"
        ),
        pytest.param(
            [datetime.datetime(2023, 1, 1), datetime.datetime(2022, 1, 1)],
            [datetime.datetime(2022, 12, 30), datetime.datetime(2022, 12, 30)],
            [datetime.datetime(2023, 1, 2), datetime.datetime(2023, 1, 2)],
            [True, False],
            id="Datetime",
        ),
    ],
)
def test_between_columns(value, lower, upper, expected) -> None:
    table = {"value": value, "lower": lower, "upper": upper}
    daft_table = MicroPartition.from_pydict(table)
    daft_table = daft_table.eval_expression_list([col("value").between(col("lower"), col("upper"))])
    pydict = daft_table.to_pydict()
    assert pydict["value"] == expected


@pytest.mark.parametrize(
    "value,lower,upper",
    [
        pytest.param(["str1", "str2"], 1, 2, id="StrIntInt"),
        pytest.param([1, 2], "str", 1, id="IntStrInt"),
    ],
)
def test_between_between_different_types(value, lower, upper) -> None:
    daft_table = MicroPartition.from_pydict({"a": value})
    with pytest.raises(ValueError):
        daft_table = daft_table.eval_expression_list([col("a").between(lower, upper)])


def test_between_bad_input() -> None:
    daft_table = MicroPartition.from_pydict({"a": [1, 2, 3]})
    with pytest.raises(TypeError):
        daft_table = daft_table.eval_expression_list([col("a").between([1, 2, 3], 1)])


@pytest.mark.parametrize(
    "value,lower,upper,expected",
    [
        pytest.param([1, 2, 3, 4], [1, 2, 3, 4], 2, [True, True, False, False], id="IntIntInt"),
        pytest.param([1, 2, 3, 4], [1.0, 2.0, 3.0, 4.0], 2.0, [True, True, False, False], id="IntFloatFloat"),
        pytest.param([1, 2, 3, 4], [1, 2, 3, 4], 2.0, [True, True, False, False], id="IntIntFloat"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0], 2.0, [True, True, False, False], id="FloatFloatFloat"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0], 2, [True, True, False, False], id="FloatIntInt"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1, 2, 3, 4], 2.0, [True, True, False, False], id="FloatIntFloat"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [None, None, None, None], 1, [None, None, None, None], id="FloatNullInt"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1, 2, 3, 4], None, [None, None, None, None], id="FloatIntNull"),
        pytest.param(
            [1.0, 2.0, 3.0, 4.0], [None, None, None, None], None, [None, None, None, None], id="FloatNullNull"
        ),
        pytest.param(
            [None, None, None, None], [None, None, None, None], None, [None, None, None, None], id="NullNullNull"
        ),
        pytest.param([None, None, None, None], [1, 2, 3, 4], 1, [None, None, None, None], id="NullIntInt"),
        pytest.param(
            [datetime.datetime(2023, 1, 1), datetime.datetime(2022, 1, 1)],
            [datetime.datetime(2022, 12, 30), datetime.datetime(2022, 12, 30)],
            datetime.datetime(2023, 1, 2),
            [True, False],
            id="Datetime",
        ),
    ],
)
def test_table_expr_between_col_and_scalar(value, lower, upper, expected) -> None:
    table = {"value": value, "lower": lower}
    daft_table = MicroPartition.from_pydict(table)
    daft_table = daft_table.eval_expression_list([col("value").between(col("lower"), upper)])
    pydict = daft_table.to_pydict()
    assert pydict["value"] == expected
