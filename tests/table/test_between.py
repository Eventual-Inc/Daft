import datetime
import pytest

from daft import col
from daft.table import MicroPartition

@pytest.mark.parametrize(
    "value,lower,upper,expected",
    [
        pytest.param([1, 2, 3, 4], 1, 2, [True, True, False, False], id="IntIntInt"),
        pytest.param([1, 2, 3, 4], 1., 2., [True, True, False, False], id="IntFloatFloat"),
        pytest.param([1, 2, 3, 4], 1, 2., [True, True, False, False], id="IntIntFloat"),
        pytest.param([1., 2., 3., 4.], 1., 2., [True, True, False, False], id="FloatFloatFloat"),
        pytest.param([1., 2., 3., 4.], 1, 2, [True, True, False, False], id="FloatIntInt"),
        pytest.param([1., 2., 3., 4.], 1, 2., [True, True, False, False], id="FloatIntFloat"),
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
        pytest.param([1, 2, 3, 4], [1., 1., 1., 1.], [2., 2., 2., 2.], [True, True, False, False], id="IntFloatFloat"),
        pytest.param([1, 2, 3, 4], [1, 1, 1, 1],[2., 2., 2., 2.], [True, True, False, False], id="IntIntFloat"),
        pytest.param([1., 2., 3., 4.],[1., 1., 1., 1.], [2., 2., 2., 2.], [True, True, False, False], id="FloatFloatFloat"),
        pytest.param([1., 2., 3., 4.], [1, 1, 1, 1], [2, 2, 2, 2], [True, True, False, False], id="FloatIntInt"),
        pytest.param([1., 2., 3., 4.], [1, 1, 1, 1], [2., 2., 2., 2.], [True, True, False, False], id="FloatIntFloat"),
        pytest.param(
            [datetime.datetime(2023, 1, 1), datetime.datetime(2022, 1, 1)],
            datetime.datetime(2022, 12, 30),
            datetime.datetime(2023, 1, 2),
            [True, False],
            id="Datetime",
        ),
        
    ],
)
def test_between_columns(value, lower, upper, expected) -> None:
    table = {"value": value,
             "lower": lower,
             "upper": upper}
    daft_table = MicroPartition.from_pydict(table)
    daft_table = daft_table.eval_expression_list([col("value").between(col("lower"), col("upper"))])
    pydict = daft_table.to_pydict()
    assert pydict["value"] == expected
