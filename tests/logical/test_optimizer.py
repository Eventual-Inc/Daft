import pytest

from daft.expressions import col
from daft.internal.rule import RuleRunner
from daft.logical.logical_plan import Projection, Scan, Selection
from daft.logical.optimizer import PushDownPredicates
from daft.logical.schema import ExpressionList


@pytest.fixture(scope="function")
def schema():
    return ExpressionList(list(map(col, ["a", "b", "c"])))


def test_pred_push_down(schema):
    rule = PushDownPredicates()
    scan = Scan(schema)

    input = scan
    for i in range(3):
        input = Projection(input, ExpressionList([col("b"), col("a")]))

    full_select = Selection(input, ExpressionList([col("a") == 1, col("b") < 10]))

    runner = RuleRunner([rule])
    output = runner.run_single_rule(full_select, rule)
    print(output.to_dot())
    assert isinstance(output, Projection)
