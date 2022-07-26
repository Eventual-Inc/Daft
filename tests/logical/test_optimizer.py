import pytest

from daft.expressions import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import Filter, Projection, Scan
from daft.logical.optimizer import (
    FoldProjections,
    PushDownClausesIntoScan,
    PushDownPredicates,
)
from daft.logical.schema import ExpressionList


@pytest.fixture(scope="function")
def schema():
    return ExpressionList(list(map(col, ["a", "b", "c"])))


def test_pred_push_down(schema):
    PushDownPredicates()
    optimizer = RuleRunner(
        [RuleBatch("pred_pushdown", Once, [PushDownPredicates(), PushDownClausesIntoScan(), FoldProjections()])]
    )

    scan = Scan(schema)

    input = scan
    for i in range(3):
        input = Projection(input, ExpressionList([col("b") * 2, col("a")]))

    full_select = Filter(input, ExpressionList([col("a") == 1, col("b") < 10]))
    full_select = Filter(full_select, ExpressionList([col("a") == 1, col("b") > 20]))

    optimizer(full_select)
