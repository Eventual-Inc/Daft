import pytest

from daft.expressions import col
from daft.internal.rule import RuleRunner
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
    pred_push_down = PushDownPredicates()
    scan = Scan(schema)

    input = scan
    for i in range(3):
        input = Projection(input, ExpressionList([col("b") * 2, col("a")]))

    full_select = Filter(input, ExpressionList([col("a") == 1, col("b") < 10]))
    full_select = Filter(full_select, ExpressionList([col("a") == 1, col("b") > 20]))

    print(full_select.to_dot())
    runner = RuleRunner([pred_push_down])

    output = runner.run_single_rule(full_select, pred_push_down)
    print(output.to_dot())

    pred_push_down_into_scan = PushDownClausesIntoScan()

    output_scan = runner.run_single_rule(output, pred_push_down_into_scan)

    print(output_scan.to_dot())

    fold_proj_rule = FoldProjections()

    fold_projs = runner.run_single_rule(output_scan, fold_proj_rule)

    print(fold_projs.to_dot())

    # assert isinstance(output, Projection)
