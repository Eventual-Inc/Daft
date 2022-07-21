import pytest

from daft.expressions import col
from daft.logical.logical_plan import Projection, Scan, Selection
from daft.logical.optimizer import PushDownPredicates
from daft.logical.schema import ExpressionList


@pytest.fixture(scope="function")
def schema():
    return ExpressionList(list(map(col, ["a", "b", "c"])))


def test_pred_push_down(schema):
    rule = PushDownPredicates()
    scan = Scan(schema)

    project = Projection(scan, ExpressionList([col("b"), col("a")]))

    full_select = Selection(project, ExpressionList([col("a") == 1, col("b") < 10]))
    rule._selection_through_projection(full_select, project)
