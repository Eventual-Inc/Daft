from typing import List, Optional

from daft.expressions import Expression
from daft.internal.treenode import TreeNode
from daft.logical.schema import PlanSchema


class LogicalPlan(TreeNode):
    _schema: PlanSchema

    def schema(self) -> PlanSchema:
        return self._schema


class Scan(LogicalPlan):
    def __init__(
        self,
        schema: PlanSchema,
        predicates: Optional[List[Expression]] = None,
        selections: Optional[List[Expression]] = None,
    ) -> None:
        self._schema = schema
        self._predicates = predicates
        self._selections = selections


class Selection(LogicalPlan):
    """Which rows to keep"""

    def __init__(self, input: LogicalPlan, predicate: List[Expression]) -> None:
        self._input = self._register_child(input)
        self._predicate = predicate


class Projection(LogicalPlan):
    """Which columns to keep"""

    def __init__(self, input: LogicalPlan, predicate: List[Expression]) -> None:
        self._input = self._register_child(input)
        self._predicate = predicate


class HStack(LogicalPlan):
    """zipping columns on without joining"""

    def __init__(self, input: LogicalPlan, to_add: List[Expression]) -> None:
        self._input = self._register_child(input)
        self._to_add = to_add
