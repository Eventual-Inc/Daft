from typing import List, Optional

from daft.expressions import ColumnExpression, Expression
from daft.internal.treenode import TreeNode
from daft.logical.schema import PlanSchema


class LogicalPlan(TreeNode):
    def __init__(self, schema: PlanSchema) -> None:
        super().__init__()
        self._schema = schema

    def schema(self) -> PlanSchema:
        return self._schema

    def _validate_columns(self, exprs: List[Expression]) -> bool:
        for expr in exprs:
            for needed_col in expr.required_columns():
                if not self._schema.contains(needed_col):
                    raise ValueError(f"input does not have required col {needed_col}")
        return True


class Scan(LogicalPlan):
    def __init__(
        self,
        schema: PlanSchema,
        selections: Optional[List[Expression]] = None,
        columns: Optional[List[str]] = None,
    ) -> None:
        super().__init__(schema)
        self._output_schema = schema

        if selections is not None:
            self._validate_columns(selections)

        if columns is not None:
            self._validate_columns([ColumnExpression(c) for c in columns])
            self._output_schema = PlanSchema(columns)

        self._columns = columns
        self._selections = selections

    def schema(self) -> PlanSchema:
        return self._output_schema


class Selection(LogicalPlan):
    """Which rows to keep"""

    def __init__(self, input: LogicalPlan, predicate: List[Expression]) -> None:
        input._validate_columns(predicate)
        super().__init__(input.schema())
        self._input = self._register_child(input)
        self._predicate = predicate


class Projection(LogicalPlan):
    """Which columns to keep"""

    def __init__(self, input: LogicalPlan, predicate: List[Expression]) -> None:
        input._validate_columns(predicate)
        schema = PlanSchema.from_expressions(predicate)
        super().__init__(schema)

        self._input = self._register_child(input)
        self._predicate = predicate


class HStack(LogicalPlan):
    """zipping columns on without joining"""

    def __init__(self, input: LogicalPlan, to_add: List[Expression]) -> None:
        input._validate_columns(to_add)
        schema = input.schema().add_columns(PlanSchema.from_expressions(to_add).fields)
        super().__init__(schema)
        self._input = self._register_child(input)
        self._to_add = to_add
