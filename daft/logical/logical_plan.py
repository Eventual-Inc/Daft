from typing import List, Optional

from daft.expressions import ColumnExpression
from daft.internal.treenode import TreeNode
from daft.logical.schema import ExpressionList


class LogicalPlan(TreeNode):
    def __init__(self, schema: ExpressionList) -> None:
        super().__init__()
        self._schema = schema

    def schema(self) -> ExpressionList:
        return self._schema


class Scan(LogicalPlan):
    def __init__(
        self,
        schema: ExpressionList,
        selections: Optional[ExpressionList] = None,
        columns: Optional[List[str]] = None,
    ) -> None:
        schema = schema.resolve()
        super().__init__(schema)

        if selections is not None:
            selections = selections.resolve(schema)

        if columns is not None:
            new_schema = ExpressionList([ColumnExpression(c) for c in columns])
            self._output_schema = new_schema.resolve(schema)
        else:
            self._output_schema = schema

        self._columns = columns
        self._selections = selections

    def schema(self) -> ExpressionList:
        return self._output_schema

    def __repr__(self) -> str:
        return f"Scan\n\tschema={self.schema()}\n\tselection={self._selections}\n\tcolumns={self._columns}"


class Selection(LogicalPlan):
    """Which rows to keep"""

    def __init__(self, input: LogicalPlan, predicate: ExpressionList) -> None:
        super().__init__(input.schema().to_column_expressions())
        self._input = self._register_child(input)
        self._predicate = predicate.resolve(input.schema())

    def __repr__(self) -> str:
        return f"Selection\n\toutput={self.schema()}\n\tpredicate={self._predicate}"


class Projection(LogicalPlan):
    """Which columns to keep"""

    def __init__(self, input: LogicalPlan, projection: ExpressionList) -> None:
        projection = projection.resolve(input_schema=input.schema())
        super().__init__(projection)

        self._input = self._register_child(input)
        self._projection = projection

    def __repr__(self) -> str:
        return f"Projection\n\toutput={self.schema()}"


class Sort(LogicalPlan):
    ...
