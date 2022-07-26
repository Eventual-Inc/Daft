from abc import abstractmethod
from typing import Any, List, Optional

from daft.expressions import ColumnExpression
from daft.internal.treenode import TreeNode
from daft.logical.schema import ExpressionList


class LogicalPlan(TreeNode["LogicalPlan"]):
    def __init__(self, schema: ExpressionList) -> None:
        super().__init__()
        self._schema = schema

    def schema(self) -> ExpressionList:
        return self._schema

    @abstractmethod
    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    @abstractmethod
    def _local_eq(self, other: Any) -> bool:
        raise NotImplementedError()

    def is_eq(self, other: Any) -> bool:
        return (
            isinstance(other, LogicalPlan)
            and self._local_eq(other)
            and self.schema() == other.schema()
            and all(
                [self_child.is_eq(other_child) for self_child, other_child in zip(self._children(), other._children())]
            )
        )

    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError(
            "The == operation is not implemented. "
            "Use .is_eq() to check if expressions are 'equal' (ignores differences in IDs but checks for the same expression structure)"
        )


class Scan(LogicalPlan):
    def __init__(
        self,
        schema: ExpressionList,
        predicate: Optional[ExpressionList] = None,
        columns: Optional[List[str]] = None,
    ) -> None:
        schema = schema.resolve()
        super().__init__(schema)

        if predicate is not None:
            self._predicate = predicate.resolve(schema)
        else:
            self._predicate = ExpressionList([])

        if columns is not None:
            new_schema = ExpressionList([ColumnExpression(c) for c in columns])
            self._output_schema = new_schema.resolve(schema)
        else:
            self._output_schema = schema

        self._columns = self._schema

    def schema(self) -> ExpressionList:
        return self._output_schema

    def __repr__(self) -> str:
        return f"Scan\n\toutput={self.schema()}\n\tpredicate={self._predicate}\n\tcolumns={self._columns}"

    def required_columns(self) -> ExpressionList:
        return self._predicate.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Scan)
            and self.schema() == other.schema()
            and self._predicate == other._predicate
            and self._columns == other._columns
        )


class Filter(LogicalPlan):
    """Which rows to keep"""

    def __init__(self, input: LogicalPlan, predicate: ExpressionList) -> None:
        super().__init__(input.schema().to_column_expressions())
        self._register_child(input)
        self._predicate = predicate.resolve(input.schema())

    def __repr__(self) -> str:
        return f"Filter\n\toutput={self.schema()}\n\tpredicate={self._predicate}"

    def required_columns(self) -> ExpressionList:
        return self._predicate.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, Filter) and self.schema() == other.schema() and self._predicate == other._predicate


class Projection(LogicalPlan):
    """Which columns to keep"""

    def __init__(self, input: LogicalPlan, projection: ExpressionList) -> None:
        projection = projection.resolve(input_schema=input.schema())
        super().__init__(projection)

        self._register_child(input)
        self._projection = projection

    def __repr__(self) -> str:
        return f"Projection\n\toutput={self.schema()}"

    def required_columns(self) -> ExpressionList:
        return self._projection.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Projection) and self.schema() == other.schema() and self._projection == other._projection
        )


class Sort(LogicalPlan):
    ...
