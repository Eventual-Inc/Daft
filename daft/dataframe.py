from __future__ import annotations

import csv
from typing import Any, Dict, List, TypeVar

from daft.expressions import Expression, col
from daft.filesystem import get_filesystem_from_path
from daft.logical import logical_plan
from daft.logical.schema import ExpressionList

UDFReturnType = TypeVar("UDFReturnType", covariant=True)


class DataFrame:
    def __init__(self, plan: logical_plan.LogicalPlan) -> None:
        self._plan = plan

    def explain(self) -> logical_plan.LogicalPlan:
        return self._plan

    def schema(self) -> ExpressionList:
        return self._plan.schema()

    ###
    # Creation methods
    ###

    @classmethod
    def from_pylist(cls, data: List[Dict[str, Any]]) -> DataFrame:
        if not data:
            raise ValueError("Unable to create DataFrame from empty list")
        schema = ExpressionList([col(header) for header in data[0]])
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
        )
        return cls(plan)

    @classmethod
    def from_pydict(cls, data: Dict[str, Any]) -> DataFrame:
        schema = ExpressionList([col(header) for header in data])
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
        )
        return cls(plan)

    @classmethod
    def from_csv(cls, path: str, headers: bool = True, delimiter: str = ",") -> DataFrame:
        fs = get_filesystem_from_path(path)

        # Read first row to ascertain schema
        schema = None
        with fs.open(path, "r") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                schema = (
                    ExpressionList([col(header) for header in row])
                    if headers
                    else ExpressionList([col(f"col_{i}") for i in range(len(row))])
                )
                break
        assert schema is not None, "Unable to read CSV file to determine schema"

        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
        )
        return cls(plan)

    ###
    # DataFrame operations
    ###

    # def with_column(self, column_name: str, udf: UDFContext[UDFReturnType]) -> DataFrame:
    #     return DataFrame(new_plan)

    def select(self, *columns: str) -> DataFrame:
        undefined_columns = {c for c in columns} - {
            col_expr.name() for col_expr in self._plan.schema().to_column_expressions()
        }
        if undefined_columns:
            raise ValueError(f"Columns not found in schema: {undefined_columns}")
        projection = logical_plan.Projection(self._plan, ExpressionList([col(c) for c in columns]))
        return DataFrame(projection)

    def where(self, expr: Expression) -> DataFrame:
        plan = logical_plan.Filter(self._plan, ExpressionList([expr]))
        return DataFrame(plan)

    # def limit(self): ...
