"""End-to-end tests backing the custom source pushdowns documentation.

See docs/connectors/custom.md ("Applying Pushdowns").
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pyarrow as pa

from daft import col
from daft.expressions import Expression, ExpressionVisitor
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema


class EqualityConstraints(ExpressionVisitor[dict[str, Any]]):
    """Collect ``column == literal`` conjuncts from a filter expression."""

    def visit_col(self, name: str) -> dict[str, Any]:
        return {}

    def visit_lit(self, value: Any) -> dict[str, Any]:
        return {}

    def visit_alias(self, expr: Expression, alias: str) -> dict[str, Any]:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: Any) -> dict[str, Any]:
        return {}

    def visit_coalesce(self, args: list[Expression]) -> dict[str, Any]:
        return {}

    def visit_function(self, name: str, args: list[Expression]) -> dict[str, Any]:
        if name == "and" and len(args) == 2:
            return {**self.visit(args[0]), **self.visit(args[1])}
        if name == "equal" and len(args) == 2:
            left, right = args
            # A missing column name does not imply a literal: the other side
            # may be an expression such as `col("id") + 2023`.
            if (column := left.column_name()) is not None and right.is_literal():
                return {column: right.as_py()}
            if (column := right.column_name()) is not None and left.is_literal():
                return {column: left.as_py()}
        return {}


def test_equality_constraints_visitor():
    constraints = EqualityConstraints()
    assert constraints.visit(col("year") == 2024) == {"year": 2024}
    assert constraints.visit((col("year") == 2024) & (col("kind") == "x")) == {"year": 2024, "kind": "x"}
    # Disjunctions cannot be treated as conjunctive constraints.
    assert constraints.visit((col("year") == 2024) | (col("year") == 2023)) == {}
    assert constraints.visit(col("year") > 2020) == {}
    # Equality against a computed expression is not a literal constraint.
    assert constraints.visit(col("year") == (col("id") + 2023)) == {}
    assert constraints.visit((col("id") + 2023) == col("year")) == {}


class InMemoryTableTask(DataSourceTask):
    def __init__(self, table: pa.Table, columns: list[str] | None, limit: int | None):
        self._table = table
        self._columns = columns
        self._limit = limit

    @property
    def schema(self) -> Schema:
        table = self._table if self._columns is None else self._table.select(self._columns)
        return Schema.from_pyarrow_schema(table.schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        table = self._table
        if self._columns is not None:
            table = table.select(self._columns)
        if self._limit is not None:
            table = table.slice(0, self._limit)
        yield RecordBatch.from_arrow_table(table)


class InMemoryTableSource(DataSource):
    """A source over pyarrow tables keyed by a `year` column."""

    def __init__(self, parts: dict[int, pa.Table]):
        self._parts = parts
        self.read_years: list[int] = []

    @property
    def name(self) -> str:
        return "in-memory table source"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(next(iter(self._parts.values())).schema)

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        columns = pushdowns.columns
        if columns is not None:
            # Keep columns that the filter needs even if the query does not return them.
            columns = list(dict.fromkeys([*columns, *pushdowns.filter_required_column_names()]))
        constraints = EqualityConstraints().visit(pushdowns.filters) if pushdowns.filters is not None else {}
        # Only truncate when the source applies every filter itself. Otherwise
        # slicing before Daft filters the rows could discard matching rows.
        limit = pushdowns.limit if pushdowns.filters is None else None
        for year, table in self._parts.items():
            # Only skip a part when the filter proves its `year` cannot match.
            if constraints.get("year", year) != year:
                continue
            self.read_years.append(year)
            yield InMemoryTableTask(table, columns, limit)


def _parts() -> dict[int, pa.Table]:
    return {
        2023: pa.table({"year": [2023] * 100, "id": list(range(100))}),
        2024: pa.table({"year": [2024] * 100, "id": list(range(100, 200))}),
    }


def test_filter_pushdown_prunes_tasks():
    source = InMemoryTableSource(_parts())
    df = source.read().where(col("year") == 2024).select("id")

    assert df.to_pydict() == {"id": list(range(100, 200))}
    assert source.read_years == [2024]


def test_filter_pushdown_keeps_filter_columns_out_of_projection():
    source = InMemoryTableSource(_parts())
    df = source.read().where(col("year") == 2024).select("id").limit(3)

    assert df.to_pydict() == {"id": [100, 101, 102]}


def test_limit_is_not_applied_before_filter():
    source = InMemoryTableSource(_parts())
    df = source.read().where(col("id") > 150).select("id").limit(3)

    # The source only prunes parts, so it must not truncate rows before Daft
    # applies the filter: the first rows of the 2024 part do not match.
    assert df.to_pydict() == {"id": [151, 152, 153]}


def test_no_filter_reads_all_tasks():
    source = InMemoryTableSource(_parts())
    df = source.read().select("id")

    assert df.to_pydict() == {"id": list(range(200))}
    assert source.read_years == [2023, 2024]


def test_unrecognized_filter_does_not_prune():
    source = InMemoryTableSource(_parts())
    df = source.read().where(col("year") > 2023)

    assert df.to_pydict() == {"year": [2024] * 100, "id": list(range(100, 200))}
    assert source.read_years == [2023, 2024]
