from __future__ import annotations

import copy
from typing import Iterable, Iterator, TypeVar, cast

from daft.expressions import ColID, ColumnExpression, Expression
from daft.resource_request import ResourceRequest

ExpressionType = TypeVar("ExpressionType", bound=Expression)


class ExpressionList(Iterable[ExpressionType]):
    def __init__(self, exprs: list[ExpressionType]) -> None:
        self.exprs = copy.deepcopy(exprs)
        self.names: list[str] = []
        name_set = set()
        for i, e in enumerate(exprs):
            e_name = e.name()
            if e_name is None:
                e_name = "col_{i}"
            if e_name in name_set:
                raise ValueError(f"duplicate name found {e_name}")
            self.names.append(e_name)
            name_set.add(e_name)
        # self.is_resolved = all(e.required_columns(unresolved_only=True) == [] for e in exprs)

    def __len__(self) -> int:
        return len(self.exprs)

    def contains(self, column: ColumnExpression) -> bool:
        for e in self.exprs:
            if e.is_same(column):
                return True
        return False

    def get_expression_by_name(self, name: str) -> ExpressionType | None:
        for i, n in enumerate(self.names):
            if n == name:
                return self.exprs[i]
        return None

    def get_expression_by_id(self, id: int) -> ExpressionType | None:
        for e in self.exprs:
            if e.get_id() == id:
                return e
        return None

    def resolve(self, input_schema: ExpressionList | None = None) -> ExpressionList:
        if input_schema is None:
            for e in self.exprs:
                assert isinstance(e, ColumnExpression), "we can only resolve ColumnExpression without an input_schema"
                e._assign_id(strict=False)
        else:
            for e in self.exprs:
                for col_expr in e.required_columns(unresolved_only=True):
                    col_expr_name = col_expr.name()
                    assert col_expr_name is not None
                    match_output_expr = input_schema.get_expression_by_name(col_expr_name)
                    if match_output_expr is not None:
                        col_expr.resolve_to_expression(match_output_expr)
                    else:
                        raise ValueError(f"Could not find expr by name {col_expr_name}")
                # Validate that types are able to be resolved, or throw a TypeError if not
                e.resolved_type()
        for e in self.exprs:
            e._assign_id(strict=False)
        # self.is_resolved = True
        return self

    def unresolve(self) -> ExpressionList:
        return ExpressionList([e._unresolve() for e in self.exprs])

    def keep(self, to_keep: list[str]) -> ExpressionList:
        # is_resolved = True
        exprs_to_keep: list[Expression] = []
        for name in to_keep:
            expr = self.get_expression_by_name(name)
            if expr is None:
                raise ValueError(f"{name} not found in {self.names}")
            exprs_to_keep.append(expr)
        return ExpressionList(exprs_to_keep)

    def __repr__(self) -> str:
        return repr(self.exprs)

    def union(
        self, other: ExpressionList, rename_dup: str | None = None, other_override: bool = False
    ) -> ExpressionList:
        """Unions two schemas together

        Note: only one of rename_dup or other_override can be specified as the behavior when resolving naming conflicts.

        Args:
            other (ExpressionList): other ExpressionList to union with this one
            rename_dup (Optional[str], optional): when conflicts in naming happen, append this string to the conflicting column in `other`. Defaults to None.
            other_override (bool, optional): when conflicts in naming happen, use the `other` column instead of `self`. Defaults to False.
        """
        assert not ((rename_dup is not None) and other_override), "Only can specify one of rename_dup or other_override"
        deduped = self.exprs.copy()
        seen: dict[str, ExpressionType] = {}
        for e in self.exprs:
            name = e.name()
            if name is not None:
                seen[name] = e

        for e in other:
            name = e.name()
            if name is None:
                deduped.append(e)
                continue

            if name in seen:
                if seen[name].is_eq(e):
                    continue
                if rename_dup is not None:
                    name = f"{rename_dup}{name}"
                    e = cast(ExpressionType, e.alias(name))
                elif other_override:
                    # Allow this expression in `other` to override the existing expressions
                    deduped = [current_expr for current_expr in deduped if current_expr.name() != name]
                else:
                    raise ValueError(
                        f"Duplicate name found with different expression. name: {name}, seen: {seen[name]}, current: {e}"
                    )
            deduped.append(e)
            seen[name] = e

        return ExpressionList(deduped)

    def to_column_expressions(self) -> ExpressionList:
        return ExpressionList([e.to_column_expression() for e in self.exprs])

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExpressionList):
            return False

        return len(self.exprs) == len(other.exprs) and all(s.is_eq(o) for s, o in zip(self.exprs, other.exprs))

    def required_columns(self) -> ExpressionList:
        name_to_expr: dict[str, ColumnExpression] = {}
        for e in self.exprs:
            for c in e.required_columns():
                name = c.name()
                assert name is not None
                name_to_expr[name] = c
        return ExpressionList([e for e in name_to_expr.values()])

    def __iter__(self) -> Iterator[ExpressionType]:
        return iter(self.exprs)

    def to_id_set(self) -> set[ColID]:
        id_set = set()
        for c in self:
            id = c.get_id()
            assert id is not None
            id_set.add(id)
        return id_set

    def resource_request(self) -> ResourceRequest:
        """Returns the requested resources for the execution of all expressions in this list"""
        return ResourceRequest.max_resources([e.resource_request() for e in self.exprs])
