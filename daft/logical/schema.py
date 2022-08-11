from __future__ import annotations

import copy
from typing import Dict, Iterable, Iterator, List, Optional, Set, cast

from daft.expressions import ColID, ColumnExpression, Expression


class ExpressionList(Iterable[Expression]):
    def __init__(self, exprs: List[Expression]) -> None:
        self.exprs = copy.deepcopy(exprs)
        self.names: List[str] = []
        name_set = set()
        for i, e in enumerate(exprs):
            e_name = e.name()
            if e_name is None:
                e_name = "col_{i}"
            if e_name in name_set:
                raise ValueError(f"duplicate name found {e_name}")
            self.names.append(e_name)
            name_set.add(e_name)

    def contains(self, column: ColumnExpression) -> bool:
        for e in self.exprs:
            if e.is_same(column):
                return True
        return False

    def get_expression_by_name(self, name: str) -> Optional[Expression]:
        for i, n in enumerate(self.names):
            if n == name:
                return self.exprs[i]
        return None

    def get_expression_by_id(self, id: int) -> Optional[Expression]:
        for e in self.exprs:
            if e.get_id() == id:
                return e
        return None

    def resolved(self) -> bool:
        return all([e.resolved() for e in self.exprs])

    def resolve(self, input_schema: ExpressionList) -> ExpressionList:
        for e in self.exprs:
            for col_expr in e.required_columns(unresolved_only=True):
                col_expr_name = col_expr.name()
                assert col_expr_name is not None
                match_output_expr = input_schema.get_expression_by_name(col_expr_name)
                if match_output_expr is None:
                    raise ValueError(f"Could not find expr by name {col_expr_name}")
                col_expr.assign_id_from_expression(match_output_expr)
            if not e.resolved():
                import pdb

                pdb.set_trace()
            assert e.resolved()
        return self

    def keep(self, to_keep: List[str]) -> ExpressionList:
        # is_resolved = True
        exprs_to_keep: List[Expression] = []
        for name in to_keep:
            expr = self.get_expression_by_name(name)
            if expr is None:
                raise ValueError(f"{name} not found in {self.names}")
            exprs_to_keep.append(expr)
        return ExpressionList(exprs_to_keep)

    def __repr__(self) -> str:
        return repr(self.exprs)

    def union(self, other: ExpressionList, strict: bool = True) -> ExpressionList:
        deduped = self.exprs.copy()
        seen: Dict[str, Expression] = {}
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
                if strict:
                    raise ValueError(
                        f"Duplicate name found with different expression. name: {name}, seen: {seen[name]}, current: {e}"
                    )
                else:
                    name = f"right.{name}"
                    e = cast(Expression, e.alias(name))
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
        name_to_expr: Dict[str, ColumnExpression] = {}
        for e in self.exprs:
            for c in e.required_columns():
                name = c.name()
                assert name is not None
                name_to_expr[name] = c
        return ExpressionList([e for e in name_to_expr.values()])

    def __iter__(self) -> Iterator[Expression]:
        return iter(self.exprs)

    def to_id_set(self) -> Set[ColID]:
        id_set = set()
        for c in self:
            id = c.get_id()
            assert id is not None
            id_set.add(id)
        return id_set
