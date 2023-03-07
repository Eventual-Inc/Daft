from __future__ import annotations

from typing import Iterable, Iterator, overload

from daft.daft import PyExpr as _PyExpr
from daft.daft import col as _col
from daft.daft import lit as _lit
from daft.datatype import DataType
from daft.expressions2.testing import expr_structurally_equal


def lit(value: object) -> Expression:
    return Expression._from_pyexpr(_lit(value))


def col(name: str) -> Expression:
    return Expression._from_pyexpr(_col(name))


class Expression:
    _expr: _PyExpr

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Expression via __init__ ")

    @property
    def agg(self) -> ExpressionAggNamespace:
        ns = ExpressionAggNamespace.__new__(ExpressionAggNamespace)
        ns._expr = self._expr
        return ns

    @staticmethod
    def _from_pyexpr(pyexpr: _PyExpr) -> Expression:
        expr = Expression.__new__(Expression)
        expr._expr = pyexpr
        return expr

    @staticmethod
    def _to_expression(obj: object) -> Expression:
        if isinstance(obj, Expression):
            return obj
        else:
            return lit(obj)

    def __bool__(self) -> bool:
        raise ValueError(
            "Expressions don't have a truth value until executed. "
            "If you reached this error using `and` / `or`, use `&` / `|` instead."
        )

    def __add__(self, other: object) -> Expression:
        """Adds two numeric expressions (``e1 + e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr + expr._expr)

    def __radd__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr + self._expr)

    def __sub__(self, other: object) -> Expression:
        """Subtracts two numeric expressions (``e1 - e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr - expr._expr)

    def __rsub__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr - self._expr)

    def __mul__(self, other: object) -> Expression:
        """Multiplies two numeric expressions (``e1 * e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr * expr._expr)

    def __rmul__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr * self._expr)

    def __truediv__(self, other: object) -> Expression:
        """True divides two numeric expressions (``e1 / e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr / expr._expr)

    def __rtruediv__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr / self._expr)

    def __mod__(self, other: Expression) -> Expression:
        """Takes the mod of two numeric expressions (``e1 % e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr % expr._expr)

    def __rmod__(self, other: Expression) -> Expression:
        """Takes the mod of two numeric expressions (``e1 % e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr % self._expr)

    def __and__(self, other: Expression) -> Expression:
        """Takes the logical AND of two expressions (``e1 & e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr & expr._expr)

    def __rand__(self, other: Expression) -> Expression:
        """Takes the logical reverse AND of two expressions (``e1 & e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr & self._expr)

    def __or__(self, other: Expression) -> Expression:
        """Takes the logical OR of two expressions (``e1 | e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr | expr._expr)

    def __ror__(self, other: Expression) -> Expression:
        """Takes the logical reverse OR of two expressions (``e1 | e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr | self._expr)

    def __lt__(self, other: Expression) -> Expression:
        """Compares if an expression is less than another (``e1 < e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr < expr._expr)

    def __le__(self, other: Expression) -> Expression:
        """Compares if an expression is less than or equal to another (``e1 <= e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr <= expr._expr)

    def __eq__(self, other: Expression) -> Expression:  # type: ignore
        """Compares if an expression is equal to another (``e1 == e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr == expr._expr)

    def __ne__(self, other: Expression) -> Expression:  # type: ignore
        """Compares if an expression is not equal to another (``e1 != e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr != expr._expr)

    def __gt__(self, other: Expression) -> Expression:
        """Compares if an expression is greater than another (``e1 > e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr > expr._expr)

    def __ge__(self, other: Expression) -> Expression:
        """Compares if an expression is greater than or equal to another (``e1 >= e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr >= expr._expr)

    def alias(self, name: str) -> Expression:
        assert isinstance(name, str)
        expr = self._expr.alias(name)
        return Expression._from_pyexpr(expr)

    def cast(self, dtype: DataType) -> Expression:
        assert isinstance(dtype, DataType)
        expr = self._expr.cast(dtype._dtype)
        return Expression._from_pyexpr(expr)

    def name(self) -> str:
        return self._expr.name()

    def __repr__(self) -> str:
        return repr(self._expr)

    def _input_mapping(self) -> str | None:
        raise NotImplementedError(
            "[RUST-INT] Implement for checking if expression is a no-op and returning the input name it maps to if so"
        )

    def _required_columns(self) -> set[str]:
        raise NotImplementedError("[RUST-INT] Implement for getting required columns in an Expression")

    def _is_column(self) -> bool:
        raise NotImplementedError("[RUST-INT] Implement for checking if this Expression is a Column")

    def _replace_column_with_expression(self, column: str, new_expr: Expression) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement replacing a Column with an Expression - used in optimizer")


class ExpressionNamespace:
    _expr: _PyExpr

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a ExpressionNamespace via __init__ ")

    @staticmethod
    def _from_pyexpr(pyexpr: _PyExpr) -> ExpressionNamespace:
        expr = ExpressionNamespace.__new__(ExpressionNamespace)
        expr._expr = pyexpr
        return expr


class ExpressionAggNamespace(ExpressionNamespace):
    def sum(self) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement expression aggregation")

    def mean(self) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement expression aggregation")

    def min(self) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement expression aggregation")

    def max(self) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement expression aggregation")

    def count(self) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement expression aggregation")

    def list(self) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement expression aggregation")

    def concat(self) -> Expression:
        raise NotImplementedError("[RUST-INT] Implement expression aggregation")


class ExpressionsProjection(Iterable[Expression]):
    """A collection of Expressions that can be projected onto a Table to produce another Table

    Invariants:
        1. All Expressions have names
        2. All Expressions have unique names
    """

    def __init__(self, exprs: list[Expression]) -> None:
        # Check invariants
        seen: set[str] = set()
        for e in exprs:
            if e.name() in seen:
                raise ValueError("Expressions must all have unique names")
            seen.add(e.name())

        self._output_name_to_exprs = {e.name(): e for e in exprs}

    def __len__(self) -> int:
        return len(self._output_name_to_exprs)

    def __iter__(self) -> Iterator[Expression]:
        return iter(self._output_name_to_exprs.values())

    @overload
    def __getitem__(self, idx: slice) -> list[Expression]:
        ...

    @overload
    def __getitem__(self, idx: int) -> Expression:
        ...

    def __getitem__(self, idx: int | slice) -> Expression | list[Expression]:
        # Relies on the fact that Python dictionaries are ordered
        return list(self._output_name_to_exprs.values())[idx]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExpressionsProjection):
            return False

        return len(self._output_name_to_exprs) == len(other._output_name_to_exprs) and all(
            (s.name() == o.name()) and expr_structurally_equal(s, o)
            for s, o in zip(self._output_name_to_exprs.values(), other._output_name_to_exprs.values())
        )

    def required_columns(self) -> set[str]:
        """Column names required to run this ExpressionsProjection"""
        result: set[str] = set()
        for e in self._output_name_to_exprs.values():
            result |= e._required_columns()
        return result

    def union(self, other: ExpressionsProjection, rename_dup: str | None = None) -> ExpressionsProjection:
        """Unions two Expressions. Output naming conflicts are handled with keyword arguments.

        Args:
            other (ExpressionList): other ExpressionList to union with this one
            rename_dup (Optional[str], optional): when conflicts in naming happen, append this string to the conflicting column in `other`. Defaults to None.
        """
        unioned: dict[str, Expression] = {}
        for expr in list(self) + list(other):
            name = expr.name()

            # Handle naming conflicts
            if name in unioned:
                if rename_dup is not None:
                    while name in unioned:
                        name = f"{rename_dup}{name}"
                    expr = expr.alias(name)
                else:
                    raise ValueError(
                        f"Duplicate name found with different expression. name: {name}, seen: {unioned[name]}, current: {expr}"
                    )

            unioned[name] = expr
        return ExpressionsProjection(list(unioned.values()))

    def to_name_set(self) -> set[str]:
        return {e.name() for e in self}

    def input_mapping(self) -> dict[str, str]:
        """Returns a map of {output_name: input_name} for all expressions that are just no-ops/aliases of an existing input"""
        result = {}
        for e in self:
            input_map = e._input_mapping()
            if input_map is not None:
                result[e.name()] = input_map
        return result

    def to_column_expressions(self) -> ExpressionsProjection:
        return ExpressionsProjection([col(e.name()) for e in self])

    def get_expression_by_name(self, name: str) -> Expression:
        if name not in self._output_name_to_exprs:
            raise ValueError(f"{name} not found in ExpressionsProjection")
        return self._output_name_to_exprs[name]
