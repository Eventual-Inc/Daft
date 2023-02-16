from __future__ import annotations

from daft.daft import PyExpr as _PyExpr
from daft.daft import col as _col
from daft.daft import lit as _lit
from daft.datatype import DataType


def lit(value: object) -> Expression:
    return Expression._from_pyexpr(_lit(value))


def col(name: str) -> Expression:
    return Expression._from_pyexpr(_col(name))


class Expression:
    _expr: _PyExpr

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Expression via __init__ ")

    @staticmethod
    def _from_pyexpr(pyexpr) -> Expression:
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
