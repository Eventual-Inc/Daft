from __future__ import annotations

import builtins
import functools
import inspect
from datetime import date
from typing import Any, Callable, Iterable, Iterator, TypeVar, overload

from daft.daft import PyExpr as _PyExpr
from daft.daft import col as _col
from daft.daft import lit as _lit
from daft.daft import udf as _udf
from daft.datatype import DataType
from daft.expressions.testing import expr_structurally_equal
from daft.logical.schema import Field, Schema
from daft.series import Series


def lit(value: object) -> Expression:
    if isinstance(value, date):
        epoch_time = value - date(1970, 1, 1)
        return lit(epoch_time.days).cast(DataType.date())
    else:
        lit_value = _lit(value)
    return Expression._from_pyexpr(lit_value)


def col(name: str) -> Expression:
    return Expression._from_pyexpr(_col(name))


def _apply_partial(
    func,
    pyvalues: dict[str, Any],
    expression_name_to_index: dict[str, int],
    arg_kwarg_keys: tuple[list[str], list[str]],
):
    @functools.wraps(func)
    def partial_func(computed_series: list[Series]):
        arg_keys, kwarg_keys = arg_kwarg_keys

        for name in arg_keys + kwarg_keys:
            assert name in expression_name_to_index, f"Expression for function parameter `{name}` not found"
            assert expression_name_to_index[name] < len(
                computed_series
            ), f"Index {expression_name_to_index[name]} for function parameter `{name}` is out of range"

        args = tuple(pyvalues.get(name, computed_series[expression_name_to_index[name]]) for name in arg_keys)
        kwargs = {name: pyvalues.get(name, computed_series[expression_name_to_index[name]]) for name in kwarg_keys}
        return func(*args, **kwargs)

    return partial_func


class Expression:
    _expr: _PyExpr

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Expression via __init__ ")

    @property
    def str(self) -> ExpressionStringNamespace:
        return ExpressionStringNamespace.from_expression(self)

    @property
    def dt(self) -> ExpressionDatetimeNamespace:
        return ExpressionDatetimeNamespace.from_expression(self)

    @property
    def float(self) -> ExpressionFloatNamespace:
        return ExpressionFloatNamespace.from_expression(self)

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

    @staticmethod
    def udf(func: Callable, args: tuple, kwargs: dict) -> Expression:
        """Creates a new UDF Expression

        Args:
            func: User-provided Python function to run
            expr_inputs: List of argument names in the function that should be Expressions
            args: Non-named arguments (potentially Expressions) to the function
            kwargs: Named arguments (potentially Expressions) to the function
        """
        bound_args = inspect.signature(func).bind(*args, **kwargs)
        bound_args.apply_defaults()
        kwarg_keys = list(bound_args.kwargs.keys())
        arg_keys = list(bound_args.arguments.keys() - bound_args.kwargs.keys())

        expressions = {}
        pyvalues = {}
        for key, val in bound_args.arguments.items():
            if isinstance(val, Expression):
                expressions[key] = val
            else:
                pyvalues[key] = val

        expression_items = [(name, e) for name, e in expressions.items()]
        expression_name_to_index = {name: i for i, (name, _) in enumerate(expression_items)}
        oredered_expressions = [e._expr for _, e in expression_items]

        curried_function = _apply_partial(func, pyvalues, expression_name_to_index, (arg_keys, kwarg_keys))
        return Expression._from_pyexpr(_udf(curried_function, oredered_expressions))

    def __bool__(self) -> bool:
        raise ValueError(
            "Expressions don't have a truth value. "
            "If you used Python keywords `and` `not` `or` on an expression, use `&` `~` `|` instead."
        )

    def __abs__(self) -> Expression:
        "Absolute value of expression"
        return self.abs()

    def abs(self) -> Expression:
        "Absolute value of expression"
        return Expression._from_pyexpr(abs(self._expr))

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

    def __invert__(self) -> Expression:
        """Inverts a bool expression (``~e``)"""
        expr = self._expr.__invert__()
        return Expression._from_pyexpr(expr)

    def alias(self, name: builtins.str) -> Expression:
        assert isinstance(name, str)
        expr = self._expr.alias(name)
        return Expression._from_pyexpr(expr)

    def cast(self, dtype: DataType) -> Expression:
        assert isinstance(dtype, DataType)
        expr = self._expr.cast(dtype._dtype)
        return Expression._from_pyexpr(expr)

    def _count(self) -> Expression:
        expr = self._expr.count()
        return Expression._from_pyexpr(expr)

    def _sum(self) -> Expression:
        expr = self._expr.sum()
        return Expression._from_pyexpr(expr)

    def _mean(self) -> Expression:
        expr = self._expr.mean()
        return Expression._from_pyexpr(expr)

    def _min(self) -> Expression:
        expr = self._expr.min()
        return Expression._from_pyexpr(expr)

    def _max(self) -> Expression:
        expr = self._expr.max()
        return Expression._from_pyexpr(expr)

    def if_else(self, if_true: Expression, if_false: Expression) -> Expression:
        if_true = Expression._to_expression(if_true)
        if_false = Expression._to_expression(if_false)
        return Expression._from_pyexpr(self._expr.if_else(if_true._expr, if_false._expr))

    def apply(self, func: Callable, return_dtype: DataType | None = None) -> Expression:
        raise NotImplementedError("[RUST-INT][UDF] Implement .apply")

    def is_null(self) -> Expression:
        expr = self._expr.is_null()
        return Expression._from_pyexpr(expr)

    def name(self) -> builtins.str:
        return self._expr.name()

    def __repr__(self) -> builtins.str:
        return repr(self._expr)

    def _to_field(self, schema: Schema) -> Field:
        return Field._from_pyfield(self._expr.to_field(schema._schema))

    def __getstate__(self) -> bytes:
        return self._expr.__getstate__()

    def __setstate__(self, state: bytes) -> None:
        self._expr = _PyExpr.__new__(_PyExpr)
        self._expr.__setstate__(state)

    ###
    # Helper methods required by optimizer:
    # These should be removed from the Python API for Expressions when logical plans and optimizer are migrated to Rust
    ###

    def _input_mapping(self) -> builtins.str | None:
        return self._expr._input_mapping()

    def _required_columns(self) -> set[builtins.str]:
        return self._expr._required_columns()

    def _is_column(self) -> bool:
        return self._expr._is_column()

    def _replace_column_with_expression(self, column: builtins.str, new_expr: Expression) -> Expression:
        return Expression._from_pyexpr(self._expr._replace_column_with_expression(column, new_expr._expr))


SomeExpressionNamespace = TypeVar("SomeExpressionNamespace", bound="ExpressionNamespace")


class ExpressionNamespace:
    _expr: _PyExpr

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a ExpressionNamespace via __init__ ")

    @classmethod
    def from_expression(cls: type[SomeExpressionNamespace], expr: Expression) -> SomeExpressionNamespace:
        ns = cls.__new__(cls)
        ns._expr = expr._expr
        return ns


class ExpressionFloatNamespace(ExpressionNamespace):
    def is_nan(self) -> Expression:
        return Expression._from_pyexpr(self._expr.is_nan())


class ExpressionDatetimeNamespace(ExpressionNamespace):
    def day(self) -> Expression:
        return Expression._from_pyexpr(self._expr.dt_day())

    def month(self) -> Expression:
        return Expression._from_pyexpr(self._expr.dt_month())

    def year(self) -> Expression:
        return Expression._from_pyexpr(self._expr.dt_year())

    def day_of_week(self) -> Expression:
        return Expression._from_pyexpr(self._expr.dt_day_of_week())


class ExpressionStringNamespace(ExpressionNamespace):
    def contains(self, substr: str) -> Expression:
        substr_expr = Expression._to_expression(substr)
        return Expression._from_pyexpr(self._expr.utf8_contains(substr_expr._expr))

    def endswith(self, suffix: str | Expression) -> Expression:
        suffix_expr = Expression._to_expression(suffix)
        return Expression._from_pyexpr(self._expr.utf8_endswith(suffix_expr._expr))

    def startswith(self, prefix: str) -> Expression:
        prefix_expr = Expression._to_expression(prefix)
        return Expression._from_pyexpr(self._expr.utf8_startswith(prefix_expr._expr))

    def concat(self, other: str) -> Expression:
        # Delegate to + operator implementation.
        return Expression._from_pyexpr(self._expr) + other

    def length(self) -> Expression:
        return Expression._from_pyexpr(self._expr.utf8_length())


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
                raise ValueError(f"Expressions must all have unique names; saw {e.name()} twice")
            seen.add(e.name())

        self._output_name_to_exprs = {e.name(): e for e in exprs}

    @classmethod
    def from_schema(cls, schema: Schema) -> ExpressionsProjection:
        return cls([col(field.name) for field in schema])

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
            other (ExpressionsProjection): other ExpressionsProjection to union with this one
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

    def resolve_schema(self, schema: Schema) -> Schema:
        fields = [e._to_field(schema) for e in self]
        return Schema._from_field_name_and_types([(f.name, f.dtype) for f in fields])
