from __future__ import annotations

import datetime
import itertools
import warnings
from abc import abstractmethod
from copy import deepcopy
from functools import partial, partialmethod
from typing import (
    Any,
    Callable,
    NewType,
    Sequence,
    Tuple,
    TypeVar,
    cast,
    get_type_hints,
)

import numpy as np
import pandas as pd

_POLARS_AVAILABLE = True
try:
    import polars as pl
except ImportError:
    _POLARS_AVAILABLE = False

import pyarrow as pa

from daft.errors import ExpressionTypeError
from daft.execution import url_operators
from daft.execution.operators import ExpressionOperator, OperatorEnum
from daft.internal.treenode import TreeNode
from daft.resource_request import ResourceRequest
from daft.runners.blocks import (
    ArrowDataBlock,
    DataBlock,
    PyListDataBlock,
    zip_blocks_as_py,
)
from daft.types import ExpressionType, PrimitiveExpressionType


def col(name: str) -> ColumnExpression:
    """Selects a column with a given name

    Example:
        >>> col("x")

    Args:
        name: Name of column

    Returns:
        ColumnExpression: Expression representing the selected column
    """
    return ColumnExpression(name)


def lit(val: Any) -> LiteralExpression:
    """Selects a column with every item being the provided value

    Example:
        >>> col("x") + lit(1)

    Args:
        val: value of column

    Returns:
        LiteralExpression: Expression representing the value provided
    """
    return LiteralExpression(val)


_COUNTER = itertools.count()
ColID = NewType("ColID", int)


class ExpressionExecutor:
    def eval(self, expr: Expression, operands: dict[str, Any]) -> DataBlock:
        result: DataBlock
        if isinstance(expr, ColumnExpression):
            name = expr.name()
            assert name is not None
            result = operands[name]
            return result
        elif isinstance(expr, LiteralExpression):
            result = expr._value
            return DataBlock.make_block(result)
        elif isinstance(expr, AliasExpression):
            result = self.eval(expr._expr, operands)
            return result
        elif isinstance(expr, CallExpression):
            eval_args: tuple[DataBlock, ...] = tuple(self.eval(a, operands) for a in expr._args)

            # Use a PyListDataBlock evaluator if any of the args are Python types
            op_evaluator = (
                PyListDataBlock.evaluator
                if any(isinstance(block, PyListDataBlock) for block in eval_args)
                else ArrowDataBlock.evaluator
            )
            op = expr._operator

            func = getattr(op_evaluator, op.name)
            result = func(*eval_args)
            return result
        elif isinstance(expr, UdfExpression):
            eval_args = tuple(self.eval(a, operands) for a in expr._args)
            eval_kwargs = {kw: self.eval(a, operands) for kw, a in expr._kwargs.items()}

            results = expr._func(*eval_args, **eval_kwargs)

            if ExpressionType.is_py(expr._func_ret_type):
                return PyListDataBlock(results if isinstance(results, list) else list(results))

            # Convert these user-provided types to pa.ChunkedArray for making blocks
            if isinstance(results, pa.Array):
                results = pa.chunked_array([results])
            elif _POLARS_AVAILABLE and isinstance(results, pl.Series):
                results = pa.chunked_array([results.to_arrow()])
            elif isinstance(results, np.ndarray):
                results = pa.chunked_array([pa.array(results)])
            elif isinstance(results, pd.Series):
                results = pa.chunked_array([pa.array(results)])
            elif isinstance(results, list):
                results = pa.chunked_array([pa.array(results)])

            if not isinstance(results, pa.ChunkedArray):
                raise NotImplementedError(f"Cannot make block for data of type: {type(results)}")

            # Explcitly cast results of the UDF here for these primitive types:
            #   1. Ensures that all blocks across all partitions will have the same underlying Arrow type after the UDF
            #   2. Ensures that any null blocks from empty partitions or partitions with all nulls have the correct type
            assert isinstance(expr._func_ret_type, PrimitiveExpressionType)
            expected_arrow_type = expr._func_ret_type.to_arrow_type()
            results = results.cast(expected_arrow_type)

            return ArrowDataBlock(results)
        elif isinstance(expr, AsPyExpression):
            raise NotImplementedError("AsPyExpressions need to be evaluated with a method call")
        else:
            raise NotImplementedError(f"Not implemented for expression type {type(expr)}: {expr}")


class Expression(TreeNode["Expression"]):
    def __init__(self) -> None:
        super().__init__()
        self._id: ColID | None = None

    def __repr__(self) -> str:
        if self.has_id():
            return f"{self._display_str()} AS {self.name()}#{self.get_id()}"
        else:
            return self._display_str()

    def _to_expression(self, input: Any) -> Expression:
        if not isinstance(input, Expression):
            return LiteralExpression(input)
        return input

    def __bool__(self) -> bool:
        raise ValueError(
            "Expressions don't have a truth value until executed. "
            "If you reached this error using `and` / `or`, use `&` / `|` instead."
        )

    def _unary_op(
        self,
        operator: OperatorEnum,
    ) -> Expression:
        return CallExpression(operator, func_args=(self,))

    def _binary_op(
        self,
        operator: OperatorEnum,
        other: Any,
    ) -> Expression:
        other_expr = self._to_expression(other)
        return CallExpression(operator, func_args=(self, other_expr))

    def _reverse_binary_op(
        self,
        operator: OperatorEnum,
        other: Any,
    ) -> Expression:
        other_expr = self._to_expression(other)
        return other_expr._binary_op(operator, self)

    def _self_resource_request(self) -> ResourceRequest:
        """Returns the ResourceRequest required by this specific Expression (not including its children)"""
        return ResourceRequest.default()

    def resource_request(self) -> ResourceRequest:
        """Returns the maximum ResourceRequest that is required by this Expression and all its children"""
        return ResourceRequest.max_resources(
            [e.resource_request() for e in self._children()] + [self._self_resource_request()]
        )

    @abstractmethod
    def resolved_type(self) -> ExpressionType | None:
        """Expressions have a resolved_type only if they are resolved"""
        return None

    def name(self) -> str | None:
        for child in self._children():
            name = child.name()
            if name is not None:
                return name
        return None

    def _assign_id(self, strict: bool = True) -> ColID:
        if not self.has_id():
            self._id = ColID(next(_COUNTER))
            return self._id
        else:
            if strict:
                raise ValueError(f"We have already assigned an id, {self.get_id()}")
            else:
                assert self._id is not None
                return self._id

    def get_id(self) -> ColID | None:
        return self._id

    def has_id(self) -> bool:
        return self.get_id() is not None

    def to_column_expression(self) -> ColumnExpression:
        if not self.has_id():
            raise ValueError("we can only convert expressions with assigned id to ColumnExpressions")
        name = self.name()
        if name is None:
            raise ValueError("we can only convert expressions to ColumnExpressions if they have a name")
        ce = ColumnExpression(name)
        ce.resolve_to_expression(self)
        return ce

    def required_columns(self, unresolved_only: bool = False) -> list[ColumnExpression]:
        to_rtn: list[ColumnExpression] = []
        for child in self._children():
            to_rtn.extend(child.required_columns(unresolved_only))
        return to_rtn

    def _replace_column_with_expression(self, col_expr: ColumnExpression, new_expr: Expression) -> Expression:
        assert col_expr.is_same(new_expr)
        if isinstance(self, ColumnExpression) and self.is_eq(col_expr):
            return new_expr
        for i in range(len(self._children())):
            self._registered_children[i] = self._registered_children[i]._replace_column_with_expression(
                col_expr, new_expr
            )
        return self

    def _unresolve(self) -> Expression:
        expr = deepcopy(self)

        def helper(e: Expression) -> None:
            e._id = None
            for child in e._children():
                helper(child)

        helper(expr)
        return expr

    # UnaryOps

    # Arithmetic
    __neg__ = partialmethod(_unary_op, OperatorEnum.NEGATE)
    __pos__ = partialmethod(_unary_op, OperatorEnum.POSITIVE)
    __abs__ = partialmethod(_unary_op, OperatorEnum.ABS)

    # Symbolic - not used during evaluation but useful for type resolution on the Expression
    _sum = partialmethod(_unary_op, OperatorEnum.SUM)
    _count = partialmethod(_unary_op, OperatorEnum.COUNT)
    _mean = partialmethod(_unary_op, OperatorEnum.MEAN)
    _list = partialmethod(_unary_op, OperatorEnum.LIST)
    _concat = partialmethod(_unary_op, OperatorEnum.CONCAT)
    _min = partialmethod(_unary_op, OperatorEnum.MIN)
    _max = partialmethod(_unary_op, OperatorEnum.MAX)
    _explode = partialmethod(_unary_op, OperatorEnum.EXPLODE)

    # Logical
    __invert__ = partialmethod(_unary_op, OperatorEnum.INVERT)

    # BinaryOps

    # Arithmetic
    __add__ = partialmethod(_binary_op, OperatorEnum.ADD)
    __sub__ = partialmethod(_binary_op, OperatorEnum.SUB)
    __mul__ = partialmethod(_binary_op, OperatorEnum.MUL)
    __floordiv__ = partialmethod(_binary_op, OperatorEnum.FLOORDIV)
    __truediv__ = partialmethod(_binary_op, OperatorEnum.TRUEDIV)
    __pow__ = partialmethod(_binary_op, OperatorEnum.POW)
    __mod__ = partialmethod(_binary_op, OperatorEnum.MOD)

    # Reverse Arithmetic
    __radd__ = partialmethod(_reverse_binary_op, OperatorEnum.ADD)
    __rsub__ = partialmethod(_reverse_binary_op, OperatorEnum.SUB)
    __rmul__ = partialmethod(_reverse_binary_op, OperatorEnum.MUL)
    __rfloordiv__ = partialmethod(_reverse_binary_op, OperatorEnum.FLOORDIV)
    __rtruediv__ = partialmethod(_reverse_binary_op, OperatorEnum.TRUEDIV)
    __rpow__ = partialmethod(_reverse_binary_op, OperatorEnum.POW)

    # Logical
    __and__ = partialmethod(_binary_op, OperatorEnum.AND)
    __or__ = partialmethod(_binary_op, OperatorEnum.OR)

    __lt__ = partialmethod(_binary_op, OperatorEnum.LT)
    __le__ = partialmethod(_binary_op, OperatorEnum.LE)
    __eq__ = partialmethod(_binary_op, OperatorEnum.EQ)  # type: ignore
    __ne__ = partialmethod(_binary_op, OperatorEnum.NEQ)  # type: ignore
    __gt__ = partialmethod(_binary_op, OperatorEnum.GT)
    __ge__ = partialmethod(_binary_op, OperatorEnum.GE)

    # Reverse Logical
    __rand__ = partialmethod(_reverse_binary_op, OperatorEnum.AND)
    __ror__ = partialmethod(_reverse_binary_op, OperatorEnum.OR)

    @abstractmethod
    def _display_str(self) -> str:
        raise NotImplementedError()

    def alias(self, name: str) -> Expression:
        """Gives the expression a new name, which is its column's name in the DataFrame schema and the name
        by which subsequent expressions can refer to the results of this expression.

        Example:
            >>> col("x").alias("y")

        Args:
            name: New name for expression

        Returns:
            Expression: Renamed expression
        """
        return AliasExpression(self, name)

    def as_py(self, type_: type) -> AsPyExpression:
        """Treats every value on the given expression as an object of the specified type. Users can then call
        methods on this object, which will be translated into a method call on every value on the Expression.

        Example:
            >>> # call .resize(28, 28) on every item in the expression
            >>> col("images").as_py(Image).resize(28, 28)
            >>>
            >>> # get the 0th element of every item in the expression
            >>> col("tuples").as_py(tuple)[0]

        Args:
            type_ (Type): type of each item in the expression

        Returns:
            AsPyExpression: A special Expression that records any method calls that a user runs on it, applying the method call to each item in the expression.
        """
        return AsPyExpression(self, ExpressionType.from_py_type(type_))

    def apply(self, func: Callable, return_type: type | None = None) -> Expression:
        """Apply a function on a given expression

        Example:
            >>> def f(x_val: str) -> int:
            >>>     return int(x_val) if x_val.isnumeric() else 0
            >>>
            >>> col("x").apply(f, return_type=int)

        Args:
            func (Callable): Function to run per value of the expression
            return_type (Optional[Type], optional): Return type of the function that was ran. This defaults to None and Daft will infer the return_type
                from the function's type annotations if available.

        Returns:
            Expression: New expression after having run the function on the expression
        """
        inferred_type = get_type_hints(func).get("return", None)
        return_type = inferred_type if inferred_type is not None else return_type
        if return_type is None:
            warnings.warn(
                f"Supplied function {func} was not annotated with a return type and no `return_type` keyword argument specified in `.apply`."
                "It is highly recommended to specify a return_type for Daft to perform optimizations and for access to vectorized expression operators."
            )

        expression_type = (
            ExpressionType.from_py_type(return_type) if return_type is not None else ExpressionType.python_object()
        )

        def apply_func(f, data):
            return list(map(f, data.iter_py()))

        return UdfExpression(
            partial(apply_func, func),
            expression_type,
            (self,),
            {},
        )

    def has_call(self) -> bool:
        if isinstance(self, CallExpression) or isinstance(self, UdfExpression):
            return True
        if len(self._children()) > 0:
            return any(c.has_call() for c in self._children())
        return False

    def is_same(self, other: Expression) -> bool:
        if self is other:
            return True
        ids_match = self.has_id() and self.get_id() == other.get_id()
        return ids_match

    @abstractmethod
    def _is_eq_local(self, other: Expression) -> bool:
        raise NotImplementedError()

    def is_eq(self, other: Expression) -> bool:
        if self.is_same(other):
            return True

        if not self._is_eq_local(other):
            return False

        if len(self._children()) != len(other._children()):
            return False

        for s, o in zip(self._children(), other._children()):
            if not s.is_eq(o):
                return False

        return True

    def if_else(self, if_true: Expression, if_false: Expression) -> Expression:
        """Choose values from `if_true` and `if_false` based on the current expression as the condition.

        Example:
            >>> # x = [2, 2, 2]
            >>> # y = [1, 2, 3]
            >>> # a = ["a", "a", "a"]
            >>> # b = ["b", "b", "b"]
            >>> # if_else_result = ["a", "b", "b"]
            >>> (col("x") > col("y")).if_else(col("a"), col("b"))

        Args:
            if_true (Expression): Values to choose if condition is true
            if_false (Expression): Values to choose if condition is false

        Returns:
            Expression: New expression where values are chosen from `if_true` and `if_false`.
        """
        return CallExpression(
            OperatorEnum.IF_ELSE,
            (self, if_true, if_false),
        )

    def is_null(self) -> Expression:
        """Checks if values in the Expression are Null (i.e. they are missing)

        Example:
            >>> # [1., None, NaN] -> [False, True, False]
            >>> col("x").is_null()

        Returns:
            Expression: LOGICAL Expression indicating whether values are missing
        """
        return CallExpression(
            OperatorEnum.IS_NULL,
            (self,),
        )

    def is_nan(self) -> Expression:
        """Checks if values in the Expression are NaN (i.e. they are invalid)

        Example:
            >>> # [1., None, NaN] -> [False, False, True]
            >>> col("x").is_nan()

        Returns:
            Expression: LOGICAL Expression indicating whether values are invalid.
        """
        return CallExpression(
            OperatorEnum.IS_NAN,
            (self,),
        )

    def cast(self, to: type) -> Expression:
        """Casts an expression to a given type

        Casting defaults to a set of "reasonable behaviors" for each ``(from_type, to_type)`` pair. For more fine-grained control, please consult
        each type's method accessors or make a feature request for an accessor if none exists for your use-case.

        This method supports:

        1. Casting of a PY type to a Primitive type (e.g. converting a PY[object] column of strings to STRING with ``.cast(str)``) - this will coerce each Python object into the specified primitive type, and then convert the data to an optimized backend representation.
        2. Casting between Primitive types, with a set of reasonable default behavior for many type pairs

        Example:

            >>> # [1.0, 2.5, None]: FLOAT -> [1, 2, None]: INTEGER
            >>> col("float").cast(int)
            >>>
            >>> # [Path("/tmp1"), Path("/tmp2"), Path("/tmp3")]: PY[Path] -> ["/tmp1", "/tmp1", "/tmp1"]: STRING
            >>> col("path_obj_col").cast(str)

        Returns:
            Expression: Expression with the specified new type
        """
        if to == str:
            return CallExpression(
                OperatorEnum.CAST_STRING,
                (self,),
            )
        elif to == int:
            return CallExpression(
                OperatorEnum.CAST_INT,
                (self,),
            )
        elif to == float:
            return CallExpression(
                OperatorEnum.CAST_FLOAT,
                (self,),
            )
        elif to == bool:
            return CallExpression(
                OperatorEnum.CAST_LOGICAL,
                (self,),
            )
        elif to == datetime.date:
            return CallExpression(
                OperatorEnum.CAST_DATE,
                (self,),
            )
        elif to == bytes:
            return CallExpression(
                OperatorEnum.CAST_BYTES,
                (self,),
            )
        raise NotImplementedError(f"Casting to a non-primitive Python object {to} not yet implemented")

    ###
    # Accessors
    ###

    @property
    def url(self) -> UrlMethodAccessor:
        """Access methods that work on columns of URLs"""
        return UrlMethodAccessor(self)

    @property
    def str(self) -> StringMethodAccessor:
        """Access methods that work on columns of strings"""
        return StringMethodAccessor(self)

    @property
    def dt(self) -> DatetimeMethodAccessor:
        """Access methods that work on columns of datetimes"""
        return DatetimeMethodAccessor(self)


class LiteralExpression(Expression):
    def __init__(self, value: Any) -> None:
        super().__init__()
        self._value = value

    def resolved_type(self) -> ExpressionType | None:
        return ExpressionType.from_py_type(type(self._value))

    def _display_str(self) -> str:
        return f"lit({self._value})"

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, LiteralExpression) and self._value == other._value


class CallExpression(Expression):
    def __init__(
        self,
        operator: OperatorEnum,
        func_args: tuple,
    ) -> None:
        super().__init__()
        self._args_ids = tuple(self._register_child(self._to_expression(arg)) for arg in func_args)
        self._operator = operator

    def resolved_type(self) -> ExpressionType | None:
        args_resolved_types = tuple(arg.resolved_type() for arg in self._args)
        if any([arg_type is None for arg_type in args_resolved_types]):
            return None
        args_resolved_types_non_none = cast(Tuple[ExpressionType, ...], args_resolved_types)
        ret_type = self._operator.value.get_return_type(args_resolved_types_non_none)
        if ret_type == ExpressionType.unknown():
            operator: ExpressionOperator = self._operator.value
            op_pretty_print = ""
            operator_symbol = operator.symbol or operator.name
            op_pretty_print = (
                f"{self._args[0]} {operator_symbol} {self._args[1]}"
                if operator.nargs == 2
                else f"{operator_symbol}({', '.join([str(arg) for arg in self._args])})"
            )
            raise ExpressionTypeError(f"Unable to resolve type for operation: {op_pretty_print}")

        return ret_type

    @property
    def _args(self) -> tuple[Expression, ...]:
        return tuple(self._children()[i] for i in self._args_ids)

    def _display_str(self) -> str:
        symbol = self._operator.value.symbol or self._operator.value.name

        # Handle Binary Case:
        if len(self._args) == 2:
            return f"[{self._args[0]._display_str()} {symbol} {self._args[1]._display_str()}]"

        args = ", ".join(a._display_str() for a in self._args)
        return f"{symbol}({args})"

    def _is_eq_local(self, other: Expression) -> bool:
        return (
            isinstance(other, CallExpression)
            and self._operator == other._operator
            and self._args_ids == other._args_ids
        )


class UdfExpression(Expression):
    def __init__(
        self,
        func: Callable[..., Sequence],
        func_ret_type: ExpressionType,
        func_args: tuple,
        func_kwargs: dict[str, Any],
        resource_request: ResourceRequest | None = None,
    ) -> None:
        super().__init__()
        self._func = func
        self._func_ret_type = func_ret_type
        self._args_ids = tuple(self._register_child(self._to_expression(arg)) for arg in func_args)
        self._kwargs_ids = {kw: self._register_child(self._to_expression(arg)) for kw, arg in func_kwargs.items()}
        self._resource_request = resource_request

    def _self_resource_request(self) -> ResourceRequest:
        if self._resource_request is not None:
            return self._resource_request
        return ResourceRequest.default()

    @property
    def _args(self) -> tuple[Expression, ...]:
        return tuple(self._children()[i] for i in self._args_ids)

    @property
    def _kwargs(self) -> dict[str, Expression]:
        return {kw: self._children()[i] for kw, i in self._kwargs_ids.items()}

    def resolved_type(self) -> ExpressionType | None:
        return self._func_ret_type

    def _display_str(self) -> str:
        func_name: str = self._func.func.__name__ if isinstance(self._func, partial) else self._func.__name__  # type: ignore
        args = ", ".join(a._display_str() for a in self._args)
        kwargs = ", ".join(f"{kw}={a._display_str()}" for kw, a in self._kwargs.items())
        if kwargs:
            return f"{func_name}({args}, {kwargs})"
        return f"{func_name}({args})"

    def _is_eq_local(self, other: Expression) -> bool:
        return (
            isinstance(other, UdfExpression)
            and self._func == other._func
            and self._func_ret_type == other._func_ret_type
            and self._args_ids == other._args_ids
            and self._kwargs_ids == other._kwargs_ids
        )

    def has_call(self) -> bool:
        return True


T = TypeVar("T")


class ColumnExpression(Expression):
    def __init__(self, name: str, expr_type: ExpressionType | None = None) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._name = name
        self._type = expr_type

    def resolved_type(self) -> ExpressionType | None:
        return self._type

    def _display_str(self) -> str:
        s = f"col({self._name}"
        if self.has_id():
            s += f"#{self._id}"
        if self.resolved_type() is not None:
            s += f": {self.resolved_type()}"
        s = s + ")"
        return s

    def __repr__(self) -> str:
        return self._display_str()

    def name(self) -> str | None:
        return self._name

    def required_columns(self, unresolved_only: bool = False) -> list[ColumnExpression]:
        if unresolved_only and self.has_id():
            return []
        return [self]

    def resolve_to_expression(self, other: Expression) -> ColID:
        assert self.name() == other.name()
        self._id = other.get_id()
        assert self._id is not None
        self._type = other.resolved_type()
        assert self._type is not None
        return self._id

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, ColumnExpression) and self._name == other._name and self.get_id() == other.get_id()


class AliasExpression(Expression):
    def __init__(self, expr: Expression, name: str) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._register_child(expr)
        self._name = name

    def resolved_type(self) -> ExpressionType | None:
        return self._expr.resolved_type()

    @property
    def _expr(self) -> Expression:
        return self._children()[0]

    def _display_str(self) -> str:
        return f"[{self._expr}].alias({self._name})"

    def name(self) -> str | None:
        return self._name

    def get_id(self) -> ColID | None:
        return self._expr.get_id()

    def _assign_id(self, strict: bool = True) -> ColID:
        return self._expr._assign_id(strict)

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, AliasExpression) and self._name == other._name


class AsPyExpression(Expression):
    def __init__(
        self,
        expr: Expression,
        type_: ExpressionType,
        attr_name: str | None = "__call__",
    ) -> None:
        super().__init__()
        self._type = type_
        self._register_child(expr)
        self._attr_name = attr_name

    def __getattr__(self, name: str):
        return AsPyExpression(
            self._expr,
            type_=self._type,
            attr_name=name,
        )

    def __call__(self, *args, **kwargs):
        python_cls = self._type.python_cls
        attr_name = self._attr_name

        def f(expr, *args, **kwargs):
            results = []
            for vals in zip_blocks_as_py(expr, *args, *[arg for arg in kwargs.values()]):
                method = getattr(python_cls, attr_name)
                result = method(
                    vals[0],  # self
                    *vals[1 : len(args) + 1],
                    **{kw: val for kw, val in zip(kwargs.keys(), *vals[len(args) + 1 :])},
                )
                results.append(result)
            return results

        return UdfExpression(
            f,
            ExpressionType.python_object(),
            func_args=(self._expr,) + args,
            func_kwargs=kwargs,
        )

    def __getitem__(self, key):
        python_cls = self._type.python_cls
        attr_name = "__getitem__"

        def __getitem__(expr, keys):
            results = []
            for expr_val, key_val in zip_blocks_as_py(expr, keys):
                method = getattr(python_cls, attr_name)
                result = method(expr_val, key_val)
                results.append(result)
            return results

        return UdfExpression(
            __getitem__,
            ExpressionType.python_object(),
            func_args=(self._expr, key),
            func_kwargs={},
        )

    def _display_str(self) -> str:
        return f"[{self._expr}] ({self._type})"

    @property
    def _expr(self) -> Expression:
        return self._children()[0]

    def resolved_type(self) -> ExpressionType | None:
        return self._type

    def get_id(self) -> ColID | None:
        return self._expr.get_id()

    def _assign_id(self, strict: bool = True) -> ColID:
        return self._expr._assign_id(strict)

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, AliasExpression) and self._name == other._name


###
# Expression Method Accessors
###


class BaseMethodAccessor:
    def __init__(self, expr: Expression):
        self._expr = expr


class UrlMethodAccessor(BaseMethodAccessor):
    def download(self) -> UdfExpression:
        """Treats each string as a URL, and downloads the bytes contents as a bytes column"""
        return UdfExpression(
            url_operators.download,
            ExpressionType.from_py_type(bytes),
            (self._expr,),
            {},
        )


class StringMethodAccessor(BaseMethodAccessor):
    def concat(self, other: Any) -> CallExpression:
        """Concatenates two string expressions together

        Args:
            other (Expression): a string expression to concatenate with

        Returns:
            CallExpression: a STRING expression which is `self` concatenated with `other`
        """
        if isinstance(other, str):
            other = lit(other)
        assert isinstance(other, Expression)
        return CallExpression(
            OperatorEnum.STR_CONCAT,
            (self._expr, other),
        )

    def contains(self, pattern: str) -> CallExpression:
        """Checks whether each string contains the given pattern in a string column

        Example:
            >>> col("x").str.contains("foo")

        Args:
            pattern (str): pattern to search for

        Returns:
            CallExpression: a LOGICAL expression indicating whether each value contains the provided pattern
        """
        if not isinstance(pattern, str):
            raise ExpressionTypeError(f"Expected pattern to be a Python string, received: {pattern}")
        return CallExpression(
            OperatorEnum.STR_CONTAINS,
            (self._expr, pattern),
        )

    def endswith(self, pattern: str) -> CallExpression:
        """Checks whether each string ends with the given pattern in a string column

        Example:
            >>> col("x").str.endswith("foo")

        Args:
            pattern (str): pattern to search for

        Returns:
            CallExpression: a LOGICAL expression indicating whether each value ends with the provided pattern
        """
        if not isinstance(pattern, str):
            raise ExpressionTypeError(f"Expected pattern to be a Python string, received: {pattern}")
        return CallExpression(
            OperatorEnum.STR_ENDSWITH,
            (self._expr, pattern),
        )

    def startswith(self, pattern: str) -> CallExpression:
        """Checks whether each string starts with the given pattern in a string column

        Example:
            >>> col("x").str.startswith("foo")

        Args:
            pattern (str): pattern to search for

        Returns:
            CallExpression: a LOGICAL expression indicating whether each value starts with the provided pattern
        """
        if not isinstance(pattern, str):
            raise ExpressionTypeError(f"Expected pattern to be a Python string, received: {pattern}")
        return CallExpression(
            OperatorEnum.STR_STARTSWITH,
            (self._expr, pattern),
        )

    def length(self) -> CallExpression:
        """Retrieves the length for of a UTF-8 string column

        Example:
            >>> col("x").str.length()

        Returns:
            CallExpression: an INTEGER expression with the length of each string
        """
        return CallExpression(
            OperatorEnum.STR_LENGTH,
            (self._expr,),
        )


class DatetimeMethodAccessor(BaseMethodAccessor):
    def day(self) -> CallExpression:
        """Retrieves the day for a datetime column

        Example:
            >>> col("x").dt.day()

        Returns:
            CallExpression: an INTEGER expression with just the day extracted from a datetime column
        """
        return CallExpression(
            OperatorEnum.DT_DAY,
            (self._expr,),
        )

    def month(self) -> CallExpression:
        """Retrieves the month for a datetime column

        Example:
            >>> col("x").dt.month()

        Returns:
            CallExpression: an INTEGER expression with just the month extracted from a datetime column
        """
        return CallExpression(
            OperatorEnum.DT_MONTH,
            (self._expr,),
        )

    def year(self) -> CallExpression:
        """Retrieves the year for a datetime column

        Example:
            >>> col("x").dt.year()

        Returns:
            CallExpression: an INTEGER expression with just the year extracted from a datetime column
        """
        return CallExpression(
            OperatorEnum.DT_YEAR,
            (self._expr,),
        )

    def day_of_week(self) -> CallExpression:
        """Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday

        Example:
            >>> col("x").dt.day_of_week()

        Returns:
            CallExpression: an INTEGER expression with just the day_of_week extracted from a datetime column
        """
        return CallExpression(
            OperatorEnum.DT_DAY_OF_WEEK,
            (self._expr,),
        )
