from __future__ import annotations

import functools
import itertools
import operator
from abc import abstractmethod
from functools import partialmethod
from typing import Any, Callable, Dict, List, NewType, Optional, Tuple, TypeVar

from daft.execution.operators import ExpressionOperator, ExpressionType, Operators
from daft.internal.treenode import TreeNode

T = TypeVar("T")


def _assert_not_none(obj: Optional[T]) -> T:
    assert obj is not None
    return obj


def col(name: str) -> ColumnExpression:
    return ColumnExpression(name)


def lit(val: Any) -> LiteralExpression:
    return LiteralExpression(val)


_COUNTER = itertools.count()
ColID = NewType("ColID", int)


class Expression(TreeNode["Expression"]):
    """Expressions are TreeNodes. They have a few notable properties.

    Name: a user-facing identifier for the expression, used for human-readable schemas and for referring
        to an expression with a human-readable name when users want to run computations on it.

    ID: every Expression that represents a new value is assigned an ID. Expressions that are **references**
        to existing Expressions (e.g. AliasExpression or ColumnExpression) are not assigned an ID.

    Resolved: Expressions are "resolved" iff they have an ID and all their children are "resolved" as well.
        Expressions are usually unresolved when they have a leaf ColumnExpression that has not yet been assigned
        as ID. All resolved Expressions have a resolved_type.

        Resolving an Expression involves looking up all the leaf ColumnExpressions, using their Names to find
        the appropriate Expression that they refer to and having them inherit that Expression's ID.
    """

    def __init__(self) -> None:
        super().__init__()
        self._id: Optional[ColID] = None

    def __repr__(self) -> str:
        s = self._display_str()
        if self.name() is not None and self.has_id():
            s = s + f" AS {self.name()}#{self.get_id()}"
        if self.resolved():
            s = s + f": {self.resolved_type()}"
        return s

    def _to_expression(self, input: Any) -> Expression:
        if not isinstance(input, Expression):
            return LiteralExpression(input)
        return input

    def _unary_op(
        self,
        operator: ExpressionOperator,
        # TODO: deprecate func and symbol in favor of just operator
        func: Callable,
        symbol: Optional[str] = None,
    ) -> Expression:
        return CallExpression(operator, func, func_args=(self,), symbol=symbol)

    def _binary_op(
        self,
        operator: ExpressionOperator,
        other: Any,
        # TODO: deprecate func and symbol in favor of just operator
        func: Callable,
        symbol: Optional[str] = None,
    ) -> Expression:
        other_expr = self._to_expression(other)
        return CallExpression(operator, func, func_args=(self, other_expr), symbol=symbol)

    def _reverse_binary_op(
        self,
        operator: ExpressionOperator,
        other: Any,
        # TODO: deprecate func and symbol in favor of just operator
        func: Callable,
        symbol: Optional[str] = None,
    ) -> Expression:
        other_expr = self._to_expression(other)
        return other_expr._binary_op(operator, self, func, symbol=symbol)

    def name(self) -> Optional[str]:
        """Expressions can have a `name` which is a user-facing identifier for the expression so that
        users can refer to them when specifying queries/computation. Names are not unique in the tree
        and are used during resolution of ColumnExpressions.
        """
        for child in self._children():
            name = child.name()
            if name is not None:
                return name
        return None

    def _assign_id(self) -> ColID:
        """Subclasses should call this on creation if they compute a new expression rather than hold a reference to one"""
        if self.has_id():
            raise ValueError(f"We have already assigned an id, {self.get_id()}")
        self._id = ColID(next(_COUNTER))
        return self._id

    def resolved(self) -> bool:
        """Expressions are "resolved" iff all children ColumnExpressions have assigned IDs."""
        if not all([child.resolved() for child in self._children()]):
            return False
        return True

    @abstractmethod
    def resolved_type(self) -> Optional[ExpressionType]:
        """Expressions have a resolved_type only if they are resolved"""
        return None

    def get_id(self) -> Optional[ColID]:
        return self._id

    def has_id(self) -> bool:
        return self.get_id() is not None

    def to_column_expression(self) -> ColumnExpression:
        """Creates a new ColumnExpression that refers to the current Expression - it inherits the current Expression's ID"""
        if not self.has_id():
            raise ValueError("we can only convert expressions with assigned id to ColumnExpressions")
        name = self.name()
        if name is None:
            raise ValueError("we can only convert expressions to ColumnExpressions if they have a name")
        ce = ColumnExpression(name)
        ce.resolve_to_expression(self)
        return ce

    def required_columns(self, unresolved_only: bool = False) -> List[ColumnExpression]:
        """Returns all the leaf nodes of the tree that are ColumnExpressions"""
        to_rtn: List[ColumnExpression] = []
        for child in self._children():
            to_rtn.extend(child.required_columns(unresolved_only))
        return to_rtn

    def _replace_column_with_expression(self, col_expr: ColumnExpression, new_expr: Expression) -> Expression:
        """For expressions in the subtree where self is the root, replace all `col_expr` with `new_expr`"""
        assert col_expr.is_same(new_expr)
        if isinstance(self, ColumnExpression) and self.is_eq(col_expr):
            return new_expr
        for i in range(len(self._children())):
            self._registered_children[i] = self._registered_children[i]._replace_column_with_expression(
                col_expr, new_expr
            )
        return self

    # UnaryOps

    # Arithmetic
    __neg__ = partialmethod(_unary_op, Operators.NEGATE.value, func=operator.neg, symbol="-")
    __pos__ = partialmethod(_unary_op, Operators.POSITIVE.value, func=operator.pos, symbol="+")
    __abs__ = partialmethod(_unary_op, Operators.ABS.value, func=operator.abs)

    # Logical
    __invert__ = partialmethod(_unary_op, Operators.INVERT.value, func=operator.not_, symbol="~")

    # BinaryOps

    # Arithmetic
    __add__ = partialmethod(_binary_op, Operators.ADD.value, func=operator.add, symbol="+")
    __sub__ = partialmethod(_binary_op, Operators.SUB.value, func=operator.sub, symbol="-")
    __mul__ = partialmethod(_binary_op, Operators.MUL.value, func=operator.mul, symbol="*")
    __floordiv__ = partialmethod(_binary_op, Operators.FLOORDIV.value, func=operator.floordiv, symbol="//")
    __truediv__ = partialmethod(_binary_op, Operators.TRUEDIV.value, func=operator.truediv, symbol="/")
    __pow__ = partialmethod(_binary_op, Operators.POW.value, func=operator.pow, symbol="**")
    __mod__ = partialmethod(_binary_op, Operators.MOD.value, func=operator.mod, symbol="%")

    # Reverse Arithmetic
    __radd__ = partialmethod(_reverse_binary_op, Operators.ADD.value, func=operator.add, symbol="+")
    __rsub__ = partialmethod(_reverse_binary_op, Operators.SUB.value, func=operator.sub, symbol="-")
    __rmul__ = partialmethod(_reverse_binary_op, Operators.MUL.value, func=operator.mul, symbol="*")
    __rfloordiv__ = partialmethod(_reverse_binary_op, Operators.FLOORDIV.value, func=operator.floordiv, symbol="//")
    __rtruediv__ = partialmethod(_reverse_binary_op, Operators.TRUEDIV.value, func=operator.truediv, symbol="/")
    __rpow__ = partialmethod(_reverse_binary_op, Operators.POW.value, func=operator.pow, symbol="**")

    # Logical
    __and__ = partialmethod(_binary_op, Operators.AND.value, func=operator.and_, symbol="&")
    __or__ = partialmethod(_binary_op, Operators.OR.value, func=operator.or_, symbol="|")

    __lt__ = partialmethod(_binary_op, Operators.LT.value, func=operator.lt, symbol="<")
    __le__ = partialmethod(_binary_op, Operators.LE.value, func=operator.le, symbol="<=")
    __eq__ = partialmethod(_binary_op, Operators.EQ.value, func=operator.eq, symbol="=")  # type: ignore
    __ne__ = partialmethod(_binary_op, Operators.NEQ.value, func=operator.ne, symbol="!=")  # type: ignore
    __gt__ = partialmethod(_binary_op, Operators.GT.value, func=operator.gt, symbol=">")
    __ge__ = partialmethod(_binary_op, Operators.GE.value, func=operator.ge, symbol=">=")

    # Reverse Logical
    __rand__ = partialmethod(_reverse_binary_op, Operators.AND.value, func=operator.and_, symbol="&")
    __ror__ = partialmethod(_reverse_binary_op, Operators.OR.value, func=operator.or_, symbol="|")

    @abstractmethod
    def eval(self, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def _display_str(self) -> str:
        raise NotImplementedError()

    def alias(self, name: str) -> Expression:
        """Creates a new AliasExpression that refers to the current Expression - it inherits the current Expression's ID"""
        return AliasExpression(self, name)

    def has_call(self) -> bool:
        if isinstance(self, CallExpression):
            return True
        if len(self._children()) > 0:
            return any(c.has_call() for c in self._children())
        return False

    def is_same(self, other: Expression) -> bool:
        """An Expression is_same as another Expression iff they have IDs and they share the same ID"""
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


class LiteralExpression(Expression):
    def __init__(self, value: Any) -> None:
        super().__init__()
        self._value = value
        self._assign_id()

    def resolved_type(self) -> Optional[ExpressionType]:
        return ExpressionType.from_py_obj(self._value)

    def _display_str(self) -> str:
        return f"lit({self._value})"

    def eval(self, **kwargs):
        return self._value

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, LiteralExpression) and self._value == other._value


class MultipleReturnSelectExpression(Expression):
    def __init__(self, expr: Expression, n: int) -> None:
        super().__init__()
        self._register_child(expr)
        self._n = n

    def resolved_type(self) -> Optional[ExpressionType]:
        return ExpressionType.UNKNOWN

    @property
    def _expr(self) -> Expression:
        return self._children()[0]

    def _display_str(self) -> str:
        return f"{self._expr}[{self._n}]"

    def eval(self, **kwargs):
        all_values = self._expr.eval(**kwargs)
        assert isinstance(all_values, tuple), f"expected multiple returns from {self._expr}"
        assert len(all_values) > self._n
        value = all_values[self._n]
        return value

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, MultipleReturnSelectExpression) and self._n == other._n


class CallExpression(Expression):
    def __init__(
        self,
        operator: ExpressionOperator,
        # TODO: deprecate func and symbol in favor of operator
        func: Callable,
        func_args: Tuple,
        func_kwargs: Optional[Dict[str, Any]] = None,
        symbol: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._assign_id()
        self._args_ids = tuple(self._register_child(self._to_expression(arg)) for arg in func_args)
        if func_kwargs is None:
            func_kwargs = dict()
        self._kwargs_ids = {k: self._register_child(self._to_expression(v)) for k, v in func_kwargs.items()}
        self._func = func
        self._symbol = symbol
        self._operator = operator

    def resolved_type(self) -> Optional[ExpressionType]:
        if not self.resolved():
            return None
        arg_types = tuple(_assert_not_none(arg.resolved_type()) for arg in self._args)
        return self._operator.type_matrix.get(arg_types, ExpressionType.UNKNOWN)

    @property
    def _args(self) -> Tuple[Expression, ...]:
        return tuple(self._children()[i] for i in self._args_ids)

    @property
    def _kwargs(self) -> Dict[str, Expression]:
        return {k: self._children()[i] for k, i in self._kwargs_ids.items()}

    def _display_str(self) -> str:
        symbol = self._func.__name__ if self._symbol is None else self._symbol

        # Handle Binary Case:
        if len(self._kwargs) == 0 and len(self._args) == 2:
            return f"[{self._args[0]._display_str()} {symbol} {self._args[1]._display_str()}]"

        args = ", ".join(a._display_str() for a in self._args)
        if len(self._kwargs) == 0:
            return f"{symbol}({args})"

        kwargs = ", ".join(f"{k}={v._display_str()}" for k, v in self._kwargs.items())
        return f"{symbol}({args}, {kwargs})"

    def eval(self, **kwargs):
        eval_args = tuple(a.eval(**kwargs) for a in self._args)
        eval_kwargs = {k: self.eval(**kwargs) for k, v in self._kwargs.items()}
        return self._func(*eval_args, **eval_kwargs)

    def _is_eq_local(self, other: Expression) -> bool:
        return (
            isinstance(other, CallExpression)
            and self._args_ids == other._args_ids
            and self._kwargs_ids == other._kwargs_ids
            and self._func == other._func
            and self._symbol == other._symbol
        )


def udf(f: Callable | None = None, num_returns: int = 1) -> Callable:
    def udf_decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            if any(isinstance(a, Expression) for a in args) or any(isinstance(a, Expression) for a in kwargs.values()):
                out_expr = CallExpression(None, func, func_args=args, func_kwargs=kwargs)
                if num_returns == 1:
                    return out_expr
                else:
                    assert num_returns > 1
                    return tuple(MultipleReturnSelectExpression(out_expr, i) for i in range(num_returns))
            else:
                return func(*args, **kwargs)

        return wrapped_func

    if f is None:
        return udf_decorator
    return udf_decorator(f)


class ColumnExpression(Expression):
    """ColumnExpressions are special Expressions that are either pointers to other Expressions or
    that can represent a scan operation.

    They have no children, always have an explicitly defined name and by default do not have an ID/type.
    When they are resolved, they inherit the ID and Type of the Expression that they resolve to.
    """

    def __init__(self, name: str, assign_id: bool = False, expr_type: Optional[ExpressionType] = None) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._name = name

        if assign_id:
            assert expr_type is not None, "Expression type must be provided if assigning ID to ColumnExpression"
            self._assign_id()
        self._type = expr_type

    def resolved_type(self) -> Optional[ExpressionType]:
        return self._type

    def _display_str(self) -> str:
        s = f"col({self._name}"
        if self.has_id():
            s += f"#{self._id}"
        if self.resolved():
            s += f": {self.resolved_type()}"
        s = s + ")"
        return s

    def __repr__(self) -> str:
        return self._display_str()

    def eval(self, **kwargs):
        if self._name not in kwargs:
            raise ValueError(f"expected column `{self._name}` to be passed into eval")
        return kwargs[self._name]

    def name(self) -> Optional[str]:
        return self._name

    def required_columns(self, unresolved_only: bool = False) -> List[ColumnExpression]:
        if unresolved_only and self.has_id():
            return []
        return [self]

    def resolve_to_expression(self, other: Expression) -> ColID:
        # TODO(jay): Could be a bug if `other` is not yet resolved but will be allocated a type later, so we
        # assert that it is resolved (guaranteed to have an ID and a Type)
        assert other.resolved(), "we do not allow resolving to unresolved Expressions"
        assert self.name() == other.name()
        self._id = other.get_id()
        assert self._id is not None
        self._type = other.resolved_type()
        assert self._type is not None
        return self._id

    def resolved(self) -> bool:
        return self.has_id()

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, ColumnExpression) and self._name == other._name and self.get_id() == other.get_id()


class AliasExpression(Expression):
    def __init__(self, expr: Expression, name: str) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._register_child(expr)
        self._name = name

    def resolved_type(self) -> Optional[ExpressionType]:
        return self._expr.resolved_type()

    @property
    def _expr(self) -> Expression:
        return self._children()[0]

    def _display_str(self) -> str:
        return f"[{self._expr}].alias({self._name})"

    def name(self) -> Optional[str]:
        return self._name

    def get_id(self) -> Optional[ColID]:
        return self._expr.get_id()

    def _assign_id(self) -> ColID:
        return self._expr._assign_id()

    def eval(self, **kwargs):
        return self._expr.eval(**kwargs)

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, AliasExpression) and self._name == other._name
