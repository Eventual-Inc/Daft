from __future__ import annotations

import functools
import itertools
import operator
from abc import abstractmethod
from functools import partialmethod
from typing import Any, Callable, Dict, List, Optional, Tuple

from daft.internal.treenode import TreeNode


def col(name: str) -> ColumnExpression:
    return ColumnExpression(name)


_COUNTER = itertools.count()


class Expression(TreeNode["Expression"]):
    def __init__(self) -> None:
        super().__init__()
        self._id: Optional[int] = None

    def __repr__(self) -> str:
        if self._id is None:
            return self._display_str()
        else:
            return f"{self._display_str()} AS {self.name()}#{self._id}"

    def _to_expression(self, input: Any) -> Expression:
        if not isinstance(input, Expression):
            return LiteralExpression(input)
        return input

    def _unary_op(self, func: Callable, symbol: Optional[str] = None) -> Expression:
        return UnaryCallExpression(self, func, symbol=symbol)

    def _binary_op(self, other: Any, func: Callable, symbol: Optional[str] = None) -> Expression:
        other_expr = self._to_expression(other)
        return BinaryCallExpression(self, other_expr, func, symbol=symbol)

    def _reverse_binary_op(self, other: Any, func: Callable, symbol: Optional[str] = None) -> Expression:
        other_expr = self._to_expression(other)
        return other_expr._binary_op(self, func, symbol=symbol)

    def name(self) -> Optional[str]:
        for child in self._children():
            name = child.name()
            if name is not None:
                return name
        return None

    def _assign_id(self, strict: bool = True) -> int:
        if self._id is None:
            self._id = next(_COUNTER)
        else:
            if strict:
                raise ValueError(f"We have already assigned an id, {self._id}")
        return self._id

    def required_columns(self, unresolved_only: bool = False) -> List[ColumnExpression]:
        to_rtn: List[ColumnExpression] = []
        for child in self._children():
            to_rtn.extend(child.required_columns(unresolved_only))
        return to_rtn

    # UnaryOps

    # Arithmetic
    __neg__ = partialmethod(_unary_op, func=operator.neg, symbol="-")
    __pos__ = partialmethod(_unary_op, func=operator.pos, symbol="+")
    __abs__ = partialmethod(_unary_op, func=operator.abs)

    __invert__ = partialmethod(_unary_op, func=operator.not_, symbol="~")

    # function
    def map(self, func: Callable) -> Expression:
        return self._unary_op(func)

    # BinaryOps

    # Arithmetic
    __add__ = partialmethod(_binary_op, func=operator.add, symbol="+")
    __sub__ = partialmethod(_binary_op, func=operator.sub, symbol="-")
    __mul__ = partialmethod(_binary_op, func=operator.mul, symbol="*")
    __floordiv__ = partialmethod(_binary_op, func=operator.floordiv, symbol="//")
    __truediv__ = partialmethod(_binary_op, func=operator.truediv, symbol="/")
    __pow__ = partialmethod(_binary_op, func=operator.pow, symbol="**")

    # Reverse Arithmetic
    __radd__ = partialmethod(_reverse_binary_op, func=operator.add, symbol="+")
    __rsub__ = partialmethod(_reverse_binary_op, func=operator.sub, symbol="-")
    __rmul__ = partialmethod(_reverse_binary_op, func=operator.mul, symbol="*")
    __rfloordiv__ = partialmethod(_reverse_binary_op, func=operator.floordiv, symbol="//")
    __rtruediv__ = partialmethod(_reverse_binary_op, func=operator.truediv, symbol="/")
    __rpow__ = partialmethod(_reverse_binary_op, func=operator.pow, symbol="**")

    # Logical
    __and__ = partialmethod(_binary_op, func=operator.and_, symbol="&")
    __or__ = partialmethod(_binary_op, func=operator.or_, symbol="|")

    __lt__ = partialmethod(_binary_op, func=operator.lt, symbol="<")
    __le__ = partialmethod(_binary_op, func=operator.le, symbol="<=")
    __eq__ = partialmethod(_binary_op, func=operator.eq, symbol="=")  # type: ignore
    __ne__ = partialmethod(_binary_op, func=operator.ne, symbol="!=")  # type: ignore
    __gt__ = partialmethod(_binary_op, func=operator.gt, symbol=">")
    __ge__ = partialmethod(_binary_op, func=operator.ge, symbol=">=")

    # Reverse Logical
    __rand__ = partialmethod(_reverse_binary_op, func=operator.and_, symbol="&")
    __ror__ = partialmethod(_reverse_binary_op, func=operator.or_, symbol="|")

    @abstractmethod
    def eval(self, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def _display_str(self) -> str:
        raise NotImplementedError()

    def alias(self, name: str) -> Expression:
        return AliasExpression(self, name)

    def has_call(self) -> bool:
        if len(self._children()) > 0:
            return any(c.has_call() for c in self._children())
        return False

    def is_same(self, other: ColumnExpression) -> bool:
        """Checks if this Expression is the symbolic the same as the following ColumnExpression

        Args:
            other (ColumnExpression): the symbolic column whos name and id have to match to self's

        Raises:
            ValueError: if the ids match but the names dont

        Returns:
            bool: if the two expressions are symbolic the same
        """
        ids_match = self._id is not None and self._id == other._id
        if ids_match and (self.name() != other.name()):
            raise ValueError(f"ids match both names dont: self {self.name()}, other {other.name()}")
        return ids_match


class LiteralExpression(Expression):
    def __init__(self, value: Any) -> None:
        super().__init__()
        self._value = value

    def _display_str(self) -> str:
        return f"lit({self._value})"

    def eval(self, **kwargs):
        return self._value


class CallExpression(Expression):
    def __init__(self) -> None:
        super().__init__()

    def has_call(self) -> bool:
        return True


class UnaryCallExpression(CallExpression):
    def __init__(self, operand: Expression, op: Callable, symbol: Optional[str] = None) -> None:
        super().__init__()
        if not isinstance(operand, Expression):
            raise ValueError(f"expected {operand} to be of type Expression, is {type(operand)}")
        self._operand = self._register_child(operand)
        self._op = op
        self._symbol = symbol

    def _display_str(self) -> str:
        op_name = self._op.__name__
        if self._symbol is None:
            return f"[{op_name}({self._operand})]"
        else:
            return f"{self._symbol}({self._operand})"

    def eval(self, **kwargs):
        operand = self._operand.eval(**kwargs)
        return self._op(operand)


class BinaryCallExpression(CallExpression):
    def __init__(self, left: Expression, right: Expression, op: Callable, symbol: Optional[str] = None) -> None:
        super().__init__()
        self._left = self._register_child(left)
        self._right = self._register_child(right)
        self._op = op
        self._symbol = symbol

    def _display_str(self) -> str:
        op_name = self._op.__name__
        if self._symbol is None:
            symbol = op_name
        else:
            symbol = self._symbol
        return f"[{self._left} {symbol} {self._right}]"

    def eval(self, **kwargs):
        eval_left = self._left.eval(**kwargs)
        eval_right = self._right.eval(**kwargs)
        return self._op(eval_left, eval_right)


class MultipleReturnSelectExpression(Expression):
    def __init__(self, expr: Expression, n: int) -> None:
        super().__init__()
        self._expr = self._register_child(expr)
        self._n = n

    def _display_str(self) -> str:
        return f"{self._expr}[{self._n}]"

    def eval(self, **kwargs):
        all_values = self._expr.eval(**kwargs)
        assert isinstance(all_values, tuple), f"expected multiple returns from {self._expr}"
        assert len(all_values) > self._n
        value = all_values[self._n]
        return value


class UDFExpression(CallExpression):
    def __init__(self, func: Callable, func_args: Tuple, func_kwargs: Optional[Dict[str, Any]] = None) -> None:
        super().__init__()
        self._args = tuple(self._register_child(self._to_expression(arg)) for arg in func_args)
        if func_kwargs is None:
            func_kwargs = dict()
        self._kwargs = {k: self._register_child(self._to_expression(v)) for k, v in func_kwargs.items()}
        self._func = func

    def is_operation(self) -> bool:
        return True

    def _display_str(self) -> str:
        func_name = self._func.__name__
        args = ", ".join(repr(a) for a in self._args)
        if len(self._kwargs) == 0:
            return f"Expr:{func_name}({args})"

        kwargs = ", ".join(f"{k}={repr(v)}" for k, v in self._kwargs.items())
        return f"Expr:{func_name}({args}, {kwargs})"

    def eval(self, **kwargs):
        eval_args = tuple(a.eval(**kwargs) for a in self._args)
        eval_kwargs = {k: self.eval(**kwargs) for k, v in self._kwargs.items()}
        return self._func(*eval_args, **eval_kwargs)


def udf(f: Callable | None = None, num_returns: int = 1) -> Callable:
    def udf_decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            if any(isinstance(a, Expression) for a in args) or any(isinstance(a, Expression) for a in kwargs.values()):
                out_expr = UDFExpression(func, func_args=args, func_kwargs=kwargs)
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
    def __init__(self, name: str) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._name = name

    def _display_str(self) -> str:
        if self._id is None:
            return f"col({self._name})"
        else:
            return f"col({self._name}#{self._id})"

    def __repr__(self) -> str:
        return self._display_str()

    def eval(self, **kwargs):
        if self._name not in kwargs:
            raise ValueError(f"expected column `{self._name}` to be passed into eval")
        return kwargs[self._name]

    def name(self) -> Optional[str]:
        return self._name

    def required_columns(self, unresolved_only: bool = False) -> List[ColumnExpression]:
        if unresolved_only and self._id is not None:
            return []
        return [self]

    def assign_id_from_expression(self, other: Expression) -> int:
        assert other._id is not None
        assert self.name() == other.name()
        self._id = other._id
        return self._id


class AliasExpression(Expression):
    def __init__(self, expr: Expression, name: str) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._expr = self._register_child(expr)
        self._name = name

    def _display_str(self) -> str:
        return f"{self._expr}.alias({self._name})"

    def name(self) -> Optional[str]:
        return self._name

    def eval(self, **kwargs):
        return self._expr.eval(**kwargs)
