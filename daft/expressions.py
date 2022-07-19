from __future__ import annotations

import functools
import operator
from abc import abstractmethod
from functools import partialmethod
from typing import Any, Callable, Dict, Optional, Tuple


def col(name: str) -> ColumnExpression:
    return ColumnExpression(name)


class Expression:
    def _to_expression(self, input: Any) -> Expression:
        if not isinstance(input, Expression):
            return LiteralExpression(input)
        return input

    def _unary_op(self, func: Callable, symbol: Optional[str] = None) -> Expression:
        return UnaryOpExpression(self, func, symbol=symbol)

    def _binary_op(self, other: Any, func: Callable, symbol: Optional[str] = None) -> Expression:
        other_expr = self._to_expression(other)
        return BinaryOpExpression(self, other_expr, func, symbol=symbol)

    def _reverse_binary_op(self, other: Any, func: Callable, symbol: Optional[str] = None) -> Expression:
        other_expr = self._to_expression(other)
        return other_expr._binary_op(self, func, symbol=symbol)

    @abstractmethod
    def is_literal(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_operation(self) -> bool:
        raise NotImplementedError()

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


class LiteralExpression(Expression):
    def __init__(self, value: Any) -> None:
        self._value = value

    def __repr__(self) -> str:
        return f"lit({self._value})"

    def is_literal(self) -> bool:
        return True

    def is_operation(self) -> bool:
        return False

    def eval(self, **kwargs):
        return self._value


class OpExpression(Expression):
    def is_literal(self) -> bool:
        return False

    def is_operation(self) -> bool:
        return True


class UnaryOpExpression(OpExpression):
    def __init__(self, operand: Expression, op: Callable, symbol: Optional[str] = None) -> None:
        if not isinstance(operand, Expression):
            raise ValueError(f"expected {operand} to be of type Expression, is {type(operand)}")
        self._operand = operand
        self._op = op
        self._symbol = symbol

    def __repr__(self) -> str:
        op_name = self._op.__name__
        if self._symbol is None:
            return f"[{op_name}({self._operand})]"
        else:
            return f"{self._symbol}({self._operand})"

    def eval(self, **kwargs):
        operand = self._operand.eval(**kwargs)
        return self._op(operand)


class BinaryOpExpression(OpExpression):
    def __init__(self, left: Expression, right: Expression, op: Callable, symbol: Optional[str] = None) -> None:
        self._left = left
        self._right = right
        self._op = op
        self._symbol = symbol

    def __repr__(self) -> str:
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


class MultipleReturnSelectExpression(OpExpression):
    def __init__(self, expr: Expression, n: int) -> None:
        self._expr = expr
        self._n = n

    def __repr__(self) -> str:
        return f"{self._expr}[{self._n}]"

    def eval(self, **kwargs):
        all_values = self._expr.eval(**kwargs)
        assert isinstance(all_values, tuple), f"expected multiple returns from {self._expr}"
        assert len(all_values) > self._n
        value = all_values[self._n]
        return value


class UDFExpression(OpExpression):
    def __init__(self, func: Callable, func_args: Tuple, func_kwargs: Optional[Dict[str, Any]] = None) -> None:
        self._args = tuple(self._to_expression(arg) for arg in func_args)
        if func_kwargs is None:
            func_kwargs = dict()
        self._kwargs = {k: self._to_expression(v) for k, v in func_kwargs.items()}
        self._func = func

    def is_operation(self) -> bool:
        return True

    def __repr__(self) -> str:
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
        if not isinstance(name, str):
            raise TypeError(f"Exprected name to be type str, is {type(name)}")
        self._name = name

    def __repr__(self) -> str:
        return f"col({self._name})"

    def is_literal(self) -> bool:
        return False

    def is_operation(self) -> bool:
        return False

    def eval(self, **kwargs):
        if self._name not in kwargs:
            raise ValueError(f"expected column `{self._name}` to be passed into eval")
        return kwargs[self._name]
