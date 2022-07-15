from __future__ import annotations

import operator
from functools import partialmethod
from typing import Any, Callable


def col(name: str) -> ColumnExpression:
    return ColumnExpression(name)


class Expression:
    def _binary_op(self, other: Any, func: Callable) -> Expression:
        if not isinstance(other, Expression):
            other = LiteralExpression(other)
        return BinaryOpExpression(self, other, func)

    def is_literal(self) -> bool:
        return False

    def is_operation(self) -> bool:
        return False

    # Arithmetic
    __add__ = partialmethod(_binary_op, func=operator.add)
    __sub__ = partialmethod(_binary_op, func=operator.sub)
    __mul__ = partialmethod(_binary_op, func=operator.mul)
    __floordiv__ = partialmethod(_binary_op, func=operator.floordiv)
    __truediv__ = partialmethod(_binary_op, func=operator.truediv)
    __pow__ = partialmethod(_binary_op, func=operator.pow)

    # Logical
    __and__ = partialmethod(_binary_op, func=operator.and_)
    __or__ = partialmethod(_binary_op, func=operator.or_)

    __lt__ = partialmethod(_binary_op, func=operator.lt)
    __le__ = partialmethod(_binary_op, func=operator.le)
    __eq__ = partialmethod(_binary_op, func=operator.eq)
    __ne__ = partialmethod(_binary_op, func=operator.ne)
    __gt__ = partialmethod(_binary_op, func=operator.gt)
    __ge__ = partialmethod(_binary_op, func=operator.ge)


class LiteralExpression(Expression):
    def __init__(self, value: Any) -> None:
        self._value = value

    def __repr__(self) -> str:
        return f"lit({self._value})"

    def is_literal(self) -> bool:
        return True


class BinaryOpExpression(Expression):
    pretty_print_symbols = {
        "add": "+",
        "sub": "-",
        "mul": "*",
        "floordiv": "//",
        "truediv": "/",
        "pow": "**",
        "and_": "&",
        "or": "|",
        "lt": "<",
        "le": "<=",
        "eq": "=",
        "ne": "!=",
        "gt": ">",
        "ge": ">=",
    }

    def __init__(self, left: Expression, right: Expression, op: Callable) -> None:
        self._left = left
        self._right = right
        self._op = op

    def __repr__(self) -> str:
        op_name = self._op.__name__
        symbol = BinaryOpExpression.pretty_print_symbols.get(op_name, op_name)
        return f"[{self._left} {symbol} {self._right}]"

    def is_operation(self) -> bool:
        return True


class ColumnExpression(Expression):
    def __init__(self, name: str) -> None:
        self._name = name

    def __repr__(self) -> str:
        return f"col({self._name})"
