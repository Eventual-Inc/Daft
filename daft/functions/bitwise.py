"""Bitwise Functions."""

from __future__ import annotations

from daft.expressions import Expression


def bitwise_and(left: Expression, right: Expression) -> Expression:
    """Bitwise AND of two integer expressions."""
    left = Expression._to_expression(left)
    right = Expression._to_expression(right)
    return Expression._from_pyexpr(left._expr & right._expr)


def bitwise_or(left: Expression, right: Expression) -> Expression:
    """Bitwise OR of two integer expressions."""
    left = Expression._to_expression(left)
    right = Expression._to_expression(right)
    return Expression._from_pyexpr(left._expr | right._expr)


def bitwise_xor(left: Expression, right: Expression) -> Expression:
    """Bitwise XOR of two integer expressions."""
    left = Expression._to_expression(left)
    right = Expression._to_expression(right)
    return Expression._from_pyexpr(left._expr ^ right._expr)


def shift_left(expr: Expression, num_bits: Expression) -> Expression:
    """Shifts the bits of an integer expression to the left (``expr << num_bits``).

    Args:
        expr: The expression to shift.
        num_bits: The number of bits to shift the expression to the left
    """
    expr = Expression._to_expression(expr)
    num_bits = Expression._to_expression(num_bits)
    return Expression._from_pyexpr(expr._expr << num_bits._expr)


def shift_right(expr: Expression, num_bits: Expression) -> Expression:
    """Shifts the bits of an integer expression to the right (``expr >> num_bits``).

    Args:
        expr: The expression to shift.
        num_bits: The number of bits to shift the expression to the right

    Note:
        For unsigned integers, this expression perform a logical right shift.
        For signed integers, this expression perform an arithmetic right shift.
    """
    expr = Expression._to_expression(expr)
    num_bits = Expression._to_expression(num_bits)
    return Expression._from_pyexpr(expr._expr >> num_bits._expr)
