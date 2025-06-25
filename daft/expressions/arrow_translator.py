from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyarrow.compute import Expression as PCExpression

    from daft.daft import PyExpr


def expr_to_arrow(expr: PyExpr) -> PCExpression:
    from pyarrow import compute as pc

    if expr.is_column():
        return pc.field(expr.column_name())
    elif expr.is_literal():
        return pc.scalar(expr.literal_value())
    elif (op := expr.operator()) is not None:
        children = [expr_to_arrow(child) for child in expr.children()]
        return _translate_operator(op, children)
    raise NotImplementedError(f"Unsupported expression: {expr}")


def _translate_operator(op: str, args: list[PCExpression]) -> PCExpression:
    from pyarrow import compute as pc

    normalized_op = op.lower()

    op_map = {
        # comparison functions
        "eq": pc.equal,
        "noteq": pc.not_equal,
        "lt": pc.less,
        "gt": pc.greater,
        "lteq": pc.less_equal,
        "gteq": pc.greater_equal,
        # logical functions
        "not": pc.invert,
        "and": lambda a, b: a & b,
        "or": lambda a, b: a | b,
        # arithmetic functions
        "plus": lambda a, b: a + b,
        "minus": lambda a, b: a - b,
        "multiply": lambda a, b: a * b,
        "divide": lambda a, b: a / b,
        "modulus": lambda a, b: a % b,
        # TODO support time-related functions
        "year": lambda a: pc.year(a),
        "month": lambda a: pc.month(a),
        "day": lambda a: pc.day(a),
        "hour": lambda a: pc.hour(a),
        "minute": lambda a: pc.minute(a),
        "second": lambda a: pc.second(a),
    }

    if normalized_op not in op_map:
        raise ValueError(f"Unsupported operator: {op}")

    if normalized_op in ["eq", "noteq", "lt", "gt", "lteq", "gteq"]:
        return op_map[normalized_op](args[0], args[1])

    if normalized_op == "not":
        return op_map[normalized_op](args[0])

    result = args[0]
    for arg in args[1:]:
        result = op_map[normalized_op](result, arg)
    return result


def _translate_function(func_name: str, args: list[PyExpr]) -> PCExpression:
    from pyarrow import compute as pc

    func_map = {
        "year": pc.year,
        "month": pc.month,
        "day": pc.day,
        "hour": pc.hour,
        "minute": pc.minute,
        "second": pc.second,
    }

    if func_name not in func_map:
        raise NotImplementedError(f"Unsupported function: {func_name}")

    return func_map[func_name](*args)
