from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.expressions.expressions import Expression

if TYPE_CHECKING:
    from daft.daft import PyExpr


def get_unique_function_name(fn: Any) -> str:
    """Compute a name for the given function using its module and qualified name.

    This name should be unique.
    """
    module_name = getattr(fn, "__module__")
    qual_name: str = getattr(fn, "__qualname__")
    if module_name:
        return f"{module_name}.{qual_name}"
    else:
        return qual_name


def get_expr_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> list[PyExpr]:
    expr_args = []
    for arg in args:
        if isinstance(arg, Expression):
            expr_args.append(arg._expr)
    for arg in kwargs.values():
        if isinstance(arg, Expression):
            expr_args.append(arg._expr)
    return expr_args
