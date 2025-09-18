from __future__ import annotations

from typing import TYPE_CHECKING, Any

import daft.pickle
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


def check_fn_serializable(fn: Any, source_decorator: str) -> None:
    """Check that the function is serializable."""
    # TODO: Expand this check to look at the global vars and identify the non-serializable one.
    try:
        _ = daft.pickle.dumps(fn)
    except Exception as e:
        raise ValueError(
            f"`{source_decorator}` requires that the UDF is serializable. Please double-check that the function does not use any global variables.\n\nIf it does, please use the legacy `@daft.udf` with a class UDF instead and initialize the global in the `__init__` method."
        ) from e
