"""File Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions import Expression
    from daft.io import IOConfig


def file(expr: Expression, io_config: IOConfig | None = None) -> Expression:
    """Converts either a string containing a file reference, or a binary column to a `daft.File` reference.

    If the input is a string, it is assumed to be a file path and is converted to a `daft.File`.
    If the input is a binary column, it is converted to a `daft.File` where the entire contents are buffered in memory.
    """
    return expr._eval_expressions("file", io_config=io_config)


def file_size(expr: Expression) -> Expression:
    """Returns the size of the file in bytes.

    Args:
        expr: The expression to evaluate.

    Returns:
        Expression: An expression containing the file size in bytes
    """
    return expr._eval_expressions("file_size")
