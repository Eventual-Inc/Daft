"""File Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions import BinaryExpr, FileExpr, IntExpr, StringExpr
    from daft.io import IOConfig


def file(expr: StringExpr | BinaryExpr, io_config: IOConfig | None = None) -> FileExpr:
    """Converts either a string containing a file reference, or a binary column to a `daft.File` reference.

    Args:
        expr: The `Binary` or `String` to evaluate.
        io_config: The IO configuration to use.

    Returns:
        Expression: A `File` expression containing the file reference.

    If the input is a string, it is assumed to be a file path and is converted to a `daft.File`.
    If the input is a binary column, it is converted to a `daft.File` where the entire contents are buffered in memory.
    """
    return expr._eval_expressions("file", io_config=io_config)


def file_size(expr: FileExpr) -> IntExpr:
    """Returns the size of the file in bytes.

    Args:
        expr: The `File` expression to evaluate.

    Returns:
        Expression: A `UInt64` expression containing the file size in bytes
    """
    return expr._eval_expressions("file_size")
