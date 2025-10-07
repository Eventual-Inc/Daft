"""File Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions import Expression
    from daft.io import IOConfig


def file(filepath_or_bytes: Expression, io_config: IOConfig | None = None) -> Expression:
    """Converts either a string containing a file reference, or a binary column to a `daft.File` reference.

    Args:
        expr: (String or Binary Expression) to evaluate.
        io_config: The IO configuration to use.

    If the input is a string, it is assumed to be a file path and is converted to a `daft.File`.
    If the input is a binary column, it is converted to a `daft.File` where the entire contents are buffered in memory.

    Returns:
        Expression: An expression containing the file reference.

    """
    return filepath_or_bytes._eval_expressions("file", io_config=io_config)


def file_size(file: Expression) -> Expression:
    """Returns the size of the file in bytes.

    Args:
        expr: (File Expression) to evaluate.

    Returns:
        Expression: A UInt64 expression containing the file size in bytes
    """
    return file._eval_expressions("file_size")
