"""File Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions import Expression
    from daft.io import IOConfig


def file(filepath_or_bytes: Expression, io_config: IOConfig | None = None) -> Expression:
    """Converts either a string containing a file reference, or a binary column to a `daft.File` reference.

    Args:
        filepath_or_bytes (String or Binary Expression):
            If the input is a string, it is assumed to be a file path and is converted to a `daft.File`.

            If the input is a binary column, it is converted to a `daft.File` where the entire contents are buffered in memory.

        io_config (IOConfig, default=None): The IO configuration to use.


    Returns:
        Expression (File Expression): An expression containing the file reference.

    """
    return filepath_or_bytes._eval_expressions("file", io_config=io_config)


def file_size(file: Expression) -> Expression:
    """Returns the size of the file in bytes.

    Args:
        file (File Expression): expression to evaluate.

    Returns:
        Expression (UInt64 Expression): expression containing the file size in bytes
    """
    return file._eval_expressions("file_size")
