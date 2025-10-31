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


def video_file(filepath_or_bytes: Expression, verify: bool = False, io_config: IOConfig | None = None) -> Expression:
    """Converts either a string containing a file reference, or a binary column to a `daft.VideoFile` reference.

    Args:
        filepath_or_bytes (String or Binary Expression):
            If the input is a string, it is assumed to be a file path and is converted to a `daft.VideoFile`.
            If the input is a binary column, it is converted to a `daft.VideoFile` where the entire contents are buffered in memory.

        verify:
            If True, verify that the file exists and is a video file.
            If **ANY** files are not videos, this will produce an error.

        io_config (IOConfig, default=None): The IO configuration to use.


    Returns:
        Expression (File[Video] Expression): An expression containing the file reference.

    """
    return filepath_or_bytes._eval_expressions("video_file", verify=verify, io_config=io_config)


def file_size(file: Expression) -> Expression:
    """Returns the size of the file in bytes.

    Args:
        file (File Expression): expression to evaluate.

    Returns:
        Expression (UInt64 Expression): expression containing the file size in bytes
    """
    return file._eval_expressions("file_size")
