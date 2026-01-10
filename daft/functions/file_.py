"""File Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions import Expression
    from daft.io import IOConfig


def file(url: Expression, io_config: IOConfig | None = None) -> Expression:
    """Converts a string containing a file reference to a `daft.File` reference.

    Args:
        url (StringExpression): the url of the file
        io_config (IOConfig, default=None): The IO configuration to use.

    Returns:
        Expression (File Expression): An expression containing the file reference.

    """
    return url._eval_expressions("file", io_config=io_config)


def video_file(url: Expression, verify: bool = False, io_config: IOConfig | None = None) -> Expression:
    """Converts a string containing a file reference to a `daft.VideoFile` reference.

    Args:
        url (String Expression): the url of the file
        verify:
            If True, verify that the file exists and is a video file.
            If **ANY** files are not videos, this will produce an error.

        io_config (IOConfig, default=None): The IO configuration to use.

    Returns:
        Expression (File[Video] Expression): An expression containing the file reference.

    """
    return url._eval_expressions("video_file", verify=verify, io_config=io_config)


def audio_file(url: Expression, verify: bool = False, io_config: IOConfig | None = None) -> Expression:
    """Converts a string containing a file reference to a `daft.AudioFile` reference.

    Args:
        url (String Expression): the url of the file
        verify:
            If True, verify that the file exists and is a audio file.
            If **ANY** files are not audios, this will produce an error.

        io_config (IOConfig, default=None): The IO configuration to use.

    Returns:
        Expression (File[Audio] Expression): An expression containing the file reference.

    """
    return url._eval_expressions("audio_file", verify=verify, io_config=io_config)


def file_size(file: Expression) -> Expression:
    """Returns the size of the file in bytes.

    Args:
        file (File Expression): expression to evaluate.

    Returns:
        Expression (UInt64 Expression): expression containing the file size in bytes
    """
    return file._eval_expressions("file_size")


def guess_mime_type(bytes_expr: Expression) -> Expression:
    """Guess the MIME type of binary data by inspecting magic bytes.

    Detects common file formats including: PNG, JPEG, GIF, WEBP, PDF, ZIP,
    MP3, WAV, OGG, MP4, MPEG, and HTML. Returns None if the format cannot
    be determined.

    Args:
        bytes_expr: Binary expression containing raw bytes.

    Returns:
        Expression: String expression containing MIME type (e.g., "image/png") or None.

    Example:
        >>> import daft
        >>> from daft.functions import guess_mime_type
        >>> df = daft.from_pydict({"data": [b"\\x89PNG\\r\\n\\x1a\\n"]})
        >>> df = df.with_column("mime", guess_mime_type(df["data"]))
        >>> df.collect()
    """
    return bytes_expr._eval_expressions("guess_mime_type")
