"""Binary Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft import DataType
from daft.expressions import Expression

if TYPE_CHECKING:
    from daft.expressions.expressions import COMPRESSION_CODEC, ENCODING_CHARSET


def encode(expr: Expression, charset: ENCODING_CHARSET) -> Expression:
    """Encode binary or string values using the specified character set.

    If an invalid encoding is encountered, an error will be raised.
    To handle invalid encodings, use `try_encode` instead.

    Args:
        expr (Binary or String Expression): The expression to encode.
        charset (str): The encoding character set (utf-8, base64).

    Returns:
        Expression (Binary Expression): A binary expression with the encoded value.

    Note:
        This inputs either a string or binary and returns a binary.
        If the input value is a string and 'utf-8' is the character set, then it's just a cast to binary.
        If the input value is a binary and 'utf-8' is the character set, we verify the bytes are valid utf-8.
    """
    return Expression._call_builtin_scalar_fn("encode", expr, codec=charset)


def decode(bytes: Expression, charset: ENCODING_CHARSET) -> Expression:
    """Decodes binary values using the specified character set.

    Note that if the charset is "utf-8" or "utf8", then this is equivalent
    to cast(bytes, daft.DataType.string())

    If an invalid encoding is encountered, an error will be raised.
    To handle invalid encodings, use `try_decode` instead.

    Args:
        bytes (Binary Expression): The expression to decode.
        charset (str): The decoding character set (utf-8, base64).

    Returns:
        Expression (Binary Expression): A binary expression with the decoded values.

    Examples:
        >>> import daft
        >>> from daft.functions import decode
        >>> df = daft.from_pydict({"bytes": [b"aGVsbG8sIHdvcmxkIQ=="]})
        >>> df.select(decode(df["bytes"], "base64")).show()
        ╭──────────────────╮
        │ bytes            │
        │ ---              │
        │ Binary           │
        ╞══════════════════╡
        │ b"hello, world!" │
        ╰──────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    if charset.lower() in ("utf-8", "utf8"):
        return bytes.cast(DataType.string())

    return Expression._call_builtin_scalar_fn("decode", bytes, codec=charset)


def try_encode(expr: Expression, charset: ENCODING_CHARSET) -> Expression:
    """Encode or null if unsuccessful.

    Tip: See Also
        [`daft.functions.encode`](https://docs.daft.ai/en/stable/api/functions/encode/)
    """
    return Expression._call_builtin_scalar_fn("try_encode", expr, codec=charset)


def try_decode(bytes: Expression, charset: ENCODING_CHARSET) -> Expression:
    """Decode or null if unsuccessful.

    Tip: See Also
        [`daft.functions.decode`](https://docs.daft.ai/en/stable/api/functions/decode/)
    """
    # TODO: Replace with a try_cast to string if the charset is "utf-8" or "utf8"
    # We currently don't have a try_cast
    return Expression._call_builtin_scalar_fn("try_decode", bytes, codec=charset)


def compress(expr: Expression, codec: COMPRESSION_CODEC) -> Expression:
    r"""Compress binary or string values using the specified codec.

    Args:
        expr (String | Binary Expression): The expression to compress.
        codec (str) The compression codec (deflate, gzip, or zlib)

    Returns:
        Expression (Binary Expression): A binary expression with the compressed value.

    Examples:
        >>> import daft
        >>> from daft.functions import compress
        >>> df = daft.from_pydict({"text": [b"hello, world!"]})  # binary
        >>> df.select(compress(df["text"], "zlib")).show()
        ╭────────────────────────────────╮
        │ text                           │
        │ ---                            │
        │ Binary                         │
        ╞════════════════════════════════╡
        │ b"x\x9c\xcbH\xcd\xc9\xc9\xd7Q… │
        ╰────────────────────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

        >>> df = daft.from_pydict({"text": ["hello, world!"]})  # string
        >>> df.select(compress(df["text"], "zlib")).show()
        ╭────────────────────────────────╮
        │ text                           │
        │ ---                            │
        │ Binary                         │
        ╞════════════════════════════════╡
        │ b"x\x9c\xcbH\xcd\xc9\xc9\xd7Q… │
        ╰────────────────────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

    """
    return Expression._call_builtin_scalar_fn("encode", expr, codec=codec)


def decompress(bytes: Expression, codec: COMPRESSION_CODEC) -> Expression:
    """Decompress binary values using the specified codec.

    Args:
        bytes (Binray Expression): The binary expression to decompress.
        codec (str): The decompression codec (deflate, gzip, zlib)

    Returns:
        Expression: A binary expression with the decoded values.

    Examples:
        >>> import zlib
        >>> import daft
        >>> from daft.functions import decompress
        >>> df = daft.from_pydict({"bytes": [zlib.compress(b"hello, world!")]})
        >>> df.select(decompress(df["bytes"], "zlib")).show()
        ╭──────────────────╮
        │ bytes            │
        │ ---              │
        │ Binary           │
        ╞══════════════════╡
        │ b"hello, world!" │
        ╰──────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

    """
    return Expression._call_builtin_scalar_fn("decode", bytes, codec=codec)


def try_compress(expr: Expression, codec: COMPRESSION_CODEC) -> Expression:
    """Compress or null if unsuccessful.

    Tip: See Also
        [`daft.functions.compress`](https://docs.daft.ai/en/stable/api/functions/compress/)
    """
    return Expression._call_builtin_scalar_fn("try_encode", expr, codec=codec)


def try_decompress(expr: Expression, codec: COMPRESSION_CODEC) -> Expression:
    """Decompress or null if unsuccessful.

    Tip: See Also
        [`daft.functions.decompress`](https://docs.daft.ai/en/stable/api/functions/decompress/)
    """
    return Expression._call_builtin_scalar_fn("try_decode", expr, codec=codec)
