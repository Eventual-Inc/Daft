from __future__ import annotations

from typing import Literal

from daft.expressions import Expression


def encode(expr: Expression, charset: Literal["utf-8", "utf8"]) -> Expression:
    """Encode binary or string values using the specified character set.

    Args:
        expr: The expression to encode.
        charset (optional): The encoding character set. Defaults to "utf-8".

    Returns:
        Expression: A binary expression with the encoded value.

    Note:
        This inputs either a string or binary and returns a binary.
        If the input value is a string and 'utf-8' is the character set, then it's just a cast to binary.
        If the input value is a binary and 'utf-8' is the character set, we verify the bytes are valid utf-8.
    """
    return Expression._call_builtin_scalar_fn("encode", expr, codec=charset)


def decode(expr: Expression, charset: Literal["utf-8", "utf8"]) -> Expression:
    """Decodes binary values using the specified character set.

    Args:
        charset (optional): The decoding character set. Defaults to "utf-8".

    Returns:
        Expression: A string expression with the decoded values.

    """
    return Expression._call_builtin_scalar_fn("decode", expr, codec=charset)


def try_encode(expr: Expression, charset: Literal["utf-8", "utf8"]) -> Expression:
    """Encode or null if unsuccessful.

    Tip: See Also
        [`daft.functions.encode`](https://docs.daft.ai/en/stable/api/functions/encode/)
    """
    return Expression._call_builtin_scalar_fn("try_encode", expr, codec=charset)


def try_decode(expr: Expression, charset: Literal["utf-8", "utf8"]) -> Expression:
    """Decode or null if unsuccessful.

    Tip: See Also
        [`daft.functions.decode`](https://docs.daft.ai/en/stable/api/functions/decode/)
    """
    return Expression._call_builtin_scalar_fn("try_decode", expr, codec=charset)


def compress(expr: Expression, codec: Literal["deflate", "gzip", "gz", "zlib"]) -> Expression:
    r"""Compress binary or string values using the specified codec.

    Args:
        expr: The expression to compress.
        codec: The compression codec (deflate, gzip, or zlib)

    Returns:
        Expression: A binary expression with the compressed value.

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


def decompress(expr: Expression, codec: Literal["deflate", "gzip", "gz", "zlib"]) -> Expression:
    """Decompress binary values using the specified codec.

    Args:
        codec: The decompression codec (deflate, gzip, zlib)

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
    return Expression._call_builtin_scalar_fn("decode", expr, codec=codec)


def try_compress(expr: Expression, codec: Literal["deflate", "gzip", "gz", "zlib"]) -> Expression:
    """Compress or null if unsuccessful.

    Tip: See Also
        [`daft.functions.compress`](https://docs.daft.ai/en/stable/api/functions/compress/)
    """
    return Expression._call_builtin_scalar_fn("try_encode", expr, codec=codec)


def try_decompress(expr: Expression, codec: Literal["deflate", "gzip", "gz", "zlib"]) -> Expression:
    """Decompress or null if unsuccessful.

    Tip: See Also
        [`daft.functions.decompress`](https://docs.daft.ai/en/stable/api/functions/decompress/)
    """
    return Expression._call_builtin_scalar_fn("try_decode", expr, codec=codec)
