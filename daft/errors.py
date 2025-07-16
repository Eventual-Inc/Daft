from __future__ import annotations


class ExpressionTypeError(Exception):
    pass


class UDFException(Exception):
    """An Daft exception raised when a UDF raises an exception.

    Also provides additional context about the error.
    """

    pass
