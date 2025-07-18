from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from traceback import TracebackException


class ExpressionTypeError(Exception):
    pass


class UDFException(Exception):
    """An Daft exception raised when a UDF raises an exception.

    Also provides additional context about the error. The original exception is either available as:
    - The original exception via `__cause__` if running in the same process
    - A replica & rendered traceback via `tb_info` if running in a different process
    Both are accessible via `original_exception`
    """

    def __init__(self, message: str, tb_info: TracebackException | None = None):
        super().__init__(message)
        self.message = message
        self.tb_info = tb_info

    @property
    def original_exception(self) -> BaseException | None:
        """The original exception that was raised by the UDF."""
        # We except every creation of UDFException to be `raise UDFException(...) from ...`
        return self.__cause__

    def __str__(self) -> str:
        if self.tb_info:
            return (
                "\n".join(self.tb_info.format())
                + "\nThe above exception was the direct cause of the following exception:\n"
                + f"\n{self.message}"
            )
        else:
            return self.message
