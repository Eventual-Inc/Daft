from __future__ import annotations

import io
import sys
from io import IOBase
from typing import TYPE_CHECKING, Any, Literal

from daft.file import File

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from daft.daft import PyDaftFile
    from daft.io import IOConfig


# Also should inherit the io.Reader protocol from 3.14+
class DaftFileIO(IOBase):
    """A read-only file-like wrapper about Daft's IO backend.

    Implements :class:`io.IOBase` so instances work with
    libraries that expect standard Python file objects.
    """

    _inner: PyDaftFile

    def __init__(self, inner: PyDaftFile) -> None:
        self._inner = inner

    @override
    def read(self, size: int = -1, /) -> bytes:
        return self._inner.read(size)

    @override
    def seek(self, offset: int, whence: int = 0) -> int:
        return self._inner.seek(offset, whence)

    @override
    def tell(self) -> int:
        return self._inner.tell()

    @override
    def close(self) -> None:
        self._inner.close()

    @override
    @property
    def closed(self) -> bool:
        return self._inner.closed()

    @override
    def readable(self) -> bool:
        return True

    @override
    def writable(self) -> bool:
        return False

    @override
    def seekable(self) -> bool:
        return True

    @override
    def isatty(self) -> bool:
        return False

    @override
    def __enter__(self) -> DaftFileIO:
        self._inner.__enter__()
        return self

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self._inner.__exit__(exc_type, exc_val, exc_tb)

    def __repr__(self) -> str:
        return f"DaftFileIO({self._inner})"


def open_file(
    url: str,
    mode: Literal["r", "rt", "rb"] = "r",
    buffering: int = 0,
    encoding: str | None = None,
    io_config: IOConfig | None = None,
) -> io.IOBase:
    """Open a file from a URL, potentially from a remote filesystem using Daft's IO backend.

    This is not intended for building expressions inside of DataFrames. Instead, this
    is a convenience function for building custom DataSources and DataSinks without
    another filesystem library like fsspec or pyarrow.fs.

    Args:
        url (str): The URL of the file to open.
        mode (Literal["r", "rt", "rb"]): The mode to open the file in.
            - "r" / "rt": Text mode (default).
            - "rb": Binary mode.
        buffering (int): The buffering strategy to use.
            - 0: No buffering (default).
            - 1: Line buffering (only applies to text mode).
            - >1: Fixed-sized buffer of size `buffering`.
        encoding (str | None): The encoding to use for text mode.
            - None: Use the default encoding (based on locale.getencoding()).
            - Any valid encoding supported by Python's codecs module.
        io_config (IOConfig | None): The IO configuration to use.

    Returns:
        The opened file object to perform read/seek/tell operations on.
    """
    file_handle: io.IOBase
    file_handle = DaftFileIO(File(url, io_config).open())

    if buffering > 0 or mode in ("rt", "r"):
        file_handle = io.BufferedReader(file_handle, buffering)  # type: ignore[type-var]

    if mode == "rb":
        return file_handle
    elif mode in ("rt", "r"):
        return io.TextIOWrapper(file_handle, encoding=encoding)  # type: ignore[type-var]
    else:
        raise ValueError(f"Invalid file open mode `{mode}`. Only read modes `r`, `rt`, and `rb` are supported.")
