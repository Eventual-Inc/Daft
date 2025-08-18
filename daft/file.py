from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.daft import PyDaftFile

if TYPE_CHECKING:
    from daft.io import IOConfig


class File:
    """A file-like object for working with file contents in Daft.

    This is an abstract base class that provides a standard file interface compatible
    with Python's file protocol. It handles both filesystem-based files and in-memory
    data through its concrete subclasses PathFile and MemoryFile.

    The File object can be used with most Python libraries that accept file-like objects,
    and implements the standard read/seek/tell interface. Files are read-only in the
    current implementation.

    Examples:
        >>> import daft
        >>> from daft.functions import file
        >>> df = daft.from_pydict({"paths": ["data.json"]})
        >>> df = df.select(file(df["paths"]))
        >>>
        >>> @daft.func
        >>> def read_json(file: daft.File) -> str:
        >>>     import json
        >>>     data = json.load(file)
        >>>     return data["text"]
    """

    _inner: PyDaftFile

    def __init__(self, str_or_bytes: str | bytes) -> None:
        if isinstance(str_or_bytes, str):
            self._inner = PyDaftFile._from_path(str_or_bytes)
        elif isinstance(str_or_bytes, bytes):
            self._inner = PyDaftFile._from_bytes(str_or_bytes)
        else:
            raise TypeError("str_or_bytes must be a string or bytes")

    @staticmethod
    def _from_py_daft_file(f: PyDaftFile) -> File:
        file = File.__new__(File)
        file._inner = f
        return file

    @staticmethod
    def _from_path(path: str) -> File:
        inner = PyDaftFile._from_path(path)
        file = PathFile.__new__(PathFile)
        file._inner = inner
        return file

    @staticmethod
    def _from_bytes(bytes: bytes) -> File:
        inner = PyDaftFile._from_bytes(bytes)
        file = MemoryFile.__new__(MemoryFile)
        file._inner = inner
        return file

    def read(self, size: int = -1) -> bytes:
        return self._inner.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def close(self) -> None:
        self._inner.close()

    def open(self, _io_config: IOConfig | None = None) -> File:
        raise NotImplementedError("File.open() not yet supported")

    def __enter__(self) -> File:
        inner = self._inner.__enter__()
        self._inner = inner
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._inner.__exit__(exc_type, exc_val, exc_tb)

    def __str__(self) -> str:
        return self._inner.__str__()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def closed(self) -> bool:
        return self._inner.closed()


class PathFile(File):
    """File object backed by a filesystem path.

    A concrete implementation of File that represents data stored in the filesystem.
    In addition to the standard file interface, PathFile implements the os.PathLike
    protocol via __fspath__, allowing it to be used with functions that expect file paths.

    """

    def __fspath__(self) -> str:
        return self._inner.__fspath__()


class MemoryFile(File):
    """File object backed by in-memory data.

    A concrete implementation of File that represents data stored in memory.
    MemoryFile provides a file-like interface to in-memory binary data,
    useful for working with data that doesn't exist on disk.

    """

    def get_bytes(self) -> bytes:
        return self._inner.read()
