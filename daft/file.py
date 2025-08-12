from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from io import TextIOWrapper, _WrappedBuffer

    from daft.daft import PyDaftFile


class File:
    _inner: PyDaftFile

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a File via __init__ ")

    @staticmethod
    def _from_py_daft_file(f: PyDaftFile) -> File:
        file = File.__new__(File)
        file._inner = f
        return file

    def read(self, size: int = -1) -> bytes:
        return self._inner.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> TextIOWrapper[_WrappedBuffer]:
        return open(self._inner)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._inner.__exit__(exc_type, exc_val, exc_tb)

    def __fspath__(self) -> str:
        return self._inner.__fspath__()

    def __str__(self) -> str:
        return self._inner.__str__()
