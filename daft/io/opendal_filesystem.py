"""PyArrow filesystem backed by the `opendal` Python package.

Protocols that neither PyArrow nor fsspec handle natively (e.g. ``oss``,
``cos``, ``obs``, ``tos``) are routed here when the ``opendal`` Python package
is installed via ``daft[extra-fs]``. The handler speaks the fsspec-like
protocol expected by :class:`pyarrow.fs.FSSpecHandler`.
"""

from __future__ import annotations

from typing import Any

from daft.dependencies import pafs
from daft.io import IOConfig


class OpenDALFileSystemHandler:
    """FSSpec-like handler delegating to a blocking ``opendal.Operator``."""

    def __init__(self, protocol: str, io_config: IOConfig | None = None) -> None:
        try:
            import opendal
        except ImportError:
            raise ImportError(
                f"Accessing '{protocol}://' paths through PyArrow requires the `opendal` "
                f"Python package, which is not installed. Install it with "
                f"`pip install 'daft[extra-fs]'` (https://docs.daft.ai/en/latest/install)."
            ) from None

        from opendal import exceptions as opendal_exceptions

        self._exceptions = opendal_exceptions

        # fsspec introspection attribute used by FSSpecHandler.get_type_name.
        self.protocol = protocol

        backend_kwargs: dict[str, str] = {}
        if io_config is not None and io_config.opendal_backends is not None:
            backend_kwargs = dict(io_config.opendal_backends.get(protocol, {}))
        self.operator = opendal.Operator(protocol, **backend_kwargs)

    def _metadata_to_info(self, path: str, metadata: Any) -> dict[str, Any]:
        is_dir = metadata.mode.is_dir()
        return {
            "name": path,
            "type": "directory" if is_dir else "file",
            "size": 0 if is_dir else metadata.content_length,
        }

    def _entry_to_info(self, entry: Any) -> dict[str, Any]:
        return self._metadata_to_info(entry.path, entry.metadata)

    def info(self, path: str) -> dict[str, Any]:
        return self._metadata_to_info(path, self.operator.stat(path))

    def exists(self, path: str) -> bool:
        return self.operator.exists(path)

    def isdir(self, path: str) -> bool:
        try:
            return self.operator.stat(path).is_dir
        except self._exceptions.NotFound:
            return False

    def isfile(self, path: str) -> bool:
        try:
            return self.operator.stat(path).is_file
        except self._exceptions.NotFound:
            return False

    def mkdir(self, path: str, create_parents: bool = True) -> None:
        # opendal's create_dir is always recursive (mkdir -p semantics).
        self.operator.create_dir(path)

    def rm(self, path: str, recursive: bool = False) -> None:
        if recursive:
            self.operator.remove_all(path)
        else:
            self.operator.delete(path)

    def listdir(self, path: str, detail: bool = False) -> list[Any]:
        entries = self.operator.list(path)
        if detail:
            return [self._entry_to_info(entry) for entry in entries]
        return [entry.path for entry in entries]

    def find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        detail: bool = False,
    ) -> list[Any] | dict[str, dict[str, Any]]:
        # opendal's list is either single-level or fully recursive; approximate
        # fsspec's maxdepth semantics with those two modes.
        recursive = maxdepth is None or maxdepth > 1
        entries = self.operator.list(path, recursive=recursive)
        if not withdirs:
            entries = [e for e in entries if not e.metadata.mode.is_dir()]
        if detail:
            return {e.path: self._entry_to_info(e) for e in entries}
        return [e.path for e in entries]

    def mv(self, src: str, dest: str, recursive: bool = True) -> None:
        self.operator.rename(src, dest)

    def copy(self, src: str, dest: str) -> None:
        self.operator.copy(src, dest)

    def open(self, path: str, mode: str = "rb") -> Any:
        # opendal's sync File implements the full file protocol
        # (read/readinto/write/seek/tell/close), which is all PythonFile needs.
        # Its write() requires true `bytes`, so adapt pyarrow Buffers here.
        return _OpenDALFileWrapper(self.operator.open(path, mode))


class _OpenDALFileWrapper:
    """Adapt an `opendal.File` for pyarrow consumption.

    pyarrow passes `pyarrow.Buffer` objects to ``write``, while opendal's
    ``File.write`` only accepts true ``bytes``. All other file operations are
    delegated transparently.
    """

    def __init__(self, file: Any) -> None:
        self._file = file

    def write(self, data: Any) -> int:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            data = bytes(data)
        return self._file.write(data)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._file, name)


class OpenDALFileSystem(pafs.PyFileSystem):  # type: ignore[misc]
    """PyArrow filesystem for protocols handled by the `opendal` Python package."""

    def __init__(self, protocol: str, io_config: IOConfig | None = None) -> None:
        handler = OpenDALFileSystemHandler(protocol=protocol, io_config=io_config)
        super().__init__(pafs.FSSpecHandler(handler))
