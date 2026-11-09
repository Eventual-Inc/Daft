from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


class IOFileType(str, Enum):
    """The type of an entry returned by an IO extension."""

    FILE = "file"
    DIRECTORY = "directory"


@dataclass(frozen=True)
class IOFileInfo:
    """Metadata for a file or directory returned by :meth:`IOExtension.ls`."""

    path: str
    file_type: IOFileType
    size: int | None = None

    def __post_init__(self) -> None:
        if self.size is not None and self.size < 0:
            raise ValueError("IOFileInfo.size must be non-negative")


@dataclass(frozen=True)
class IOListing:
    """One page of results returned by :meth:`IOExtension.ls`."""

    files: Sequence[IOFileInfo]
    continuation_token: str | None = None
    not_found_if_empty: bool = False


@dataclass(frozen=True)
class IOReadRange:
    """An HTTP-style half-open byte range passed to :meth:`IOExtension.get`.

    A bounded request has both ``start`` and ``end``. An offset request has only
    ``start``. A suffix request has only ``suffix``.
    """

    start: int | None = None
    end: int | None = None
    suffix: int | None = None

    def __post_init__(self) -> None:
        if self.suffix is not None:
            if self.start is not None or self.end is not None or self.suffix < 0:
                raise ValueError("A suffix range must only set a non-negative suffix")
            return
        if self.start is None or self.start < 0:
            raise ValueError("A non-suffix range must set a non-negative start")
        if self.end is not None and self.end < self.start:
            raise ValueError("IOReadRange.end must be greater than or equal to start")


class IOExtension:
    """Async interface for extending Daft's IO layer with a Python filesystem.

    Register an instance for a URI scheme through
    ``IOConfig(io_extensions={"scheme": extension})``. Implementations must be
    serializable because Daft may send the configured extension to workers.
    Paths passed to methods and returned in listings are full URIs.
    """

    async def supports_range(self, path: str) -> bool:
        """Return whether ``get`` supports byte-range requests for ``path``."""
        raise NotImplementedError

    async def get(self, path: str, byte_range: IOReadRange | None = None) -> bytes:
        """Read bytes from ``path``, optionally restricted to ``byte_range``."""
        raise NotImplementedError

    async def put(self, path: str, data: bytes) -> None:
        """Write ``data`` to ``path``."""
        raise NotImplementedError

    async def get_size(self, path: str) -> int:
        """Return the size of the file at ``path`` in bytes."""
        raise NotImplementedError

    async def ls(
        self,
        path: str,
        *,
        posix: bool,
        continuation_token: str | None = None,
        page_size: int | None = None,
    ) -> IOListing:
        """Return one page of files and directories under ``path``."""
        raise NotImplementedError

    async def delete(self, path: str) -> None:
        """Delete ``path`` if it exists."""
        raise NotImplementedError


def _read_range(kind: str | None, start: int | None, end: int | None) -> IOReadRange | None:
    if kind is None:
        return None
    if kind == "bounded":
        return IOReadRange(start=start, end=end)
    if kind == "offset":
        return IOReadRange(start=start)
    if kind == "suffix":
        return IOReadRange(suffix=start)
    raise ValueError(f"Unknown IO read range kind: {kind}")


async def _supports_range(extension: IOExtension, path: str) -> bool:
    result = await extension.supports_range(path)
    if not isinstance(result, bool):
        raise TypeError("IOExtension.supports_range() must return bool")
    return result


async def _get(
    extension: IOExtension,
    path: str,
    range_kind: str | None,
    range_start: int | None,
    range_end: int | None,
) -> bytes:
    result = await extension.get(path, _read_range(range_kind, range_start, range_end))
    if not isinstance(result, bytes):
        raise TypeError("IOExtension.get() must return bytes")
    return result


async def _put(extension: IOExtension, path: str, data: bytes) -> None:
    await extension.put(path, data)


async def _get_size(extension: IOExtension, path: str) -> int:
    result = await extension.get_size(path)
    if not isinstance(result, int) or isinstance(result, bool) or result < 0:
        raise TypeError("IOExtension.get_size() must return a non-negative int")
    return result


async def _ls(
    extension: IOExtension,
    path: str,
    posix: bool,
    continuation_token: str | None,
    page_size: int | None,
) -> tuple[list[tuple[str, int | None, str]], str | None, bool]:
    result = await extension.ls(
        path,
        posix=posix,
        continuation_token=continuation_token,
        page_size=page_size,
    )
    if not isinstance(result, IOListing):
        raise TypeError("IOExtension.ls() must return IOListing")

    files: list[tuple[str, int | None, str]] = []
    for file in result.files:
        if not isinstance(file, IOFileInfo):
            raise TypeError("IOListing.files must contain IOFileInfo values")
        files.append((file.path, file.size, file.file_type.value))
    return files, result.continuation_token, result.not_found_if_empty


async def _delete(extension: IOExtension, path: str) -> None:
    await extension.delete(path)
