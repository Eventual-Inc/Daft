from __future__ import annotations

import shutil
import tempfile
from typing import TYPE_CHECKING

from daft.daft import PyDaftFile, PyFileReference, PyMediaType
from daft.datatype import MediaType
from daft.dependencies import av

if TYPE_CHECKING:
    from tempfile import _TemporaryFileWrapper

    from daft.file.video import VideoFile
    from daft.io import IOConfig


class File:
    """A file-like object for working with file contents in Daft.

    This is an abstract base class that provides a standard file interface compatible
    with Python's file protocol.

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
        >>>     with file.open() as f:
        >>>         data = json.load(f)
        >>>         return data["text"]
    """

    _inner: PyFileReference | None = None
    _loaded_bytes: bytes | None = None

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> File:
        instance = File.__new__(File)
        instance._inner = reference
        return instance

    @staticmethod
    def _from_bytes(b: bytes) -> File:
        """Internal method. DO NOT USE DIRECTLY.

        Creating a file directly from bytes is deprecated and will eventually be phased out.
        """
        instance = File.__new__(File)
        instance._loaded_bytes = b
        return instance

    def __init__(
        self, url: str, io_config: IOConfig | None = None, media_type: MediaType = MediaType.unknown()
    ) -> None:
        self._inner = PyFileReference._from_tuple((media_type._media_type, url, io_config))  # type: ignore

    def open(self) -> PyDaftFile:
        if self._loaded_bytes is not None:
            return PyDaftFile._from_bytes(PyMediaType.unknown(), self._loaded_bytes)
        if self._inner is not None:
            return PyDaftFile._from_file_reference(self._inner)
        raise ValueError("File must either contain bytes or a file reference")

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

    def size(self) -> int:
        if self._loaded_bytes is not None:
            return len(self._loaded_bytes)
        if self._inner is not None:
            return PyDaftFile._from_file_reference(self._inner).size()

        raise ValueError("File must either contain bytes or a file reference")

    def mime_type(self) -> str:
        """Attempts to determine the MIME type of the file.

        If the MIME type is undetectable, returns 'application/octet-stream'.
        """
        with self.open() as f:
            maybe_mime_type = f.guess_mime_type()
            return maybe_mime_type if maybe_mime_type else "application/octet-stream"

    def to_tempfile(self) -> _TemporaryFileWrapper[bytes]:
        """Create a temporary file with the contents of this file.

        Returns:
            _TemporaryFileWrapper[bytes]: The temporary file object.

        The temporary file will be automatically deleted when the returned context manager is closed.

        It's important to note that `to_tempfile` closes the original file object, so it CANNOT be used after calling this method.

        Example:
            >>> with file.to_tempfile() as temp_path:
            >>> # Do something with the temporary file
            >>>     pass
        """
        with self.open() as f:
            temp_file = tempfile.NamedTemporaryFile(
                prefix="daft_",
            )
            f.seek(0)

            size = f.size()
            # if its either a really small file, or doesn't support range requests. Just read it normally
            if not f._supports_range_requests() or size < 1024:
                temp_file.write(f.read())
            else:
                shutil.copyfileobj(f, temp_file, length=size)
            # close it as `to_tempfile` is a consuming method
            f.close()
            temp_file.seek(0)

            return temp_file

    def is_video(self) -> bool:
        mimetype = self.mime_type()
        if mimetype.startswith("video/"):
            return True
        return False

    def as_video(self) -> VideoFile:
        """Convert to VideoFile if this file contains video data."""
        if not av.module_available():
            raise ImportError("The 'av' module is required to convert files to video.")
        # this is purposely inside the function, and after the `av` check
        # because using VideoFile means that the user has `av` installed
        from daft.file.video import VideoFile

        if not self.is_video():
            raise ValueError(f"File {self} is not a video file")

        cls = VideoFile.__new__(VideoFile)
        cls._inner = self._inner

        return cls
