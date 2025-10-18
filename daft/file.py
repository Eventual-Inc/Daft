from __future__ import annotations

import shutil
import tempfile
from typing import TYPE_CHECKING

from daft.daft import PyDaftFile, PyFileReference
from daft.dependencies import av

if TYPE_CHECKING:
    from collections.abc import Iterator
    from tempfile import _TemporaryFileWrapper

    import PIL

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

    _inner: PyFileReference

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> File:
        instance = File.__new__(File)
        instance._inner = reference
        return instance

    def __init__(self, str_or_bytes: str | bytes, io_config: IOConfig | None = None) -> None:
        if isinstance(str_or_bytes, str):
            self._inner = PyFileReference._from_tuple((str_or_bytes, io_config))  # type: ignore
        elif isinstance(str_or_bytes, bytes):
            self._inner = PyFileReference._from_tuple((str_or_bytes, io_config))  # type: ignore
        else:
            raise TypeError("str_or_bytes must be a string or bytes")

    def open(self) -> PyDaftFile:
        return PyDaftFile._from_file_reference(self._inner)

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
        return PyDaftFile._from_file_reference(self._inner).size()

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
        # Check magic bytes - most are in first 16 bytes
        try:
            with self.open() as f:
                header = f.read(16)

                if header.startswith(b"\x00\x00\x00"):  # MP4 variants
                    return b"ftyp" in header
                if header.startswith(b"\x1a\x45\xdf\xa3"):  # Matroska/MKV
                    return True
                if header.startswith(b"RIFF"):  # AVI
                    return header[8:12] == b"AVI "
                if header.startswith(b"FLV"):  # FLV
                    return True

                return False

        except Exception:
            return False

    class VideoFile:
        """A video-specific file interface that provides video operations."""

        def __init__(self, file: File):
            self.file = file

        def keyframes(self, start_time: float = 0, end_time: float | None = None) -> Iterator[PIL.Image.Image]:
            """Lazy iterator of keyframes as PIL Images within time range."""
            with self.file.open() as f:
                container = av.open(f)
                stream = container.streams.video[0]

                # Seek to start time
                if start_time > 0:
                    seek_timestamp = int(start_time * av.time_base)
                    container.seek(seek_timestamp)

                for frame in container.decode(stream):
                    if not frame.key_frame:
                        continue

                    # Check end time if specified
                    if end_time is not None:
                        frame_time = frame.time
                        if frame_time and frame_time > end_time:
                            break

                    yield frame.to_image()

                container.close()

    def as_video(self) -> VideoFile:
        """Convert to VideoFile if this file contains video data."""
        if not av.module_available():
            raise ImportError("The 'av' module is required to convert files to video.")

        if not self.is_video():
            raise ValueError(f"File {self} is not a video file")

        return File.VideoFile(self)
