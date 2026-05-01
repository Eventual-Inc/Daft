from __future__ import annotations

import io
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from daft.daft import PyDaftFile, PyFileReference, io_put
from daft.datatype import MediaType
from daft.dependencies import av, sf

if TYPE_CHECKING:
    from tempfile import _TemporaryFileWrapper

    from daft.file.audio import AudioFile
    from daft.file.video import VideoFile
    from daft.io import IOConfig


class _DaftWritableFile:
    def __init__(self, path: str, io_config: IOConfig | None, mode: str, encoding: str) -> None:
        self.path = path
        self._io_config = io_config
        self._mode = mode
        self._encoding = encoding
        self._closed = False
        self._buffer = io.BytesIO()

    def __enter__(self) -> _DaftWritableFile:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        if exc_type is not None:
            self._buffer.close()
            self._closed = True
            return
        self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return not self._closed

    def seekable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def write(self, data: str | bytes | bytearray | memoryview) -> int:
        if self._closed:
            raise ValueError("Cannot write to closed file")

        if "b" in self._mode:
            if isinstance(data, str):
                raise TypeError("write() argument must be bytes-like in binary mode")
            payload = bytes(data)
            self._buffer.write(payload)
            return len(payload)

        if not isinstance(data, str):
            raise TypeError("write() argument must be str in text mode")
        payload = data.encode(self._encoding)
        self._buffer.write(payload)
        return len(data)

    def tell(self) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._buffer.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._buffer.seek(offset, whence)

    def truncate(self, size: int | None = None) -> int:
        if self._closed:
            raise ValueError("Cannot truncate closed file")
        if size is None:
            size = self._buffer.tell()
        return self._buffer.truncate(size)

    def flush(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        io_put(path=self.path, data=self._buffer.getvalue(), multithreaded_io=True, io_config=self._io_config)

    def close(self) -> None:
        if self._closed:
            return
        try:
            io_put(path=self.path, data=self._buffer.getvalue(), multithreaded_io=True, io_config=self._io_config)
        finally:
            self._buffer.close()
            self._closed = True


class _DaftReadableTextFile:
    def __init__(self, file_ref: PyFileReference, encoding: str) -> None:
        self._inner = PyDaftFile._from_file_reference(file_ref)
        self._encoding = encoding

    def __enter__(self) -> _DaftReadableTextFile:
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self._inner.__exit__(exc_type, exc_val, exc_tb)

    @property
    def closed(self) -> bool:
        return self._inner.closed()

    def read(self, size: int = -1) -> str:
        data = self._inner.read(size)
        if isinstance(data, str):
            return data
        return data.decode(self._encoding)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def close(self) -> None:
        self._inner.close()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False


class File:
    """A file-like object for working with file contents in Daft.

    This is an abstract base class that provides a standard file interface compatible
    with Python's file protocol.

    The File object can be used with most Python libraries that accept file-like objects,
    and implements standard file-style interfaces for reads and writes.

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

    def __init__(
        self,
        url: str,
        io_config: IOConfig | None = None,
        media_type: MediaType = MediaType.unknown(),
        offset: int | None = None,
        length: int | None = None,
    ) -> None:
        self._inner = PyFileReference._from_tuple((media_type._media_type, url, io_config, offset, length))  # type: ignore

    @overload
    def open(self, mode: Literal["rb"] = "rb", encoding: str = "utf-8") -> PyDaftFile: ...

    @overload
    def open(self, mode: Literal["r", "rt"], encoding: str = "utf-8") -> _DaftReadableTextFile: ...

    @overload
    def open(self, mode: Literal["w", "wt", "wb"], encoding: str = "utf-8") -> _DaftWritableFile: ...

    def open(self, mode: str = "rb", encoding: str = "utf-8") -> PyDaftFile | _DaftReadableTextFile | _DaftWritableFile:
        if mode == "rb":
            return PyDaftFile._from_file_reference(self._inner)
        if mode in ("r", "rt"):
            return _DaftReadableTextFile(file_ref=self._inner, encoding=encoding)
        if mode in ("w", "wt", "wb"):
            inner = cast("Any", self._inner)
            _, path, io_config = inner._get_file()
            return _DaftWritableFile(path=path, io_config=io_config, mode=mode, encoding=encoding)
        raise ValueError(f"Unsupported mode: {mode}. Supported modes are 'r', 'rt', 'rb', 'w', 'wt', and 'wb'")

    def __str__(self) -> str:
        return self._inner.__str__()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    @property
    def path(self) -> str:
        """The full path or URL of the file.

        Returns:
            str: The file path or URL.

        Example:
            >>> import daft
            >>> f = daft.File("s3://bucket/path/to/data.csv")
            >>> f.path
            's3://bucket/path/to/data.csv'
        """
        return self._inner.path()

    @property
    def name(self) -> str:
        """The filename (basename) extracted from the file path or URL.

        Returns:
            str: The filename without directory components.

        Example:
            >>> import daft
            >>> f = daft.File("s3://bucket/path/to/data.csv")
            >>> f.name
            'data.csv'
        """
        return self._inner.name()

    @property
    def offset(self) -> int | None:
        """The byte offset for range reads, or None for full-file reads."""
        return self._inner.offset()

    @property
    def length(self) -> int | None:
        """The byte length for range reads, or None for full-file reads."""
        return self._inner.length()

    def size(self) -> int:
        return PyDaftFile._from_file_reference(self._inner).size()

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

    def is_audio(self) -> bool:
        mimetype = self.mime_type()
        if mimetype.startswith("audio/"):
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

    def as_audio(self) -> AudioFile:
        """Convert to AudioFile if this file contains audio data."""
        if not sf.module_available():
            raise ImportError(
                "The 'soundfile' module is required to convert files to audio. "
                "Please install it with: pip install 'daft[audio]'"
            )
        # this is purposely inside the function, and after the `sf` check
        # because using AudioFile means that the user has `sf` installed
        from daft.file.audio import AudioFile

        if not self.is_audio():
            raise ValueError(f"File {self} is not an audio file")

        cls = AudioFile.__new__(AudioFile)
        cls._inner = self._inner

        return cls
