from __future__ import annotations

import mimetypes
import shutil
import tempfile
import warnings
from typing import TYPE_CHECKING

from daft.daft import PyDaftFile, PyFileReference
from daft.datatype import MediaType
from daft.dependencies import av, h5py, pil_image, sf

BUFFER_SNIFF: int = 4 * 1024  # 4KB
BUFFER_METADATA: int = 64 * 1024  # 64KB
BUFFER_COPY: int = 1024 * 1024  # 1MB

if TYPE_CHECKING:
    from tempfile import _TemporaryFileWrapper

    from daft.file.audio import AudioFile
    from daft.file.hdf5 import Hdf5File
    from daft.file.image import ImageFile
    from daft.file.video import VideoFile
    from daft.io import IOConfig


class File:
    """A file-like object for working with file contents in Daft.

    This is an abstract base class that provides a standard file interface compatible
    with Python's file protocol.

    The File object can be used with most Python libraries that accept file-like objects,
    and implements the standard read/seek/tell interface. Files can also be opened for
    writing with `open(mode="wb")` or `open(mode="w")`.

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
        position: int | None = None,
        size: int | None = None,
        offset: int | None = None,
        length: int | None = None,
    ) -> None:
        if offset is not None:
            warnings.warn(
                "`offset` is deprecated; use `position` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if position is None:
                position = offset

        if length is not None:
            warnings.warn(
                "`length` is deprecated; use `size` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if size is None:
                size = length

        self._inner = PyFileReference._from_tuple((media_type._media_type, url, io_config, position, size))  # type: ignore

    def open(self, mode: str = "rb", *, buffer_size: int | None = None) -> PyDaftFile:
        """Open the file for reading or writing.

        Args:
            mode (str): Mode to open the file in. Supported modes:

                - "r" / "rb": binary read (default).
                - "wb": binary write; `write()` accepts bytes-like objects.
                - "w" / "wt": text write; `write()` accepts str, encoded as UTF-8.
                - "x" / "xt" / "xb": like the corresponding write mode, but raises
                  `FileExistsError` if the file already exists. The existence check
                  runs at open time; a concurrent writer between open and close can
                  still win the race on object stores.
            buffer_size (int | None): Read buffer size in bytes. Only valid for read modes.

        Writes are buffered in memory and only committed to storage with a single
        upload when the file is closed. If the `with` block exits with an exception,
        nothing is committed and the destination is left untouched.
        """
        if mode in ("r", "rb"):
            if self.position is None and self._inner.size() is None and not self.exists():
                raise FileNotFoundError(f"File {self.path} does not exist")
            return PyDaftFile._from_file_reference(self._inner, buffer_size=buffer_size)
        if mode in ("w", "wt", "wb", "x", "xt", "xb"):
            if buffer_size is not None:
                raise ValueError("buffer_size is only supported for read modes")
            if mode.startswith("x") and self.exists():
                raise FileExistsError(f"File {self.path} already exists")
            return PyDaftFile._create_writer(self._inner, text=not mode.endswith("b"))
        raise ValueError(
            f"Unsupported mode: {mode}. Supported modes are 'r', 'rb', 'w', 'wt', 'wb', 'x', 'xt', and 'xb'"
        )

    def write(self, data: bytes | bytearray | memoryview | str) -> int:
        """Writes `data` to the file in one call, replacing any existing content.

        `str` data is written as UTF-8 text; bytes-like data is written as binary.
        Returns the number of characters (text) or bytes (binary) written. Equivalent
        to opening in "w"/"wb" and writing once, so the same commit semantics apply:
        the content is only visible at the destination once the write completes.
        """
        mode = "w" if isinstance(data, str) else "wb"
        with self.open(mode) as f:
            return f.write(data)

    def __str__(self) -> str:
        return self._inner.__str__()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        # Byte-range files refuse write modes in open(), so mirror that here.
        return self._inner.position() is None and self._inner.size() is None

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
    def position(self) -> int | None:
        """The starting byte position for range reads, or None for full-file reads."""
        return self._inner.position()

    @property
    def offset(self) -> int | None:
        """Deprecated alias for `position`. The byte offset for range reads, or None for full-file reads."""
        warnings.warn(
            "`File.offset` is deprecated; use `File.position` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._inner.position()

    @property
    def length(self) -> int | None:
        """Deprecated alias for the byte-range read window size, or None for full-file reads.

        Note: this returns the requested range size (caller intent), not the derived file
        size. Use `File.size()` for the actual file size.
        """
        warnings.warn(
            "`File.length` is deprecated; use `File.size()` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._inner.size()

    def size(self) -> int:
        """The size of the file in bytes, derived from the underlying file."""
        return PyDaftFile._from_file_reference(self._inner, buffer_size=BUFFER_SNIFF).size()

    def exists(self) -> bool:
        """Whether the file exists at its path or URL."""
        return self._inner.exists()

    def mime_type(self) -> str:
        """Attempts to determine the MIME type of the file.

        If the MIME type is undetectable, returns 'application/octet-stream'.
        """
        try:
            with self.open(buffer_size=BUFFER_SNIFF) as f:
                maybe_mime_type = f.guess_mime_type()
                return maybe_mime_type if maybe_mime_type else "application/octet-stream"
        except FileNotFoundError:
            if self.path.lower().endswith((".h5", ".hdf5")):
                return "application/vnd.hdfgroup.hdf5"
            maybe_mime_type, _ = mimetypes.guess_type(self.path)
            return maybe_mime_type if maybe_mime_type else "application/octet-stream"

    def to_tempfile(self, buffer_size: int = BUFFER_COPY) -> _TemporaryFileWrapper[bytes]:
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
                shutil.copyfileobj(f, temp_file, length=buffer_size)  # Default buffer size is 1MB
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

    def is_image(self) -> bool:
        mimetype = self.mime_type()
        if mimetype.startswith("image/"):
            return True
        return False

    def is_hdf5(self) -> bool:
        mimetype = self.mime_type()
        return mimetype == "application/vnd.hdfgroup.hdf5"

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

    def as_image(self) -> ImageFile:
        """Convert to ImageFile if this file contains image data."""
        if not pil_image.module_available():
            raise ImportError(
                "The 'pillow' module is required to convert files to images. "
                "Please install it with: pip install 'daft[image]'"
            )
        from daft.file.image import ImageFile

        if not self.is_image():
            raise ValueError(f"File {self} is not an image file")

        cls = ImageFile.__new__(ImageFile)
        cls._inner = self._inner

        return cls

    def as_hdf5(self) -> Hdf5File:
        """Convert to Hdf5File if this file contains HDF5 data."""
        if not h5py.module_available():  # ty:ignore[unresolved-attribute]
            raise ImportError(
                "The 'h5py' module is required to convert files to HDF5. Please install it with: pip install 'h5py'"
            )
        from daft.file.hdf5 import Hdf5File

        if not self.is_hdf5():
            raise ValueError(f"File {self} is not an HDF5 file")

        cls = Hdf5File.__new__(Hdf5File)
        cls._inner = self._inner

        return cls
