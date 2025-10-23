from __future__ import annotations

import shutil
import tempfile
from typing import TYPE_CHECKING

from daft.daft import PyDaftFile, PyFileReference
from daft.dependencies import av

if TYPE_CHECKING:
    from collections.abc import Iterator
    from tempfile import _TemporaryFileWrapper
    from typing import Any

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

        if not self.is_video():
            raise ValueError(f"File {self} is not a video file")

        cls = VideoFile.__new__(VideoFile)
        cls._inner = self._inner

        return cls


class VideoFile(File):
    """A video-specific file interface that provides video operations."""

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> VideoFile:
        instance = VideoFile.__new__(VideoFile)
        instance._inner = reference
        return instance

    def __init__(self, str_or_bytes: str | bytes, io_config: IOConfig | None = None) -> None:
        if not av.module_available():
            raise ImportError("The 'av' module is required to create video files.")
        super().__init__(str_or_bytes, io_config)

    def __post_init__(self) -> None:
        if not self.is_video():
            raise ValueError(f"File {self} is not a video file")

    def metadata(
        self,
        *,
        probesize: str = "64k",
        analyzeduration_us: int = 200_000,
    ) -> dict[str, Any]:
        """Extract basic video metadata from container headers.

        Returns:
        -------
        dict
            width, height, fps, frame_count, time_base, keyframe_pts, keyframe_indices
        """
        options = {
            "probesize": str(probesize),
            "analyzeduration": str(analyzeduration_us),
        }
        with self.open() as f:

            with av.open(f, mode="r", options=options, metadata_encoding="utf-8") as container:
                video = next(
                    (stream for stream in container.streams if stream.type == "video"),
                    None,
                )
                if video is None:
                    return {
                        "width": None,
                        "height": None,
                        "fps": None,
                        "frame_count": None,
                        "time_base": None,
                    }
    
                # Basic stream properties ----------
                width = video.width
                height = video.height
                time_base = float(video.time_base) if video.time_base else None
    
                # Frame rate -----------------------
                fps = None
                if video.average_rate:
                    fps = float(video.average_rate)
                elif video.guessed_rate:
                    fps = float(video.guessed_rate)
    
                # Duration -------------------------
                duration = None
                if container.duration and container.duration > 0:
                    duration = container.duration / 1_000_000.0
                elif video.duration:
                    # Fallback time_base only for duration computation if missing
                    tb_for_dur = float(video.time_base) if video.time_base else (1.0 / 1_000_000.0)
                    duration = float(video.duration * tb_for_dur)
    
                # Frame count -----------------------
                frame_count = video.frames
                if not frame_count or frame_count <= 0:
                    if duration and fps:
                        frame_count = int(round(duration * fps))
                    else:
                        frame_count = None
    
                return {
                    "width": width,
                    "height": height,
                    "fps": fps,
                    "duration": duration,
                    "frame_count": frame_count,
                    "time_base": time_base,
                }

    def keyframes(self, start_time: float = 0, end_time: float | None = None) -> Iterator[PIL.Image.Image]:
        """Lazy iterator of keyframes as PIL Images within time range."""
        with self.open() as f:
            container = av.open(f)
            video = next(
                (stream for stream in container.streams if stream.type == "video"),
                None,
            )
            if video is None:
                raise ValueError("No video stream found")

            # Seek to start time
            if start_time > 0:
                seek_timestamp = int(start_time * av.time_base)
                container.seek(seek_timestamp)

            for frame in container.decode(video):
                if not frame.key_frame:
                    continue

                # Check end time if specified
                if end_time is not None:
                    frame_time = frame.time
                    if frame_time and frame_time > end_time:
                        break

                yield frame.to_image()

            container.close()
