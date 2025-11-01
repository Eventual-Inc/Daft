from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias
from urllib.parse import urlparse

import av

from daft.daft import FileInfos, ImageMode
from daft.datatype import DataType
from daft.filesystem import _infer_filesystem, glob_path_with_stats
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator
    from fractions import Fraction

    from av.video import VideoFrame

    from daft.daft import IOConfig
    from daft.io.pushdowns import Pushdowns


if TYPE_CHECKING:
    from daft.dependencies import np

    _VideoFrameData: TypeAlias = np.typing.NDArray[Any]
else:
    _VideoFrameData: TypeAlias = Any


@dataclass
class _VideoFrame:
    """Represents a single video frame.

    Note:
        The field name 'data' is required due to a casting bug.
        See: https://github.com/Eventual-Inc/Daft/issues/4872
    """

    path: str
    frame_index: int
    frame_time: float
    frame_time_base: Fraction
    frame_pts: int
    frame_dts: int | None
    frame_duration: int | None
    is_key_frame: bool
    data: _VideoFrameData


@dataclass
class _VideoFramesSource(DataSource):
    """DataSource for streaming video files into rows of images.

    Note:
        This API requires [PyAV](https://github.com/PyAV-Org/PyAV), please do `pip install av`.

    Attributes:
        paths (list[str]): Path(s) to the video file(s).
        image_height (int): Height to which each frame will be resized.
        image_width (int): Width to which each frame will be resized.
        is_key_frame (bool|None): If True, only include key frames; if False, only non-key frames; if None, include all frames.
        io_config (IOConfig|None): Optional IOConfig.
    """

    paths: list[str]
    image_height: int
    image_width: int
    is_key_frame: bool | None = None
    io_config: IOConfig | None = None

    @property
    def name(self) -> str:
        return "VideoFramesSource"

    @property
    def schema(self) -> Schema:
        return _schema(
            image_height=self.image_height,
            image_width=self.image_width,
        )

    def _list_file_infos(self) -> Generator[FileInfos]:
        for file_paths in self.paths:
            yield glob_path_with_stats(file_paths, file_format=None, io_config=self.io_config)

    def _list_file_paths(self) -> Generator[str]:
        all_youtube = all(_is_youtube_url(p) for p in self.paths)
        any_youtube = any(_is_youtube_url(p) for p in self.paths)
        if all_youtube:
            _assert_youtube_available()
            yield from self.paths
        elif any_youtube:
            raise ValueError("Either all or none of the paths must be YouTube URLs.")
        else:
            for file_infos in self._list_file_infos():
                yield from file_infos.file_paths

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        for path in self._list_file_paths():
            yield _VideoFramesSourceTask(
                path=path,
                image_height=self.image_height,
                image_width=self.image_width,
                is_key_frame=self.is_key_frame,
                io_config=self.io_config,
            )


@dataclass
class _VideoFramesSourceTask(DataSourceTask):
    """DataSourceTask which yields micropartitions of images from a video file."""

    path: str
    image_height: int
    image_width: int
    is_key_frame: bool | None
    io_config: IOConfig | None

    _max_partition_size = 10 * 1024 * 1024  # 10 MB

    @property
    def schema(self) -> Schema:
        return _schema(
            image_height=self.image_height,
            image_width=self.image_width,
        )

    def _list_frames(self, path: str, file: Any) -> Generator[_VideoFrame]:
        container = None
        try:
            container = av.open(file)

            # TODO support reading frames for multiple video streams
            stream = next((s for s in container.streams if s.type == "video"), None)
            if stream is None:
                container.close()
                raise RuntimeError(f"No video stream found in file: {path}")

            if self.is_key_frame:
                # https://pyav.org/docs/develop/cookbook/basics.html#saving-keyframes
                stream.codec_context.skip_frame = "NONKEY"

            frame_index: int = 0
            frame: VideoFrame
            while True:
                try:
                    frame = next(container.decode(stream))
                except av.EOFError:
                    break
                except StopIteration:
                    break

                frame = frame.reformat(
                    width=self.image_width,
                    height=self.image_height,
                )

                yield _VideoFrame(
                    path=path,
                    frame_index=frame_index,
                    frame_time=frame.time,
                    frame_time_base=frame.time_base,
                    frame_pts=frame.pts,
                    frame_dts=frame.dts,
                    frame_duration=frame.duration,
                    is_key_frame=frame.key_frame,
                    data=frame.to_ndarray(format="rgb24"),
                )
                frame_index += 1
        finally:
            if container:
                container.close()

    def _open(self) -> Any:
        if _is_youtube_url(self.path):
            return self._open_youtube_file()
        else:
            fp, fs, _ = _infer_filesystem(self.path, io_config=self.io_config)
            return fs.open_input_file(fp)

    @contextmanager
    def _open_youtube_file(self) -> Any:
        import yt_dlp

        def selector(ctx):  # type: ignore
            best_fmt = None
            best_fit = float("inf")  # lower is better

            for fmt in ctx["formats"]:
                if fmt.get("ext") != "mp4":
                    continue

                h, w = fmt.get("height"), fmt.get("width")
                if h is None or w is None:
                    continue

                fit = abs(h - self.image_height) + abs(w - self.image_width)
                if fit == 0:
                    yield fmt
                    return
                if fit < best_fit:
                    best_fmt = fmt
                    best_fit = fit

            yield best_fmt

        # Note:
        #   Streaming youtube downloads requires deeper work and was error-prone.
        #   The parsed youtube urls use m3u8 which requires decoding which ydl
        #   will handle for us. We cannot reliable pass a file-like HTTP response
        #   to PyAV; hence why this will download to a tempfile then open the file.

        temp_file = next(tempfile._get_candidate_names())  # type: ignore

        params = {
            "format": selector,
            "quiet": True,
            "outtmpl": temp_file,
            "no_warnings": True,
            "extract_flat": False,
            "no_check_certificate": True,
            "ignoreerrors": False,
            "consoletitle": False,
            "noprogress": True,
        }

        try:
            with yt_dlp.YoutubeDL(params) as ydl:
                ydl.download([self.path])
            yield open(temp_file, mode="rb")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        with self._open() as file:
            buffer = _VideoFramesBuffer(
                image_height=self.image_height,
                image_width=self.image_width,
            )
            for frame in self._list_frames(self.path, file):
                buffer.append(frame)
                # yield when full
                if buffer.size() >= self._max_partition_size:
                    yield buffer.to_micropartition()
                    buffer.clear()
            # yield if non-empty
            if buffer and buffer.size() > 0:
                yield buffer.to_micropartition()


class _VideoFramesBuffer:
    """A micropartition buffer/builder for video frames.

    Note:
        This enables decoupling the video source from a particular
        library, making it possible for the VideoSource to leverage
        an Iterable[_VideoFrame[T]] at some later time. How the iterator
        is implemented e.g. open-cv vs. PyAV or other library is not
        important, just that we have an Iterable of frames which this
        builder and the source and stream as appropriately sized partitions.
    """

    image_height: int
    image_width: int

    _arr_path: list[str]
    _arr_frame_index: list[int]
    _arr_frame_time: list[float]
    _arr_frame_time_base: list[str]
    _arr_frame_pts: list[int]
    _arr_frame_dts: list[int | None]
    _arr_frame_duration: list[int | None]
    _arr_is_key_frame: list[bool]
    _arr_data: list[_VideoFrameData]
    _size_in_bytes: int
    _size_of_metadata = 64

    def __init__(self, image_height: int, image_width: int):
        self.image_height = image_height
        self.image_width = image_width
        self.clear()

    def clear(self) -> None:
        self._arr_path = []
        self._arr_frame_index = []
        self._arr_frame_time = []
        self._arr_frame_time_base = []
        self._arr_frame_pts = []
        self._arr_frame_dts = []
        self._arr_frame_duration = []
        self._arr_is_key_frame = []
        self._arr_data = []
        self._size_in_bytes = 0

    def size(self) -> int:
        """Returns the current size of the partition in bytes."""
        return self._size_in_bytes

    def append(self, frame: _VideoFrame) -> None:
        """Appends the frame to this partition builder.

        Note:
            We encode the time_base as a fraction string.
            See: https://github.com/Eventual-Inc/Daft/issues/4971
        """
        self._arr_path.append(frame.path)
        self._arr_frame_index.append(frame.frame_index)
        self._arr_frame_time.append(frame.frame_time)
        self._arr_frame_time_base.append(str(frame.frame_time_base))
        self._arr_frame_pts.append(frame.frame_pts)
        self._arr_frame_dts.append(frame.frame_dts)
        self._arr_frame_duration.append(frame.frame_duration)
        self._arr_is_key_frame.append(frame.is_key_frame)
        self._arr_data.append(frame.data)
        self._size_in_bytes += frame.data.nbytes + self._size_of_metadata

    def to_micropartition(self) -> MicroPartition:
        """Returns a MicroPartition for this builder."""
        return MicroPartition.from_pydict(
            {
                "path": self._arr_path,
                "frame_index": self._arr_frame_index,
                "frame_time": self._arr_frame_time,
                "frame_time_base": self._arr_frame_time_base,
                "frame_pts": self._arr_frame_pts,
                "frame_dts": self._arr_frame_dts,
                "frame_duration": self._arr_frame_duration,
                "is_key_frame": self._arr_is_key_frame,
                "data": self._arr_data,
            }
        )


def _schema(image_height: int, image_width: int) -> Schema:
    """Returns the common schema which is needed in several places."""
    return Schema.from_pydict(
        {
            "path": DataType.string(),
            "frame_index": DataType.int64(),
            "frame_time": DataType.float64(),
            "frame_time_base": DataType.string(),
            "frame_pts": DataType.int64(),
            "frame_dts": DataType.int64(),
            "frame_duration": DataType.int64(),
            "is_key_frame": DataType.bool(),
            "data": DataType.image(
                height=image_height,
                width=image_width,
                mode=ImageMode.RGB,  # make configurable at later time
            ),
        }
    )


def _is_youtube_url(url: str) -> bool:
    hostname = urlparse(url).hostname
    if hostname is None:
        return False
    return hostname in {"www.youtube.com", "youtube.com", "youtu.be"}


def _assert_youtube_available() -> None:
    try:
        import yt_dlp  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "YouTube video support requires the 'yt-dlp' package. Please install it with `pip install yt-dlp`."
        ) from e
