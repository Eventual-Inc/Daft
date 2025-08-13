from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import cv2

from daft.datatype import DataType, ImageMode
from daft.io import DataSource, DataSourceTask, IOConfig
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

    from daft.io.pushdowns import Pushdowns


@dataclass
class VideoSource(DataSource):
    """DataSource for streaming video files into rows of images.

    Attributes:
        path (str|list[str]): Path(s) to the video file(s).
        image_height (int): Height to which each frame will be resized.
        image_width (int): Width to which each frame will be resized.
        io_config (IOConfig|None): Optional IOConfig.
    """

    path: str | list[str]
    image_height: int
    image_width: int
    io_config: IOConfig | None = None

    @property
    def name(self) -> str:
        return "VideoSource"

    @property
    def schema(self) -> Schema:
        return _schema(
            image_height=self.image_height,
            image_width=self.image_width,
        )

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:

        paths: list[str] = [self.path] if isinstance(self.path, str) else self.path

        for path in paths:
            yield VideoSourceTask(
                path=path,
                image_height=self.image_height,
                image_width=self.image_width,
                io_config=self.io_config,
            )


@dataclass
class VideoSourceTask(DataSourceTask):
    """DataSourceTask which yields micropartitions of images from a video file."""

    path: str
    image_height: int
    image_width: int
    io_config: IOConfig | None = None

    _max_partition_size = 10 * 1024 * 1024 # 10 MB

    @property
    def schema(self) -> Schema:
        return _schema(
            image_height=self.image_height,
            image_width=self.image_width,
        )

    def _new_partition(self) -> _VideoPartition:
        return _VideoPartition(
            image_height=self.image_height,
            image_width=self.image_width,
        )

    def get_micro_partitions(self) -> Iterator[MicroPartition]:

        # TODO switch to PyAV for file-like objects.

        frames = _frames(
            path=self.path,
            image_height=self.image_height,
            image_width=self.image_width,
        )
        curr_partition = self._new_partition()
        for frame in frames:
            curr_partition.append(frame)
            if curr_partition.size() >= self._max_partition_size:
                yield curr_partition.to_micropartition()
                curr_partition = self._new_partition()
        if curr_partition and curr_partition.size() > 0:
            yield curr_partition.to_micropartition()


@dataclass
class _VideoFrame:
    """Represents a single video frame.

    Note:
        The field name 'data' is required due to a casting bug.
        See: https://github.com/Eventual-Inc/Daft/issues/4872

        This could also be _VideoFrame(Generic[T]) to accomodate
        other video libraries, but for now we just use open-cv.
        This implementation may graduate to PyAV if/when needed.
    """

    path: str
    frame_number: int
    frame_timestamp_ms: int
    data: cv2.UMat


class _VideoPartition:
    """A micropartition builder for video frames.

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
    _arr_frame_number: list[int]
    _arr_frame_timestamp_ms: list[int]
    _arr_data: list[cv2.UMat]
    _size_in_bytes: int
    _size_of_metad = 64

    def __init__(self, image_height: int, image_width: int):
        self.image_height = image_height
        self.image_width = image_width
        self._arr_path = []
        self._arr_frame_number = []
        self._arr_frame_timestamp_ms = []
        self._arr_data = []
        self._size_in_bytes = 0

    def size(self) -> int:
        """Returns the current size of the partition in bytes."""
        return self._size_in_bytes

    def append(self, frame: _VideoFrame):
        """Appends the frame to this partition builder."""
        self._arr_path.append(frame.path)
        self._arr_frame_number.append(frame.frame_number)
        self._arr_frame_timestamp_ms.append(frame.frame_timestamp_ms)
        self._arr_data.append(frame.data)
        self._size_in_bytes += frame.data.nbytes + self._size_of_metad # type: ignore

    def to_micropartition(self) -> MicroPartition:
        """Returns a MicroPartition for this builder."""
        return MicroPartition.from_pydict(
            {
                "path": self._arr_path,
                "frame_number": self._arr_frame_number,
                "frame_timestamp_ms": self._arr_frame_timestamp_ms,
                "data": self._arr_data,
            }
        )


def _schema(image_height: int, image_width: int) -> Schema:
    """Returns the common schema which is needed in several places."""
    return Schema.from_pydict(
        {
            "path": DataType.string(),
            "frame_number": DataType.int64(),
            "frame_timestamp_ms": DataType.int64(),
            "data": DataType.image(
                height=image_height,
                width=image_width,
                mode=ImageMode.RGB,  # make configurable at later time
            ),
        }
    )


def _frames(path: str, image_height: int, image_width: int) -> Generator[_VideoFrame]:
    """An open-cv backed _VideoFrame generator."""
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        return
    try:
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = 0
        while True:
            curr_pos_ms = cap.get(cv2.CAP_PROP_POS_MSEC) # current timestamp
            ret, frame = cap.read()
            if not ret:
                break
            try:
                if curr_pos_ms > 0:
                    timestamp_ms = int(curr_pos_ms)
                else:
                    timestamp_ms = (int(frame_count * (1000.0 / fps)) if fps > 0 else 0)
                frame_rgb = frame # already RGB
                frame_resized = cv2.resize(frame_rgb, (image_width, image_height), interpolation=cv2.INTER_AREA)
                yield _VideoFrame(
                    path=path,
                    frame_number=frame_count,
                    frame_timestamp_ms=timestamp_ms,
                    data=frame_resized,
                )
                frame_count += 1
            except Exception:
                # skip frame
                frame_count += 1
                continue
    finally:
        cap.release()
