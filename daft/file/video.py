from __future__ import annotations

from typing import TYPE_CHECKING

from daft.datatype import MediaType
from daft.dependencies import av
from daft.file import File
from daft.file.typing import VideoMetadata

if TYPE_CHECKING:
    from collections.abc import Iterator

    import PIL

    from daft.daft import PyFileReference
    from daft.io import IOConfig


class VideoFile(File):
    """A video-specific file interface that provides video operations."""

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> VideoFile:
        instance = VideoFile.__new__(VideoFile)
        instance._inner = reference
        return instance

    def __init__(self, url: str, io_config: IOConfig | None = None) -> None:
        if not av.module_available():
            raise ImportError("The 'av' module is required to create video files.")
        super().__init__(url, io_config, MediaType.video())

    def __post_init__(self) -> None:
        if not self.is_video():
            raise ValueError(f"File {self} is not a video file")

    def metadata(self) -> VideoMetadata:
        """Extract basic video metadata from container headers.

        Returns:
            VideoMetadata: Video metadata object containing width, height, fps, frame_count, time_base, keyframe_pts, keyframe_indices

        """
        with self.open() as f:
            with av.open(f, mode="r", metadata_encoding="utf-8") as container:
                video = next(
                    (stream for stream in container.streams if stream.type == "video"),
                    None,
                )
                if video is None:
                    return VideoMetadata(
                        width=None,
                        height=None,
                        fps=None,
                        duration=None,
                        frame_count=None,
                        time_base=None,
                    )

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

                return VideoMetadata(
                    width=width,
                    height=height,
                    fps=fps,
                    duration=duration,
                    frame_count=frame_count,
                    time_base=time_base,
                )

    def keyframes(self, start_time: float = 0, end_time: float | None = None) -> Iterator[PIL.Image.Image]:
        """Lazy iterator of keyframes as PIL Images within time range."""
        with self.open() as f:
            with av.open(f) as container:
                video = next(
                    (stream for stream in container.streams if stream.type == "video"),
                    None,
                )
                if video is None:
                    raise ValueError("No video stream found")
                # Seek to start time
                if start_time > 0:
                    seek_timestamp = int(start_time * video.time_base)
                    container.seek(seek_timestamp)

                # skip non keyframes
                video.codec_context.skip_frame = "NONKEY"
                for frame in container.decode(video):
                    # Check end time if specified
                    if end_time is not None:
                        frame_time = frame.time
                        if frame_time and frame_time > end_time:
                            break

                    yield frame.to_image()
