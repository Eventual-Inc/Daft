from __future__ import annotations

from daft.io.video.video_source import VideoSource


def test_video_tasks():
    source = VideoSource(
        path="/Users/rch/Desktop/assets/zoo.mp4",
        image_height=200,
        image_width=200,
    )
    df = source.read()
    df.show()
