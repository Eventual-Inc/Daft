from __future__ import annotations

import daft

df = daft.read_video_frames(
    path="https://www.youtube.com/watch?v=jNQXAC9IVRw",
    image_height=480,
    image_width=640,
)
df.show()
