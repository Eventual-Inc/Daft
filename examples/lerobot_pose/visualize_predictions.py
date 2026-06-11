# /// script
# description = "Overlay predicted vs ground-truth 48-D hand poses on EgoDex video frames"
# requires-python = ">=3.12, <3.13"
# dependencies = [
#     "daft>=0.7.15",
#     "av",
#     "pillow",
#     "numpy",
# ]
# ///
"""Project predicted and ground-truth hand poses onto the camera frames.

The 48-D vectors are 3D *world-frame* points (per hand: wrist position, wrist
6D rotation, 5 fingertip positions — see H_RDT's precompute_48d_actions.py).
To draw them on a frame we follow Apple's reference visualizer
(ml-egodex/visualize_2d.py):

  1. world -> camera: multiply by the inverse of that frame's camera pose
     (`observation.extrinsics`, a 4x4 matrix).
  2. camera -> pixels: pinhole projection u = fx*X/Z + cx, v = fy*Y/Z + cy,
     with EgoDex's constant intrinsics (fx = fy = 736.6339, cx = 960, cy = 540).

Ground truth is drawn in green, the model's prediction in red. Note Apple's
caveat: Vision Pro video is synthesized from multiple cameras, so even
ground-truth reprojections can be a few pixels off the visible hands.

Run after predict_hrdt.py:

    uv run visualize_predictions.py
"""

import os

import lerobot  # vendored copy of daft.datasets.lerobot
import numpy as np
from PIL import Image, ImageDraw

import daft
from daft import col

DATASET_URI = "pepijn223/egodex-test"
PREDICTIONS_DIR = os.path.join(os.path.dirname(__file__), "out", "egodex_hrdt_predictions")
OVERLAYS_DIR = os.path.join(os.path.dirname(__file__), "out", "overlays")

# EgoDex camera intrinsics (constant across the dataset, from apple/ml-egodex).
FX = FY = 736.6339
CX, CY = 960.0, 540.0

GROUND_TRUTH_COLOR = (0, 220, 0)  # green
PREDICTION_COLOR = (255, 40, 40)  # red

# Playback speed of the stitched per-episode mp4s. The dataset is 30 fps, so
# 15 fps plays at half speed (e.g. 30 predicted frames -> a 2-second video).
VIDEO_FPS = int(os.environ.get("VIDEO_FPS", "15"))


def hand_points(vec48: np.ndarray, side: int) -> np.ndarray:
    """Extract the 6 drawable 3D points of one hand (wrist + 5 fingertips).

    Layout per hand (24 dims): [0:3] wrist xyz, [3:9] wrist 6D rotation
    (not drawable as a point, skipped), [9:24] thumb/index/middle/ring/little
    fingertip xyz. side: 0 = left hand (dims 0-23), 1 = right hand (24-47).
    """
    base = side * 24
    wrist = vec48[base : base + 3]
    fingertips = vec48[base + 9 : base + 24].reshape(5, 3)
    return np.vstack([wrist, fingertips])  # (6, 3): wrist first


def project_to_pixels(points_world: np.ndarray, extrinsics: np.ndarray) -> np.ndarray:
    """World-frame 3D points -> (u, v) pixel coordinates (NaN if behind camera)."""
    cam_from_world = np.linalg.inv(extrinsics)
    homogeneous = np.hstack([points_world, np.ones((len(points_world), 1))])  # (N, 4)
    in_camera = (cam_from_world @ homogeneous.T).T[:, :3]
    x, y, z = in_camera[:, 0], in_camera[:, 1], in_camera[:, 2]
    with np.errstate(divide="ignore", invalid="ignore"):
        u = FX * x / z + CX
        v = FY * y / z + CY
    uv = np.stack([u, v], axis=1)
    uv[z <= 0] = np.nan  # behind the camera
    return uv


def write_episode_video(episode: int, frames: list[tuple[int, np.ndarray]], fps: int) -> None:
    """Encode one episode's overlay frames (sorted by frame_index) into an mp4.

    Uses PyAV, which bundles its own FFmpeg — no ffmpeg binary required.
    """
    import av

    frames = sorted(frames, key=lambda pair: pair[0])
    out_path = os.path.join(OVERLAYS_DIR, f"episode{episode:03d}.mp4")
    height, width = frames[0][1].shape[:2]
    container = av.open(out_path, mode="w")
    stream = container.add_stream("h264", rate=fps)
    stream.width, stream.height = width, height
    stream.pix_fmt = "yuv420p"
    for _, image_array in frames:
        for packet in stream.encode(av.VideoFrame.from_ndarray(image_array, format="rgb24")):
            container.mux(packet)
    for packet in stream.encode():  # flush buffered frames
        container.mux(packet)
    container.close()
    print(f"Saved {out_path} ({len(frames)} frames @ {fps} fps = {len(frames) / fps:.2f}s)")


def draw_skeleton(draw: ImageDraw.ImageDraw, vec48: np.ndarray, extrinsics: np.ndarray, color: tuple) -> None:
    """Draw both hands of one 48-D pose: wrist dot + lines fanning to fingertips."""
    for side in (0, 1):
        uv = project_to_pixels(hand_points(vec48, side), extrinsics)
        if np.isnan(uv).any():
            continue
        wrist, fingertips = uv[0], uv[1:]
        for tip in fingertips:
            draw.line([tuple(wrist), tuple(tip)], fill=color, width=3)
            draw.ellipse([tip[0] - 7, tip[1] - 7, tip[0] + 7, tip[1] + 7], fill=color)
        draw.ellipse([wrist[0] - 11, wrist[1] - 11, wrist[0] + 11, wrist[1] + 11], fill=color)


if __name__ == "__main__":
    # 1. Load the predictions and note which frames they belong to.
    preds = daft.read_parquet(f"{PREDICTIONS_DIR}/**").select(
        "episode_index", "frame_index", "ground_truth_action", "predicted_action"
    )
    pred_rows = preds.to_pydict()
    episodes = sorted(set(pred_rows["episode_index"]))
    frames = sorted(set(pred_rows["frame_index"]))
    print(f"Found {len(pred_rows['frame_index'])} predicted frames: episodes {episodes}, frames {frames}")

    # 2. Re-read just those frames from the dataset, keeping the image and the
    #    per-frame camera pose. The .where filter pushes down past the video
    #    decoder, so only the frames we need are decoded.
    frames_df = (
        lerobot.read(DATASET_URI, load_video_frames=True)
        .where(col("episode_index").is_in(episodes) & col("frame_index").is_in(frames))
        .select("episode_index", "frame_index", col("observation.image"), col("observation.extrinsics"))
    )

    # 3. Join images to predictions on (episode, frame) and pull the handful of
    #    rows into plain Python for drawing.
    rows = frames_df.join(preds, on=["episode_index", "frame_index"]).to_pydict()

    os.makedirs(OVERLAYS_DIR, exist_ok=True)
    episode_frames: dict[int, list[tuple[int, np.ndarray]]] = {}
    for i in range(len(rows["frame_index"])):
        episode = rows["episode_index"][i]
        frame = rows["frame_index"][i]
        extrinsics = np.array(rows["observation.extrinsics"][i], dtype=np.float64).reshape(4, 4)
        ground_truth = np.array(rows["ground_truth_action"][i], dtype=np.float64)
        predicted = np.array(rows["predicted_action"][i], dtype=np.float64)

        image = Image.fromarray(np.asarray(rows["observation.image"][i]))
        draw = ImageDraw.Draw(image)
        draw_skeleton(draw, ground_truth, extrinsics, GROUND_TRUTH_COLOR)
        draw_skeleton(draw, predicted, extrinsics, PREDICTION_COLOR)
        draw.text((20, 20), "green = ground truth   red = predicted", fill=(255, 255, 255))

        out_path = os.path.join(OVERLAYS_DIR, f"episode{episode:03d}_frame{frame:05d}.png")
        image.save(out_path)
        print(f"Saved {out_path}")
        episode_frames.setdefault(episode, []).append((frame, np.asarray(image)))

    # Stitch each episode's overlays into a watchable mp4. The join above does
    # not guarantee row order, so write_episode_video sorts by frame_index.
    for episode, frames in sorted(episode_frames.items()):
        write_episode_video(episode, frames, VIDEO_FPS)
