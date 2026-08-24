"""Generate synthetic OmniSharing DF-2 HDF5 episodes matching the probed layout.

Shapes, dtypes and attribute names mirror a real 440 MB DF-2 episode, so tests
never need the 1.08 TB public release -- which is also CC-BY-NC-SA licensed, so
none of its bytes belong in this repo. Camera payloads are encoded locally to get
genuinely decodable streams in the same formats the dataset uses.

Details taken from the real file, several of which the published layout omits:

- ``observation/audio`` is raw ``float64`` PCM with ``samplerate`` and ``txt``
  attrs, not the "compressed audio stream" the docs describe.
- ``RGB_Camera*`` payloads are H.265/HEVC in Annex-B format; ``RGBD_*``
  sub-streams are Matroska.
- ``aligned_depth`` exists only on ``RGBD_0`` and carries no attrs at all.
- RGB camera ids are non-contiguous and run on a faster clock than
  ``aligned_timestamp``; ``RGBD_*`` additionally keeps a clock per eye.
- ``tactile`` is 3465 wide, split across 15 pads by ``sensor_lengths``.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import numpy as np

# Exactly the 15 sensors and widths observed in the real DF-2 file.
SENSOR_NAMES = [
    "palm_sensor1",
    "J32L",
    "J42L",
    "M6L",
    "S22L",
    "S32L",
    "S6L",
    "Z22L",
    "Z32L",
    "Z6L",
    "W22L",
    "W32L",
    "W6L",
    "X22L",
    "X6L",
]
SENSOR_LENGTHS = [219, 219, 282, 378, 195, 105, 318, 195, 105, 318, 195, 105, 318, 195, 318]
TACTILE_WIDTH = sum(SENSOR_LENGTHS)  # 3465

JOINT_NAMES = [
    "J1J",
    "J2J",
    "J3J",
    "J4J",
    "M1J",
    "M2J",
    "M3J",
    "M4J",
    "M5J",
    "X1J",
    "X2J",
    "X3J",
    "X4J",
    "X5J",
    "S1J",
    "S2J",
    "S3J",
    "S4J",
    "S5J",
    "Z1J",
    "Z2J",
    "Z3J",
    "Z4J",
    "Z5J",
    "W1J",
    "W2J",
    "W3J",
    "W4J",
    "W5J",
]
N_JOINTS = len(JOINT_NAMES)  # 29

HANDPOSE_ORDER = "[x, y, z, qw, qx, qy, qz]"
HANDPOSE_DETAIL = "前3维为位置坐标xyz，后4维为四元数"

# Deliberately non-contiguous, like the real capture rig.
RGB_CAMERA_IDS = (0, 1, 2, 4, 6)
RGBD_CAMERA_IDS = (0, 1)

#: Baseline, in metres, between the left eye and the colour sensor. Mirrors the
#: real data, where the transform is near-identity with a small x translation.
STEREO_BASELINE = -0.059160967498978596


def left_to_color_matrix(cam_id: int) -> list[list[float]]:
    """The ``left_to_color`` transform stored in ``inner_extrinsic``.

    Shaped like the real thing: a near-identity rotation plus a small
    translation, varied per camera so tests can tell them apart.
    """
    offset = cam_id * 0.001
    return [
        [0.9999999746025623, -0.00016700296889540808, -0.0001513435923511511, STEREO_BASELINE + offset],
        [0.0001672514853199877, 0.9999986353917404, 0.0016435454353450811, -0.00033976005189582253],
        [0.00015106890885919824, -0.001643570706043841, 0.999998637925832, 0.00021016499893739072],
        [0.0, 0.0, 0.0, 1.0],
    ]


def inner_extrinsic_payload(cam_id: int) -> dict:
    """The JSON blob stored at ``RGBD_{id}/inner_extrinsic``."""
    return {
        "calib_date": "20250925171218",
        "no_pose_cal_names": [],
        "pt3d_info": {"left_to_color": 2904, "color_to_color": 2904},
        "cam_spread_info": {
            "left_to_color": ["color", "left"],
            "color_to_color": ["color", "left", "color"],
        },
        "left_to_color": left_to_color_matrix(cam_id),
    }


# Real magic bytes so codec sniffing can be tested for real.
HEVC_ANNEXB_HEAD = bytes([0x00, 0x00, 0x01, 0x40, 0x01, 0x0C, 0x01, 0xFF, 0xFF, 0x01, 0x60])
MATROSKA_HEAD = bytes([0x1A, 0x45, 0xDF, 0xA3, 0xA3, 0x42, 0x86, 0x81, 0x01])

#: Frame geometry of the locally-encoded playable streams.
PLAYABLE_WIDTH = 64
PLAYABLE_HEIGHT = 48
PLAYABLE_FRAMES = 4


def encode_playable_stream(kind: str) -> np.ndarray:
    """Encode a tiny real video stream locally, as ``uint8`` bytes.

    Produces genuinely decodable payloads in the same formats the dataset uses
    -- H.265/HEVC Annex-B for RGB cameras and Matroska for RGBD sub-streams --
    without embedding any bytes from the (CC-BY-NC-SA licensed) dataset.

    Returns an empty array if the required encoder is unavailable, so callers
    can fall back to non-decodable placeholder payloads. libx265 in particular
    is absent from some PyAV wheels.
    """
    # av and h5py come from optional extras, so they stay function-local: this
    # module is imported by tests that guard on those extras being present.
    import av

    fmt, codec = ("hevc", "libx265") if kind == "hevc" else ("matroska", "libx264")
    buffer = io.BytesIO()
    try:
        container = av.open(buffer, "w", format=fmt)
        stream = container.add_stream(codec, rate=10)
        stream.width, stream.height = PLAYABLE_WIDTH, PLAYABLE_HEIGHT
        stream.pix_fmt = "yuv420p"
        if codec == "libx265":
            stream.options = {"x265-params": "log-level=none"}
        for i in range(PLAYABLE_FRAMES):
            frame_rgb = np.full((PLAYABLE_HEIGHT, PLAYABLE_WIDTH, 3), i * 40, dtype=np.uint8)
            frame_rgb[:, :, 1] = i * 20
            video_frame = av.VideoFrame.from_ndarray(frame_rgb, format="rgb24")
            for packet in stream.encode(video_frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)
        container.close()
    except (av.FFmpegError, LookupError, ValueError):
        # A missing or misconfigured encoder, not a bug in the caller.
        return np.empty(0, dtype=np.uint8)
    return np.frombuffer(buffer.getvalue(), dtype=np.uint8)


def _payload(head: bytes, size: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    body = rng.integers(0, 256, size=max(size - len(head), 0), dtype=np.uint8)
    return np.concatenate([np.frombuffer(head, dtype=np.uint8), body])


def write_df2_episode(
    path: Path,
    *,
    n_frames: int = 12,
    n_objects: int = 2,
    tactile_width: int = TACTILE_WIDTH,
    n_joints: int = N_JOINTS,
    include_audio: bool = True,
    include_meta: bool = True,
    instruction: str = "将燕京啤酒放入红色啤酒架。",
    playable: bool = False,
    seed: int = 7,
) -> Path:
    """Write one synthetic DF-2 episode. Returns the path written.

    With ``playable=True`` the camera payloads are real, locally-encoded HEVC
    and Matroska streams that PyAV can decode. Otherwise they are correct magic
    bytes followed by random noise, which is enough to exercise codec sniffing.
    """
    import h5py

    rng = np.random.default_rng(seed)
    path.parent.mkdir(parents=True, exist_ok=True)

    hevc_stream = encode_playable_stream("hevc") if playable else np.empty(0, dtype=np.uint8)
    mkv_stream = encode_playable_stream("matroska") if playable else np.empty(0, dtype=np.uint8)

    with h5py.File(path, "w") as f:
        root = f.create_group("dataset")
        root.attrs["generated_time"] = "2025-12-11-21:31:35"
        root.attrs["data_id"] = "gARdlC4="

        if include_meta:
            meta = root.create_group("meta")
            meta.attrs["vendor"] = "paxini"
            meta.attrs["任务物品"] = np.array(["燕京啤酒+红色啤酒架"], dtype=object)
            meta.attrs["啤酒架的初始摆放位置"] = np.array(["左中"], dtype=object)
            meta.attrs["物品初始摆放高度"] = np.array(["所有物品位于台面上"], dtype=object)

        # ---------------------------------------------------------- hands
        def write_hand(parent: h5py.Group, side: str, *, with_tactile: bool) -> None:
            hand = parent.create_group(side)
            hand.attrs["description"] = f"exoskeleton_hand_{side.replace('hand', '')}_2_0_description"

            joints = hand.create_group("joints")
            jdata = joints.create_dataset(
                "data",
                data=rng.standard_normal((n_frames, n_joints)).astype(np.float32),
            )
            jdata.attrs["joint_names"] = np.array(JOINT_NAMES[:n_joints], dtype=object)

            pose = hand.create_group("handpose")
            pose.attrs["order"] = HANDPOSE_ORDER
            pose.attrs["detail"] = HANDPOSE_DETAIL
            pose.attrs["source_camera"] = "RGBD_0"
            pose.create_dataset("data", data=rng.standard_normal((n_frames, 7)).astype(np.float32))

            if with_tactile:
                tac = hand.create_group("tactile")
                tdata = tac.create_dataset(
                    "data",
                    data=rng.random((n_frames, tactile_width)).astype(np.float32),
                )
                widths = _scaled_sensor_lengths(tactile_width)
                tdata.attrs["sensor_names"] = np.array(SENSOR_NAMES, dtype=object)
                tdata.attrs["sensor_lengths"] = np.array(widths, dtype=np.int64)

        action = root.create_group("action")
        write_hand(action, "lefthand", with_tactile=False)
        write_hand(action, "righthand", with_tactile=False)

        obs = root.create_group("observation")
        write_hand(obs, "lefthand", with_tactile=True)
        write_hand(obs, "righthand", with_tactile=True)

        # ------------------------------------------------- timestamps/audio
        base_ts = 1_733_000_000_000_000
        obs.create_dataset(
            "aligned_timestamp",
            data=(base_ts + np.arange(n_frames, dtype=np.int64) * 33_000),
        )

        if include_audio:
            audio = obs.create_dataset("audio", data=rng.standard_normal((n_frames * 40, 1)).astype(np.float64))
            audio.attrs["samplerate"] = np.int64(8000)
            audio.attrs["txt"] = instruction

        # ---------------------------------------------------------- cameras
        image = obs.create_group("image")
        image.attrs["checked_cam_name"] = "Camera4"

        for cam_id in RGB_CAMERA_IDS:
            cam = image.create_group(f"RGB_Camera{cam_id}")
            rgb_payload = hevc_stream if hevc_stream.size else _payload(HEVC_ANNEXB_HEAD, 4096, seed + cam_id)
            cam.create_dataset("data", data=rgb_payload)
            ext = cam.create_dataset("extrinsics", data=np.eye(4, dtype=np.float64))
            ext.attrs["calib_date"] = "20251204154931"
            ext.attrs["relative_to_who"] = "RGB_Camera6"
            intr = cam.create_dataset(
                "intrinsics",
                data=np.array([[1000.0, 0, 960], [0, 1000.0, 600], [0, 0, 1]], dtype=np.float32),
            )
            intr.attrs["width"] = np.int64(1920)
            intr.attrs["height"] = np.int64(1200)
            intr.attrs["distortion"] = "[0.93, 0.64, 0.0002, -0.0002, 0.05, 0.86, 0.67, 0.078]"
            # RGB streams intentionally have MORE frames than the observation clock.
            cam.create_dataset(
                "timestamp",
                data=(base_ts + np.arange(n_frames + 3 + cam_id, dtype=np.int64) * 31_000),
            )

        for cam_id in RGBD_CAMERA_IDS:
            cam = image.create_group(f"RGBD_{cam_id}")
            cam.create_dataset("timestamp", data=(base_ts + np.arange(n_frames, dtype=np.int64) * 33_000))
            cam.create_dataset(
                "inner_extrinsic",
                data=np.array([json.dumps(inner_extrinsic_payload(cam_id)).encode()]),
            )
            if cam_id == 0:
                cam.create_dataset(
                    "aligned_depth",
                    data=rng.integers(0, 4096, size=(n_frames, 8, 12), dtype=np.uint16),
                )
            for sub in ("color", "left", "right"):
                grp = cam.create_group(sub)
                rgbd_payload = (
                    mkv_stream if mkv_stream.size else _payload(MATROSKA_HEAD, 2048, seed + cam_id + len(sub))
                )
                grp.create_dataset("data", data=rgbd_payload)
                sintr = grp.create_dataset(
                    "intrinsics",
                    data=np.array([[900.0, 0, 640], [0, 900.0, 360], [0, 0, 1]], dtype=np.float32),
                )
                sintr.attrs["width"] = np.int64(1280)
                sintr.attrs["height"] = np.int64(720)
                sintr.attrs["distortion"] = "[-0.055, 0.063, 0.0003, 0.0007, -0.021]"
                if sub == "color":
                    sext = grp.create_dataset("extrinsics", data=np.eye(4, dtype=np.float64))
                    sext.attrs["calib_date"] = "20251204154824"
                    sext.attrs["relative_to_who"] = "RGBD_0"
                else:
                    # The left eye is given its own faster clock and one extra
                    # sample, so tests can prove that per-eye alignment is
                    # actually used. The right eye deliberately shares the colour
                    # clock, giving a control case.
                    if sub == "left":
                        eye_ts = base_ts + np.arange(n_frames + 1, dtype=np.int64) * 31_000
                    else:
                        eye_ts = base_ts + np.arange(n_frames, dtype=np.int64) * 33_000
                    cam.create_dataset(f"{sub}_timestamp", data=eye_ts)

        # ---------------------------------------------------------- objects
        for i in range(1, n_objects + 1):
            obj = obs.create_group(f"obj{i}")
            odata = obj.create_dataset("data", data=rng.standard_normal((n_frames, 17)).astype(np.float32))
            odata.attrs["obj_name"] = f"object_{i}"
            odata.attrs["obj_id"] = np.int64(100 + i)
            odata.attrs["order"] = "[4x4 transform (16) + validity (1)]"

    return path


def _scaled_sensor_lengths(total: int) -> list[int]:
    """Sensor widths that sum to ``total``, keeping the real proportions."""
    if total == TACTILE_WIDTH:
        return list(SENSOR_LENGTHS)
    widths = [max(1, round(w * total / TACTILE_WIDTH)) for w in SENSOR_LENGTHS]
    widths[-1] += total - sum(widths)
    return widths


def episode_filename(
    index: int,
    capture_time: str,
    room_id: int,
    personnel_id: int,
    suffix: str | None = "glove",
) -> str:
    tail = f"_{suffix}" if suffix else ""
    return f"episode_{index}_{capture_time}_{room_id}_{personnel_id}{tail}.hdf5"
