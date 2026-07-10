from __future__ import annotations

import json
import os

import pytest

pytest.importorskip("mcap")

from mcap.writer import CompressionType, Writer

import daft
from daft.datasets import abc


def _varint(value: int) -> bytes:
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _length_delimited(field: int, value: bytes) -> bytes:
    return _varint(field << 3 | 2) + _varint(len(value)) + value


def _annotation_payload(timestamp_ns: int, label: str) -> bytes:
    seconds, nanos = divmod(timestamp_ns, 1_000_000_000)
    timestamp = _varint(1 << 3) + _varint(seconds) + _varint(2 << 3) + _varint(nanos)
    return _length_delimited(1, timestamp) + _length_delimited(2, label.encode())


def _write_episode(path, *, metadata_name: str, metadata: dict[str, str], base_time: int) -> None:
    path.parent.mkdir(parents=True)
    with path.open("wb") as output:
        writer = Writer(output, chunk_size=128, compression=CompressionType.NONE)
        writer.start()
        state_schema = writer.register_schema("yam.RobotState", "protobuf", b"state-schema")
        video_schema = writer.register_schema("foxglove.CompressedVideo", "protobuf", b"video-schema")
        state_channel = writer.register_channel("/left-arm-state", "protobuf", state_schema)
        writer.register_channel("/top-camera", "protobuf", video_schema)
        for sequence in range(2):
            writer.add_message(
                state_channel,
                log_time=base_time + sequence * 10,
                publish_time=base_time + sequence * 10 + 1,
                sequence=sequence,
                data=f"state-{sequence}".encode(),
            )
        writer.add_metadata(metadata_name, metadata)
        writer.finish()


def _write_annotations(path, *, timestamp_ns: int, labels: list[str]) -> None:
    with path.open("wb") as output:
        writer = Writer(output, chunk_size=128, compression=CompressionType.NONE)
        writer.start()
        schema = writer.register_schema("yam.Annotation", "protobuf", b"annotation-schema")
        channel = writer.register_channel("/subtask-annotation", "protobuf", schema)
        for sequence, label in enumerate(labels):
            message_time = timestamp_ns + sequence * 10
            writer.add_message(
                channel,
                log_time=message_time,
                publish_time=message_time,
                sequence=sequence,
                data=_annotation_payload(message_time, label),
            )
        writer.finish()


@pytest.fixture
def abc_root(tmp_path):
    first = tmp_path / "data/train/fold_the_towel/episode_11111111-1111-1111-1111-111111111111"
    second = tmp_path / "data/val/pick_up_the_cup/episode_22222222-2222-2222-2222-222222222222"
    _write_episode(
        first / "episode.mcap",
        metadata_name="episode-metadata",
        metadata={
            "session_id": "11111111-1111-1111-1111-111111111111",
            "operator_id": "operator-one",
            "task_name": "fold the towel",
            "duration": "1.25",
        },
        base_time=1_000,
    )
    _write_annotations(first / "annotation.mcap", timestamp_ns=1_000, labels=["pick up towel", "fold"])
    _write_episode(
        second / "episode.mcap",
        metadata_name="session-metadata",
        metadata={
            "session-uuid": "22222222-2222-2222-2222-222222222222",
            "operator-id": "operator-two",
            "instruction": "pick up the cup",
        },
        base_time=2_000,
    )
    return tmp_path


def test_raw_builds_lazy_episode_catalog(abc_root):
    result = abc.raw(str(abc_root)).sort("episode_id").to_pydict()

    assert result["split"] == ["train", "val"]
    assert result["task_slug"] == ["fold_the_towel", "pick_up_the_cup"]
    assert result["episode_id"] == [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
    ]
    assert result["annotated"] == [True, False]
    assert result["episode_size"] == [
        (abc_root / "data/train/fold_the_towel/episode_11111111-1111-1111-1111-111111111111/episode.mcap")
        .stat()
        .st_size,
        (abc_root / "data/val/pick_up_the_cup/episode_22222222-2222-2222-2222-222222222222/episode.mcap")
        .stat()
        .st_size,
    ]
    assert result["annotation_size"][0] > 0
    assert result["annotation_size"][1] is None
    assert all(isinstance(file, daft.McapFile) for file in result["episode_mcap"])
    assert isinstance(result["annotation_mcap"][0], daft.McapFile)
    assert result["annotation_mcap"][1] is None


def test_raw_pushes_split_and_task_into_catalog_glob(abc_root):
    result = abc.raw(str(abc_root), split="train", tasks="fold_the_towel").to_pydict()

    assert result["episode_id"] == ["11111111-1111-1111-1111-111111111111"]


def test_raw_empty_local_task_selection_returns_empty_catalog(abc_root):
    result = abc.raw(str(abc_root), split="train", tasks="missing_task").to_pydict()

    assert all(values == [] for values in result.values())
    assert set(result) == {
        "split",
        "task_slug",
        "episode_id",
        "episode_dir",
        "episode_path",
        "episode_size",
        "annotation_path",
        "annotation_size",
        "annotated",
        "episode_mcap",
        "annotation_mcap",
    }


def test_raw_task_filter_accepts_documented_task_prefix(abc_root):
    episode = abc_root / "data/train/task=documented_task/episode_33333333-3333-3333-3333-333333333333"
    _write_episode(
        episode / "episode.mcap",
        metadata_name="episode-metadata",
        metadata={"session_id": "33333333-3333-3333-3333-333333333333"},
        base_time=3_000,
    )

    result = abc.raw(str(abc_root), split="train", tasks="documented_task").to_pydict()

    assert result["task_slug"] == ["documented_task"]
    assert result["episode_id"] == ["33333333-3333-3333-3333-333333333333"]


def test_metadata_normalizes_current_and_legacy_records(abc_root):
    result = abc.metadata(abc.raw(str(abc_root))).sort("episode_id").to_pydict()

    assert result["session_id"] == [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
    ]
    assert result["operator_id"] == ["operator-one", "operator-two"]
    assert result["task_name"] == ["fold the towel", "pick up the cup"]
    assert result["duration_seconds"][0] == 1.25
    assert result["duration_seconds"][1] == pytest.approx(10 / 1_000_000_000)
    assert result["message_count"] == [2, 2]
    assert result["message_start_time"] == [1_000, 2_000]
    assert result["message_end_time"] == [1_010, 2_010]
    assert result["topics"] == [
        ["/left-arm-state", "/top-camera"],
        ["/left-arm-state", "/top-camera"],
    ]
    assert result["video_topics"] == [["/top-camera"], ["/top-camera"]]
    assert result["indexed"] == [True, True]
    assert json.loads(result["episode_metadata_json"][0])["name"] == "episode-metadata"
    assert json.loads(result["episode_metadata_json"][1])["name"] == "session-metadata"


def test_messages_reads_exact_bounded_paths_with_episode_identity(abc_root):
    episodes = abc.raw(str(abc_root))
    result = abc.messages(episodes, topics="/left-arm-state", start_time=1_005).sort("source_path").to_pydict()

    assert result["data"] == [b"state-1", b"state-0", b"state-1"]
    assert result["split"] == ["train", "val", "val"]
    assert result["task_slug"] == ["fold_the_towel", "pick_up_the_cup", "pick_up_the_cup"]
    assert result["episode_id"] == [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
        "22222222-2222-2222-2222-222222222222",
    ]
    assert result["file_kind"] == ["episode", "episode", "episode"]


def test_messages_handles_empty_bounded_catalog(abc_root):
    episodes = abc.raw(str(abc_root)).where(daft.col("episode_id") == "missing")

    result = abc.messages(episodes, topics="/left-arm-state").to_pydict()

    assert all(values == [] for values in result.values())
    assert set(result) >= {"source_path", "topic", "data", "episode_id"}


def test_annotations_decodes_free_form_labels(abc_root):
    episodes = abc.raw(str(abc_root)).where(daft.col("annotated"))

    result = abc.annotations(episodes).sort("sequence").to_pydict()

    assert result["label"] == ["pick up towel", "fold"]
    assert result["timestamp_ns"] == [1_000, 1_010]
    assert result["log_time"] == [1_000, 1_010]
    assert result["file_kind"] == ["annotation", "annotation"]


def test_camera_frames_rejects_unknown_alias(abc_root):
    with pytest.raises(ValueError, match="Unknown camera"):
        abc.camera_frames(abc.raw(str(abc_root)), cameras="overhead")


@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("HF_TOKEN"), reason="ABC-130k requires gated Hugging Face access")
def test_abc_huggingface_smoke():
    episode_id = "5b33995f-ba4a-49f8-bfb7-c6c034df0865"
    episodes = (
        abc.raw(
            split="train",
            tasks="clip_the_socks_to_the_hanger",
            io_config=daft.io.IOConfig(),
        )
        .where(daft.col("episode_id") == episode_id)
        .limit(1)
    )

    sniffed = abc.metadata(episodes).select("episode_id", "message_count", "chunk_count").to_pydict()
    assert sniffed == {"episode_id": [episode_id], "message_count": [9_849], "chunk_count": [18]}

    rows = abc.messages(
        episodes,
        topics="/left-arm-state",
        start_time=1_750_450_520_000_000_000,
        end_time=1_750_450_521_000_000_000,
    ).select("episode_id", "source_path", "topic", "log_time")
    result = rows.to_pydict()
    assert result["episode_id"]
    assert set(result["episode_id"]) == {episode_id}
    assert set(result["topic"]) == {"/left-arm-state"}
    assert all(path.endswith(f"/episode_{episode_id}/episode.mcap") for path in result["source_path"])

    labels = abc.annotations(episodes).select("episode_id", "timestamp_ns", "label").to_pydict()
    assert len(labels["label"]) == 9
    assert set(labels["episode_id"]) == {episode_id}
    assert all(timestamp is not None for timestamp in labels["timestamp_ns"])
    assert all(label for label in labels["label"])


@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("HF_TOKEN"), reason="ABC-130k requires gated Hugging Face access")
def test_abc_huggingface_camera_smoke():
    pytest.importorskip("av")
    episode_id = "5b33995f-ba4a-49f8-bfb7-c6c034df0865"
    episodes = (
        abc.raw(split="train", tasks="clip_the_socks_to_the_hanger")
        .where(daft.col("episode_id") == episode_id)
        .limit(1)
    )

    result = (
        abc.camera_frames(
            episodes,
            cameras="left_wrist",
            start_time=1_750_450_525_202_761_905,
            end_time=1_750_450_525_503_013_648,
        )
        .select("episode_id", "camera", "topic", "format", "timestamp_ns")
        .limit(1)
        .to_pydict()
    )
    assert result["episode_id"] == [episode_id]
    assert result["camera"] == ["left_wrist"]
    assert result["topic"] == ["/left-wrist-camera"]
    assert result["format"] == ["h264"]
