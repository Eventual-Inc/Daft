from __future__ import annotations

import os

import pytest
from mcap.writer import Writer as MCAPWriter
from mcap_ros2.writer import Writer

import daft
from daft.filesystem import _resolve_paths_and_filesystem
from daft.io import IOConfig, S3Config

HAS_S3 = bool(
    os.environ.get("AWS_ACCESS_KEY_ID")
    and os.environ.get("AWS_SECRET_ACCESS_KEY")
    and os.environ.get("S3_ENDPOINT_URL")
)


def _get_s3_io_config() -> IOConfig:
    return IOConfig(
        s3=S3Config(
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            force_virtual_addressing=True,
            key_id=os.environ["AWS_ACCESS_KEY_ID"],
            access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            verify_ssl=True,
            region_name=os.environ.get("AWS_REGION", "cn-beijing"),
        )
    )


@pytest.fixture(scope="function")
def mcap_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("mcap")
    file_path = tmp_dir / "test.mcap"

    with open(file_path, "wb") as f:
        writer = Writer(f)
        schema = writer.register_msgdef(datatype="std_msgs/msg/String", msgdef_text="string data")

        for i in range(100):
            writer.write_message(
                topic="/test_topic",
                schema=schema,
                message={"data": f"Chatter #{i}", "antother": f"Another field {i}"},
                log_time=i * 100,
                publish_time=i * 100,
                sequence=i,
            )
        writer.finish()

    yield file_path


@pytest.fixture(scope="function")
def raw_bytes_mcap_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("mcap")
    file_path = tmp_dir / "special.mcap"

    with open(file_path, "wb") as f:
        writer = MCAPWriter(f)
        writer.start()
        schema_id = writer.register_schema(name="", encoding="", data=b"")
        channel_id_1 = writer.register_channel(
            topic="/robot0/sensor/camera0/compressed",
            message_encoding="",
            schema_id=schema_id,
        )
        channel_id_2 = writer.register_channel(
            topic="/robot0/sensor/imu",
            message_encoding="",
            schema_id=schema_id,
        )

        for i in range(2000):
            payload = bytes(j % 256 for j in range(i % 4096))
            writer.add_message(
                channel_id=channel_id_1,
                log_time=i * 1_000_000,
                publish_time=i * 1_000_000,
                sequence=i,
                data=payload,
            )
            writer.add_message(
                channel_id=channel_id_2,
                log_time=i * 1_000_000 + 1,
                publish_time=i * 1_000_000 + 1,
                sequence=i,
                data=b"\x00\x01\x02\x03\x04\x05\x06\x07",
            )

        writer.finish()

    yield file_path


@pytest.fixture(scope="function")
def robotics_mcap_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("mcap")
    file_path = tmp_dir / "robotics_session.mcap"

    with open(file_path, "wb") as f:
        writer = MCAPWriter(f)
        writer.start()
        schema_id = writer.register_schema(name="robotics/RawBytes", encoding="protobuf", data=b"")
        camera_channel_id = writer.register_channel(
            topic="/robot0/camera/front/compressed",
            message_encoding="jpeg",
            schema_id=schema_id,
        )
        imu_channel_id = writer.register_channel(
            topic="/robot0/imu",
            message_encoding="cdr",
            schema_id=schema_id,
        )
        lidar_channel_id = writer.register_channel(
            topic="/robot0/lidar/points",
            message_encoding="cdr",
            schema_id=schema_id,
        )

        for i in range(120):
            writer.add_message(
                channel_id=camera_channel_id,
                log_time=i * 1_000_000,
                publish_time=i * 1_000_000,
                sequence=i,
                data=b"\xff\xd8" + bytes([i % 256]) * 256,
            )

        for i in range(400):
            writer.add_message(
                channel_id=imu_channel_id,
                log_time=i * 250_000,
                publish_time=i * 250_000,
                sequence=i,
                data=bytes([i % 256]) * 48,
            )

        for i in range(40):
            writer.add_message(
                channel_id=lidar_channel_id,
                log_time=i * 2_500_000,
                publish_time=i * 2_500_000,
                sequence=i,
                data=bytes([i % 256]) * 2048,
            )

        writer.finish()

    yield file_path


@pytest.fixture(scope="function")
def data_from_s3():
    s3_file_path = "s3://kamui/las/mcap/test.mcap"
    io_config = _get_s3_io_config()
    [file_path], fs = _resolve_paths_and_filesystem(s3_file_path, io_config)
    with fs.open_output_stream(file_path) as f:
        writer = Writer(f)
        schema = writer.register_msgdef(datatype="std_msgs/msg/String", msgdef_text="string data")

        for i in range(100):
            writer.write_message(
                topic="/test_topic",
                schema=schema,
                message={"data": f"Chatter #{i}", "antother": f"Another field {i}"},
                log_time=i * 100,
                publish_time=i * 100,
                sequence=i,
            )
        writer.finish()

    yield s3_file_path


@pytest.mark.parametrize("mcap_dataset_path", ["mcap_dataset_path"], indirect=True)
def test_mcap_read(mcap_dataset_path):
    df = daft.read_mcap(mcap_dataset_path, start_time=0, end_time=100000, topics=["/test_topic"])
    df = df.collect()

    pdf = df.to_pandas()

    assert len(pdf) == 100
    assert "topic" in pdf.columns
    assert "data" in pdf.columns
    assert pdf["publish_time"].between(0, 9900).all()


def test_mcap_record_metadata_is_opt_in(mcap_dataset_path):
    default_pdf = daft.read_mcap(mcap_dataset_path, topics=["/test_topic"]).limit(1).to_pandas()

    assert set(default_pdf.columns) == {"topic", "log_time", "publish_time", "sequence", "data"}


def test_mcap_read_includes_schema_and_payload_metadata(mcap_dataset_path):
    df = daft.read_mcap(
        mcap_dataset_path,
        topics=["/test_topic"],
        include_record_metadata=True,
    ).limit(3)
    pdf = df.to_pandas()

    assert set(pdf.columns) == {
        "topic",
        "log_time",
        "publish_time",
        "sequence",
        "data",
        "schema_name",
        "schema_encoding",
        "message_encoding",
        "message_size",
    }
    assert (pdf["topic"] == "/test_topic").all()
    assert (pdf["schema_name"] == "std_msgs/msg/String").all()
    assert (pdf["schema_encoding"] == "ros2msg").all()
    assert (pdf["message_encoding"] == "cdr").all()
    assert (pdf["message_size"] > 0).all()


def test_mcap_inspect_builds_topic_manifest(mcap_dataset_path):
    pdf = daft.inspect_mcap(mcap_dataset_path).to_pandas()

    assert len(pdf) == 1
    row = pdf.iloc[0]
    assert row["file_path"] == str(mcap_dataset_path)
    assert row["topic"] == "/test_topic"
    assert row["schema_name"] == "std_msgs/msg/String"
    assert row["schema_encoding"] == "ros2msg"
    assert row["message_encoding"] == "cdr"
    assert row["message_count"] == 100
    assert row["first_log_time"] == 0
    assert row["last_log_time"] == 9900
    assert row["first_publish_time"] == 0
    assert row["last_publish_time"] == 9900
    assert row["min_message_size"] > 0
    assert row["max_message_size"] >= row["min_message_size"]
    assert row["total_message_size"] >= row["max_message_size"]
    assert row["avg_message_size"] > 0


def test_mcap_inspect_respects_topic_and_time_filters(raw_bytes_mcap_dataset_path):
    pdf = daft.inspect_mcap(
        raw_bytes_mcap_dataset_path,
        topics=["/robot0/sensor/imu"],
        start_time=1_000_001,
        end_time=5_000_001,
    ).to_pandas()

    assert len(pdf) == 1
    row = pdf.iloc[0]
    assert row["topic"] == "/robot0/sensor/imu"
    assert row["message_count"] == 4
    assert row["first_log_time"] == 1_000_001
    assert row["last_log_time"] == 4_000_001
    assert row["min_message_size"] == 8
    assert row["max_message_size"] == 8
    assert row["total_message_size"] == 32
    assert row["avg_message_size"] == 8


def test_mcap_inspect_returns_empty_manifest_when_filters_match_no_messages(mcap_dataset_path):
    pdf = daft.inspect_mcap(mcap_dataset_path, topics=["/missing"]).to_pandas()

    assert len(pdf) == 0
    assert set(pdf.columns) == {
        "file_path",
        "topic",
        "schema_name",
        "schema_encoding",
        "message_encoding",
        "message_count",
        "first_log_time",
        "last_log_time",
        "first_publish_time",
        "last_publish_time",
        "min_message_size",
        "max_message_size",
        "total_message_size",
        "avg_message_size",
    }


def test_mcap_plan_reads_shards_robotics_topics(robotics_mcap_dataset_path):
    pdf = daft.plan_mcap_reads(robotics_mcap_dataset_path, max_messages_per_task=100).to_pandas()

    camera_windows = pdf[pdf["topic"] == "/robot0/camera/front/compressed"]
    imu_windows = pdf[pdf["topic"] == "/robot0/imu"]
    lidar_windows = pdf[pdf["topic"] == "/robot0/lidar/points"]

    assert len(camera_windows) == 2
    assert len(imu_windows) == 4
    assert len(lidar_windows) == 1
    assert camera_windows["estimated_message_count"].sum() == 120
    assert imu_windows["estimated_message_count"].sum() == 400
    assert lidar_windows["estimated_message_count"].sum() == 40
    assert (pdf["estimated_payload_bytes"] > 0).all()
    assert (pdf["end_time"] > pdf["start_time"]).all()
    assert set(pdf["window_index"]) >= {0}


def test_mcap_plan_reads_can_resume_from_planned_windows(robotics_mcap_dataset_path):
    plan_pdf = daft.plan_mcap_reads(
        robotics_mcap_dataset_path,
        topics=["/robot0/imu"],
        max_messages_per_task=100,
    ).to_pandas()

    total_rows = 0
    for row in plan_pdf.itertuples(index=False):
        window_pdf = daft.read_mcap(
            row.file_path,
            topics=[row.topic],
            start_time=row.start_time,
            end_time=row.end_time,
        ).to_pandas()
        assert (window_pdf["topic"] == "/robot0/imu").all()
        assert window_pdf["log_time"].min() >= row.start_time
        assert window_pdf["log_time"].max() < row.end_time
        total_rows += len(window_pdf)

    assert total_rows == 400


def test_mcap_plan_reads_validates_target_window_size(mcap_dataset_path):
    with pytest.raises(ValueError, match="max_messages_per_task"):
        daft.plan_mcap_reads(mcap_dataset_path, max_messages_per_task=0)


def test_mcap_read_huggingface():
    """Read a public MCAP file from Hugging Face via hf://."""
    # Public gameplay MCAP from https://huggingface.co/datasets/open-world-agents/D2E-480p (~1.4 MiB).
    HF_MCAP_PATH = "hf://datasets/open-world-agents/D2E-480p/PEAK/recording_20250901_122320__8bd56fb0_split_02.mcap"

    df = daft.read_mcap(HF_MCAP_PATH, topics=["mouse"]).limit(10)
    pdf = df.to_pandas()

    assert len(pdf) == 10
    assert set(pdf.columns) == {"topic", "log_time", "publish_time", "sequence", "data"}
    assert (pdf["topic"] == "mouse").all()
    assert pdf["log_time"].is_monotonic_increasing


@pytest.mark.skipif(not HAS_S3, reason="S3 Env not set, skip S3 tests")
@pytest.mark.parametrize("data_from_s3", ["data_from_s3"], indirect=True)
def test_mcap_read_s3(data_from_s3):
    io_config = _get_s3_io_config()
    df = daft.read_mcap(data_from_s3, start_time=0, end_time=100000, topics=["/test_topic"], io_config=io_config)
    df = df.collect()

    pdf = df.to_pandas()

    assert len(pdf) == 100
    assert pdf["sequence"].nunique() == 100
    assert pdf["data"].startswith("Chatter #").all()


@pytest.mark.parametrize("raw_bytes_mcap_dataset_path", ["raw_bytes_mcap_dataset_path"], indirect=True)
def test_mcap_show_raw_bytes_does_not_crash(raw_bytes_mcap_dataset_path):
    df = daft.read_mcap(raw_bytes_mcap_dataset_path, batch_size=256)
    df.show(10)


def test_mcap_per_file_keyframe_scanner(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("mcap")
    dir_path = tmp_dir / "multi"
    dir_path.mkdir()

    file0 = dir_path / "a.mcap"
    file1 = dir_path / "b.mcap"
    topic0 = "/robot0/sensor/camera0/compressed"
    topic1 = "/robot0/sensor/camera1/compressed"

    for path, topic in [(file0, topic0), (file1, topic1)]:
        with open(path, "wb") as f:
            writer = MCAPWriter(f)
            writer.start()
            schema_id = writer.register_schema(name="", encoding="", data=b"")
            channel_id = writer.register_channel(topic=topic, message_encoding="", schema_id=schema_id)
            for i in range(20):
                writer.add_message(
                    channel_id=channel_id,
                    log_time=i * 100,
                    publish_time=i * 100,
                    sequence=i,
                    data=b"\x00\x00\x00\x01\x67",
                )
            writer.finish()

    def scan_for_keyframes(mcap_path: str) -> dict[str, int]:
        if mcap_path.endswith("a.mcap"):
            return {topic0: 500}
        if mcap_path.endswith("b.mcap"):
            return {topic1: 1000}
        return {}

    df = daft.read_mcap(
        str(dir_path),
        topic_start_time_resolver=scan_for_keyframes,
    ).collect()

    pdf = df.to_pandas()
    assert pdf[pdf["topic"] == topic0]["log_time"].min() >= 500
    assert pdf[pdf["topic"] == topic1]["log_time"].min() >= 1000


def test_mcap_topic_start_time_resolver_errors_are_not_swallowed(mcap_dataset_path):
    def broken_resolver(path: str) -> dict[str, int]:
        raise RuntimeError(f"cannot inspect keyframes for {path}")

    with pytest.raises(RuntimeError, match="cannot inspect keyframes"):
        daft.read_mcap(
            str(mcap_dataset_path),
            topic_start_time_resolver=broken_resolver,
        ).collect()
