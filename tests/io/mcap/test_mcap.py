from __future__ import annotations

import os

import pytest
from mcap.writer import Writer as MCAPWriter
from mcap_ros2.writer import Writer

import daft
from daft.filesystem import _infer_filesystem
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
def data_from_s3():
    s3_file_path = "s3://kamui/las/mcap/test.mcap"
    io_config = _get_s3_io_config()
    file_path, fs, _ = _infer_filesystem(s3_file_path, io_config)
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
