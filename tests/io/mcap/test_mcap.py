from __future__ import annotations

import os
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, SimpleHTTPRequestHandler, ThreadingHTTPServer

import pytest
from mcap.writer import CompressionType, IndexType
from mcap.writer import Writer as MCAPWriter
from mcap_ros2.writer import Writer

import daft
from daft.filesystem import _resolve_paths_and_filesystem
from daft.io import IOConfig, S3Config
from daft.io.mcap._mcap import list_files

HAS_S3 = bool(
    os.environ.get("AWS_ACCESS_KEY_ID")
    and os.environ.get("AWS_SECRET_ACCESS_KEY")
    and os.environ.get("S3_ENDPOINT_URL")
)


class QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


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
def mcap_http_url(mcap_dataset_path):
    handler = partial(QuietHTTPRequestHandler, directory=str(mcap_dataset_path.parent))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield f"http://127.0.0.1:{server.server_port}/{mcap_dataset_path.name}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


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
    assert pdf["source_path"].unique().tolist() == [str(mcap_dataset_path)]
    assert all(isinstance(value, bytes) for value in pdf["data"])
    assert pdf["publish_time"].between(0, 9900).all()


def test_mcap_read_sequence_of_exact_paths_preserves_source_path(tmp_path):
    paths = [tmp_path / "a.mcap", tmp_path / "b.mcap"]
    expected = []
    for index, path in enumerate(paths):
        topic = f"/topic-{index}"
        payload = f"payload-{index}".encode()
        with path.open("wb") as output:
            writer = MCAPWriter(output, compression=CompressionType.NONE)
            writer.start()
            schema_id = writer.register_schema(name="", encoding="", data=b"")
            channel_id = writer.register_channel(topic=topic, message_encoding="", schema_id=schema_id)
            writer.add_message(
                channel_id,
                log_time=index + 1,
                publish_time=index + 11,
                sequence=index,
                data=payload,
            )
            writer.finish()
        expected.append((str(path), topic, index + 1, index + 11, index, payload))

    result = daft.read_mcap(paths).sort("source_path").to_pydict()

    assert (
        list(
            zip(
                result["source_path"],
                result["topic"],
                result["log_time"],
                result["publish_time"],
                result["sequence"],
                result["data"],
            )
        )
        == expected
    )
    assert daft.read_mcap(paths).where(daft.col("topic") == "/missing").count_rows() == 0


def test_mcap_read_empty_path_sequence_returns_standard_empty_schema():
    dataframe = daft.read_mcap([])

    assert dataframe.schema() == daft.Schema.from_pydict(
        {
            "source_path": daft.DataType.string(),
            "topic": daft.DataType.string(),
            "log_time": daft.DataType.uint64(),
            "publish_time": daft.DataType.uint64(),
            "sequence": daft.DataType.uint32(),
            "data": daft.DataType.binary(),
        }
    )
    assert dataframe.to_pydict() == {
        "source_path": [],
        "topic": [],
        "log_time": [],
        "publish_time": [],
        "sequence": [],
        "data": [],
    }


def test_mcap_read_nonempty_sequence_rejects_missing_paths(tmp_path):
    existing = tmp_path / "existing.mcap"
    existing.write_bytes(b"")
    missing = tmp_path / "missing.mcap"

    with pytest.raises(FileNotFoundError, match="missing.mcap"):
        daft.read_mcap([missing])
    with pytest.raises(FileNotFoundError, match="missing.mcap"):
        daft.read_mcap([existing, missing])


def test_mcap_http_url_resolves_through_pyarrow_fsspec_handler(mcap_http_url):
    [resolved_path], fs = _resolve_paths_and_filesystem(mcap_http_url)

    assert resolved_path == mcap_http_url
    assert list_files(mcap_http_url, io_config=None) == [mcap_http_url]
    with fs.open_input_file(resolved_path) as f:
        assert f.read(5) == b"\x89MCAP"


def test_mcap_read_huggingface():
    """Read a public MCAP file from Hugging Face via hf://."""
    # Public gameplay MCAP from https://huggingface.co/datasets/open-world-agents/D2E-480p (~1.4 MiB).
    HF_MCAP_PATH = "hf://datasets/open-world-agents/D2E-480p/PEAK/recording_20250901_122320__8bd56fb0_split_02.mcap"

    df = daft.read_mcap(HF_MCAP_PATH, topics=["mouse"]).limit(10)
    pdf = df.to_pandas()

    assert len(pdf) == 10
    assert set(pdf.columns) == {"source_path", "topic", "log_time", "publish_time", "sequence", "data"}
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
    assert all(isinstance(value, bytes) for value in pdf["data"])


@pytest.mark.parametrize("raw_bytes_mcap_dataset_path", ["raw_bytes_mcap_dataset_path"], indirect=True)
def test_mcap_show_raw_bytes_does_not_crash(raw_bytes_mcap_dataset_path):
    df = daft.read_mcap(raw_bytes_mcap_dataset_path, batch_size=256)
    df.show(10)


def test_mcap_planner_pushes_topic_time_and_projection(raw_bytes_mcap_dataset_path):
    camera_topic = "/robot0/sensor/camera0/compressed"
    result = (
        daft.read_mcap(raw_bytes_mcap_dataset_path)
        .where(
            daft.col("topic").is_in([camera_topic])
            & (daft.col("log_time") >= 100_000_000)
            & (daft.col("log_time") < 110_000_000)
        )
        .select("source_path", "topic", "log_time")
        .sort("log_time")
        .to_pydict()
    )

    assert result["topic"] == [camera_topic] * 10
    assert result["log_time"] == [i * 1_000_000 for i in range(100, 110)]
    assert set(result["source_path"]) == {str(raw_bytes_mcap_dataset_path)}
    assert set(result) == {"source_path", "topic", "log_time"}


def test_mcap_projection_keeps_residual_filter_columns(raw_bytes_mcap_dataset_path):
    result = (
        daft.read_mcap(raw_bytes_mcap_dataset_path)
        .where(daft.col("log_time") >= 1_990_000_000)
        .select("topic")
        .limit(5)
        .to_pydict()
    )

    assert len(result["topic"]) == 5
    assert set(result) == {"topic"}


def test_mcap_limit_is_not_applied_before_unsupported_residual(raw_bytes_mcap_dataset_path):
    result = (
        daft.read_mcap(raw_bytes_mcap_dataset_path)
        .where(daft.col("sequence") >= 1_995)
        .select("sequence")
        .limit(5)
        .to_pydict()
    )

    assert result["sequence"] == [1_995, 1_995, 1_996, 1_996, 1_997]


def test_mcap_explicit_and_planner_filters_intersect(raw_bytes_mcap_dataset_path):
    camera_topic = "/robot0/sensor/camera0/compressed"
    result = (
        daft.read_mcap(
            raw_bytes_mcap_dataset_path,
            topics=[camera_topic],
            start_time=100_000_000,
            end_time=200_000_000,
        )
        .where((daft.col("log_time") >= 150_000_000) & (daft.col("log_time") < 160_000_000))
        .select("topic", "log_time")
        .sort("log_time")
        .to_pydict()
    )

    assert result["topic"] == [camera_topic] * 10
    assert result["log_time"] == [i * 1_000_000 for i in range(150, 160)]


def test_mcap_nonexistent_topic_short_circuits(raw_bytes_mcap_dataset_path):
    assert daft.read_mcap(raw_bytes_mcap_dataset_path).where(daft.col("topic") == "/missing").count_rows() == 0


@pytest.mark.parametrize("compression", [CompressionType.NONE, CompressionType.LZ4, CompressionType.ZSTD])
def test_mcap_native_reader_compression(tmp_path, compression):
    path = tmp_path / f"{compression.name.lower()}.mcap"
    with path.open("wb") as output:
        writer = MCAPWriter(output, compression=compression)
        writer.start()
        schema_id = writer.register_schema(name="", encoding="", data=b"")
        channel_id = writer.register_channel(topic="/state", message_encoding="", schema_id=schema_id)
        writer.add_message(channel_id, log_time=1, publish_time=2, sequence=3, data=b"raw-payload")
        writer.finish()

    assert daft.read_mcap(path).select("data").to_pydict() == {"data": [b"raw-payload"]}


def test_mcap_native_reader_unindexed_fallback(tmp_path):
    path = tmp_path / "unindexed.mcap"
    with path.open("wb") as output:
        writer = MCAPWriter(output, index_types=IndexType.NONE)
        writer.start()
        schema_id = writer.register_schema(name="", encoding="", data=b"")
        channel_id = writer.register_channel(topic="/state", message_encoding="", schema_id=schema_id)
        for sequence in range(3):
            writer.add_message(
                channel_id,
                log_time=sequence,
                publish_time=sequence,
                sequence=sequence,
                data=bytes([sequence]),
            )
        writer.finish()

    assert daft.read_mcap(path).select("sequence", "data").to_pydict() == {
        "sequence": [0, 1, 2],
        "data": [b"\x00", b"\x01", b"\x02"],
    }
    assert daft.read_mcap(path, topics=["/state"], start_time=1, end_time=3).select("sequence").to_pydict() == {
        "sequence": [1, 2]
    }


def test_mcap_time_pushdown_reduces_remote_bytes_by_over_10x(tmp_path):
    path = tmp_path / "chunked.mcap"
    with path.open("wb") as output:
        writer = MCAPWriter(output, chunk_size=256 * 1024, compression=CompressionType.NONE)
        writer.start()
        schema_id = writer.register_schema(name="", encoding="", data=b"")
        channel_id = writer.register_channel(topic="/camera", message_encoding="", schema_id=schema_id)
        payload = b"x" * (240 * 1024)
        for sequence in range(128):
            writer.add_message(
                channel_id=channel_id,
                log_time=sequence,
                publish_time=sequence,
                sequence=sequence,
                data=payload,
            )
        writer.finish()
    contents = path.read_bytes()

    class CountingRangeHandler(BaseHTTPRequestHandler):
        bytes_served = 0

        def log_message(self, format, *args):
            pass

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(contents)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            header = self.headers.get("Range")
            if header is None:
                start, end, status = 0, len(contents) - 1, 200
            else:
                start_text, end_text = header.removeprefix("bytes=").split("-", 1)
                start = int(start_text)
                end = min(int(end_text) if end_text else len(contents) - 1, len(contents) - 1)
                status = 206
            payload = contents[start : end + 1]
            type(self).bytes_served += len(payload)
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(contents)}")
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), CountingRangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/chunked.mcap"
        full_rows = daft.read_mcap(url).select("log_time").count_rows()
        full_bytes = CountingRangeHandler.bytes_served

        CountingRangeHandler.bytes_served = 0
        filtered = daft.read_mcap(url).where(daft.col("log_time") == 64).select("log_time").to_pydict()
        filtered_bytes = CountingRangeHandler.bytes_served
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert full_rows == 128
    assert filtered == {"log_time": [64]}
    assert filtered_bytes * 10 < full_bytes


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
