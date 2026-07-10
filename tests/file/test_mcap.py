from __future__ import annotations

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import pytest

pytest.importorskip("mcap")

from mcap.writer import CompressionType, IndexType, Writer

import daft
from daft.exceptions import DaftCoreException


@pytest.fixture
def sample_mcap_path(tmp_path):
    path = tmp_path / "sample.mcap"
    with path.open("wb") as output:
        writer = Writer(output, chunk_size=128)
        writer.start(profile="test-profile", library="test-writer")
        schema_id = writer.register_schema("example.Message", "protobuf", b"schema-bytes")
        state_channel = writer.register_channel(
            topic="/state",
            message_encoding="protobuf",
            schema_id=schema_id,
            metadata={"role": "state"},
        )
        video_channel = writer.register_channel(
            topic="/camera",
            message_encoding="protobuf",
            schema_id=schema_id,
            metadata={"codec": "h264"},
        )

        for sequence in range(10):
            writer.add_message(
                channel_id=state_channel,
                log_time=1_000 + sequence * 10,
                publish_time=1_001 + sequence * 10,
                sequence=sequence,
                data=f"state-{sequence}".encode(),
            )
            writer.add_message(
                channel_id=video_channel,
                log_time=1_005 + sequence * 10,
                publish_time=1_006 + sequence * 10,
                sequence=sequence,
                data=b"\x00\x00\x00\x01\x65",
            )

        writer.add_metadata("session", {"episode_id": "episode-1"})
        writer.finish()
    return str(path)


def test_mcap_file_native_metadata(sample_mcap_path):
    file = daft.McapFile(sample_mcap_path)
    metadata = file.metadata()

    assert metadata["indexed"] is True
    assert metadata["has_chunk_indexes"] is True
    assert metadata["has_message_indexes"] is True
    assert metadata["header"] == {"profile": "test-profile", "library": "test-writer"}
    assert metadata["statistics"] == {
        "message_count": 20,
        "schema_count": 1,
        "channel_count": 2,
        "attachment_count": 0,
        "metadata_count": 1,
        "chunk_count": len(metadata["chunks"]),
        "message_start_time": 1_000,
        "message_end_time": 1_095,
    }
    assert file.topics() == ["/state", "/camera"]
    assert file.message_count() == 20
    assert file.time_range() == {"start_time": 1_000, "end_time": 1_095}

    channels = {channel["topic"]: channel for channel in metadata["channels"]}
    assert channels["/state"]["message_count"] == 10
    assert channels["/camera"]["metadata"] == {"codec": "h264"}
    assert metadata["schemas"][0]["name"] == "example.Message"
    assert base64.b64decode(metadata["schemas"][0]["data_base64"]) == b"schema-bytes"
    assert metadata["metadata"] == [
        {
            "name": "session",
            "metadata": {"episode_id": "episode-1"},
            "position": metadata["metadata"][0]["position"],
            "size": metadata["metadata"][0]["size"],
        }
    ]
    assert len(metadata["chunks"]) > 1
    assert all(chunk["position"] >= 0 and chunk["size"] > 0 for chunk in metadata["chunks"])


def test_mcap_file_dtype_and_expressions(sample_mcap_path):
    df = daft.from_pydict({"path": [sample_mcap_path]}).select(
        daft.functions.mcap_file(daft.col("path"), verify=True).alias("recording")
    )
    assert df.schema()["recording"].dtype == daft.DataType.file(daft.MediaType.mcap())

    result = (
        df.select(
            daft.col("recording").mcap_metadata().alias("metadata"),
            daft.col("recording").mcap_topics().alias("topics"),
            daft.col("recording").mcap_message_count().alias("message_count"),
            daft.col("recording").mcap_time_range().alias("time_range"),
        )
        .collect()
        .to_pydict()
    )

    assert json.loads(result["metadata"][0])["statistics"]["message_count"] == 20
    assert result["topics"] == [["/state", "/camera"]]
    assert result["message_count"] == [20]
    assert result["time_range"] == [{"start_time": 1_000, "end_time": 1_095}]


def test_as_mcap_from_generic_file(sample_mcap_path):
    assert daft.File(sample_mcap_path).as_mcap().message_count() == 20

    df = daft.from_pydict({"path": [sample_mcap_path]})
    df = df.select(daft.functions.mcap_file(daft.functions.file(df["path"]), verify=True))
    assert isinstance(df.collect().to_pydict()["path"][0], daft.McapFile)


def test_mcap_file_rejects_invalid_magic(tmp_path):
    path = tmp_path / "not-mcap.mcap"
    path.write_bytes(b"not an mcap")

    with pytest.raises(ValueError, match="not an MCAP file"):
        daft.McapFile(str(path))

    df = daft.from_pydict({"path": [str(path)]})
    with pytest.raises(DaftCoreException, match="Invalid MCAP file"):
        df.select(daft.functions.mcap_file(daft.col("path"), verify=True)).collect()


def test_mcap_metadata_does_not_scan_unindexed_data(tmp_path):
    path = tmp_path / "unindexed.mcap"
    with path.open("wb") as output:
        writer = Writer(
            output,
            index_types=IndexType.NONE,
            repeat_channels=False,
            repeat_schemas=False,
            use_statistics=False,
            use_summary_offsets=False,
        )
        writer.start()
        schema_id = writer.register_schema("example.Message", "protobuf", b"schema")
        channel_id = writer.register_channel("/state", "protobuf", schema_id)
        writer.add_message(channel_id, log_time=1, publish_time=1, sequence=0, data=b"payload")
        writer.finish()

    metadata = daft.McapFile(str(path)).metadata()
    assert metadata["indexed"] is False
    assert metadata["has_chunk_indexes"] is False
    assert metadata["has_message_indexes"] is False
    assert metadata["statistics"] is None
    assert metadata["chunks"] == []


def test_mcap_summary_without_chunk_indexes(tmp_path):
    path = tmp_path / "summary-only.mcap"
    with path.open("wb") as output:
        writer = Writer(output, index_types=IndexType.NONE)
        writer.start()
        schema_id = writer.register_schema("example.Message", "protobuf", b"schema")
        channel_id = writer.register_channel("/state", "protobuf", schema_id)
        writer.add_message(channel_id, log_time=1, publish_time=1, sequence=0, data=b"payload")
        writer.finish()

    metadata = daft.McapFile(str(path)).metadata()
    assert metadata["has_summary"] is True
    assert metadata["has_chunk_indexes"] is False
    assert metadata["has_message_indexes"] is False
    assert metadata["statistics"]["message_count"] == 1
    assert [channel["topic"] for channel in metadata["channels"]] == ["/state"]


def test_mcap_metadata_uses_remote_ranges(tmp_path):
    path = tmp_path / "large.mcap"
    with path.open("wb") as output:
        writer = Writer(output, chunk_size=32 * 1024 * 1024, compression=CompressionType.NONE)
        writer.start()
        schema_id = writer.register_schema("example.Message", "protobuf", b"schema")
        channel_id = writer.register_channel("/state", "protobuf", schema_id)
        writer.add_message(
            channel_id,
            log_time=1,
            publish_time=1,
            sequence=0,
            data=b"\x00" * (20 * 1024 * 1024),
        )
        writer.finish()
    contents = path.read_bytes()

    class RangeHandler(BaseHTTPRequestHandler):
        bytes_served = 0
        ranges: ClassVar[list[str | None]] = []

        def log_message(self, format, *args):
            pass

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(contents)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            range_header = self.headers.get("Range")
            type(self).ranges.append(range_header)
            if range_header is None:
                start, end = 0, len(contents) - 1
                status = 200
            else:
                units, requested = range_header.split("=", 1)
                assert units == "bytes"
                start_text, end_text = requested.split("-", 1)
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

    server = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/large.mcap"
        metadata = daft.McapFile(url).metadata()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert metadata["statistics"]["message_count"] == 1
    assert RangeHandler.ranges and all(request is not None for request in RangeHandler.ranges)
    assert RangeHandler.bytes_served < len(contents) // 4
