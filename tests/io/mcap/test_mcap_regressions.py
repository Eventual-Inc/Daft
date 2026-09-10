"""Regression coverage for native MCAP discovery, filtering, and validation."""

from __future__ import annotations

import io
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from mcap.reader import make_reader
from mcap.writer import CompressionType, IndexType, Writer

import daft


def mcap_bytes(times=(1, 2, 3), *, index_types=IndexType.ALL, payload=b"PAYLOAD-ORIGINAL", **options):
    output = io.BytesIO()
    compression = options.pop("compression", CompressionType.NONE)
    writer = Writer(output, compression=compression, index_types=index_types, **options)
    writer.start()
    channel = writer.register_channel(topic="/a", message_encoding="raw", schema_id=0)
    for sequence, time in enumerate(times):
        writer.add_message(channel, time, payload, time, sequence)
    writer.finish()
    return output.getvalue()


@pytest.mark.parametrize("top_level_file", [False, True])
@pytest.mark.parametrize("trailing_slash", [False, True])
def test_directory_keeps_recursive_discovery(tmp_path, top_level_file, trailing_slash):
    if top_level_file:
        (tmp_path / "a.mcap").write_bytes(mcap_bytes((1, 2, 3)))
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "b.mcap").write_bytes(mcap_bytes((4, 5)))
    expected = daft.read_mcap(str(tmp_path / "**" / "*.mcap")).sort("log_time").select("log_time").to_pydict()
    assert expected == {"log_time": [1, 2, 3, 4, 5] if top_level_file else [4, 5]}
    path = str(tmp_path) + ("/" if trailing_slash else "")
    assert daft.read_mcap(path).sort("log_time").select("log_time").to_pydict() == expected


def test_recursive_discovery_preserves_exact_file_and_explicit_glob(tmp_path):
    path = tmp_path / "extensionless"
    path.write_bytes(mcap_bytes())
    assert daft.read_mcap(path).count_rows() == 3
    (tmp_path / "a.mcap").write_bytes(mcap_bytes())
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "b.mcap").write_bytes(mcap_bytes())
    assert daft.read_mcap(str(tmp_path / "*.mcap")).count_rows() == 3


def test_nanosecond_where_matches_explicit_time_bound(tmp_path):
    start = 1609459200000000000
    path = tmp_path / "time.mcap"
    path.write_bytes(mcap_bytes(range(start, start + 5)))
    expected = daft.read_mcap(path, end_time=start + 2).select("sequence").to_pydict()
    assert expected == {"sequence": [0, 1]}
    assert daft.read_mcap(path).where(daft.col("log_time") < start + 2).select("sequence").to_pydict() == expected


def test_timestamp_pushdown_preserves_residual_semantics(tmp_path):
    start = 1609459200000000000
    path = tmp_path / "time.mcap"
    path.write_bytes(mcap_bytes(range(start, start + 5)))
    df = daft.read_mcap(path)
    predicate = daft.col("log_time") >= start + 2
    pushed = df.where(predicate).select("sequence").to_pydict()
    # /absent does not occur in the fixture; OR prevents constraint extraction.
    residual = df.where(predicate | (daft.col("topic") == "/absent")).select("sequence").to_pydict()
    assert pushed == residual
    assert pushed == {"sequence": [2, 3, 4]}


@pytest.mark.parametrize("index_types", [IndexType.NONE, IndexType.ALL])
def test_chunk_crc_is_checked_in_both_reader_modes(tmp_path, index_types):
    contents = mcap_bytes(index_types=index_types)
    damaged = contents.replace(b"PAYLOAD-ORIGINAL", b"PAYLOAD-CORRUPT!")
    assert len(damaged) == len(contents)
    path = tmp_path / "corrupt.mcap"
    path.write_bytes(damaged)
    with pytest.raises(Exception, match="(?i)crc"):
        daft.read_mcap(path).collect()


@pytest.mark.parametrize("compression", list(CompressionType))
def test_indexed_chunk_crc_validation_for_all_compressions(tmp_path, compression):
    contents = bytearray(mcap_bytes(compression=compression))
    # Corrupt only the stored checksum; compressed bytes and indexes stay valid.
    offset = 8
    while contents[offset] != 6:  # Chunk opcode
        offset += 9 + struct.unpack_from("<Q", contents, offset + 1)[0]
    contents[offset + 9 + 24] ^= 1
    path = tmp_path / "corrupt.mcap"
    path.write_bytes(contents)
    with pytest.raises(Exception, match="(?i)crc"):
        daft.read_mcap(path).limit(1).collect()


@pytest.mark.parametrize("compression", list(CompressionType))
def test_overlapping_chunks_preserve_time_order_and_ties(tmp_path, compression):
    contents = mcap_bytes([1, 7, 4, 2, 8, 4, 3, 9, 4], compression=compression, chunk_size=350, payload=b"x" * 100)
    reader = make_reader(io.BytesIO(contents))
    assert len(reader.get_summary().chunk_indexes) >= 3
    expected = [msg.sequence for _, _, msg in reader.iter_messages()]
    path = tmp_path / "overlapping.mcap"
    path.write_bytes(contents)
    result = daft.read_mcap(path, batch_size=2).select("sequence").to_pydict()
    assert result == {"sequence": expected}


@pytest.mark.parametrize("advertise_ranges", [False, True])
def test_http_without_byte_range_support(advertise_ranges):
    contents = mcap_bytes(reversed(range(1000)), payload=b"x" * 100)
    assert len(contents) > 64 * 1024

    class FullResponseHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(contents)))
            if advertise_ranges:
                self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            self.do_HEAD()
            self.wfile.write(contents)

    server = ThreadingHTTPServer(("127.0.0.1", 0), FullResponseHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/test.mcap"
        # This is the previous implementation's read path.
        if not advertise_ranges:
            with daft.open_file(url, "rb") as source:
                assert len(list(make_reader(source, decoder_factories=[]).iter_messages())) == 1000
        assert daft.read_mcap(url).count_rows() == 1000
        assert daft.read_mcap(url).limit(3).select("log_time").to_pydict() == {"log_time": [0, 1, 2]}
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
