from __future__ import annotations

import pytest

deltalake = pytest.importorskip("deltalake")

from daft.io.delta_lake.delta_lake_write import normalize_delta_compression

# Each rejected codec must explain its OWN reason, not a generic "unsupported".
_REJECTED_REASON_SUBSTRING = {
    "lz4_raw": "cannot encode",
    "lzo": "lzo",
}


@pytest.mark.parametrize("codec", ["none", "snappy", "gzip", "brotli", "lz4", "zstd"])
def test_accepted_codecs_pass_through(codec):
    assert normalize_delta_compression(codec) == codec


def test_uncompressed_is_an_alias_for_none():
    assert normalize_delta_compression("uncompressed") == "none"


@pytest.mark.parametrize("codec", ["SNAPPY", "Snappy", "ZSTD", "UNCOMPRESSED", " lz4 "])
def test_codec_names_are_case_insensitive_and_stripped(codec):
    cleaned = codec.strip().lower()
    expected = "none" if cleaned == "uncompressed" else cleaned
    assert normalize_delta_compression(codec) == expected


@pytest.mark.parametrize("codec", ["lz4_raw", "lzo", "LZO", "  lzo  "])
def test_pyarrow_unencodable_codecs_are_rejected_with_their_own_reason(codec):
    with pytest.raises(ValueError) as excinfo:
        normalize_delta_compression(codec)
    msg = str(excinfo.value)
    # Echoes the raw input so the user can find it in their code.
    assert codec in msg
    # Names the real reason, and did NOT fall through to the generic branch.
    assert "pyarrow" in msg.lower()
    assert "unsupported compression codec" not in msg.lower()
    assert _REJECTED_REASON_SUBSTRING[codec.strip().lower()] in msg.lower()


def test_lzo_really_is_unencodable_by_pyarrow():
    """Guard the reason we reject lzo: it CONSTRUCTS fine and fails on first write.

    A capability probe that writes zero rows wrongly reports lzo as supported.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    tbl = pa.table({"a": list(range(100))})
    buf = pa.BufferOutputStream()
    writer = pq.ParquetWriter(buf, tbl.schema, compression="lzo")  # succeeds
    with pytest.raises(OSError, match="not supported by the C\\+\\+ implementation"):
        writer.write_table(tbl)


def test_unknown_codec_is_rejected_and_lists_accepted():
    with pytest.raises(ValueError) as excinfo:
        normalize_delta_compression("bogus")
    msg = str(excinfo.value)
    assert "bogus" in msg
    assert "snappy" in msg and "zstd" in msg and "uncompressed" in msg
