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


import json

import daft


def _sole_add(tmp_path):
    log = tmp_path / "_delta_log" / "00000000000000000000.json"
    adds = [
        json.loads(line)["add"]
        for line in log.read_text().splitlines()
        if "add" in json.loads(line)
    ]
    assert len(adds) == 1
    return adds[0]


def _codec_of(tmp_path):
    import pyarrow.parquet as pq

    add = _sole_add(tmp_path)
    return pq.ParquetFile(tmp_path / add["path"]).metadata.row_group(0).column(0).compression


def test_default_is_snappy(tmp_path):
    daft.from_pydict({"a": [1, 2, 3]}).write_deltalake(str(tmp_path))
    assert _codec_of(tmp_path) == "SNAPPY"


@pytest.mark.parametrize("codec", ["none", "snappy", "gzip", "brotli", "lz4", "zstd"])
def test_codec_is_applied_and_table_is_readable(tmp_path, codec):
    daft.from_pydict({"a": [1, 2, 3], "s": ["x", "y", "z"]}).write_deltalake(
        str(tmp_path), compression=codec
    )
    expected = "UNCOMPRESSED" if codec == "none" else codec.upper()
    assert _codec_of(tmp_path) == expected

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("a")
    assert table.column("a").to_pylist() == [1, 2, 3]
    assert table.column("s").to_pylist() == ["x", "y", "z"]


def test_uncompressed_matches_none(tmp_path):
    daft.from_pydict({"a": [1]}).write_deltalake(str(tmp_path), compression="uncompressed")
    assert _codec_of(tmp_path) == "UNCOMPRESSED"


@pytest.mark.parametrize("codec", ["lz4_raw", "lzo"])
def test_unencodable_codec_rejected_before_anything_is_written(tmp_path, codec):
    with pytest.raises(ValueError, match=codec):
        daft.from_pydict({"a": [1]}).write_deltalake(str(tmp_path), compression=codec)
    assert not (tmp_path / "_delta_log").exists()


def test_mixed_codec_table_reads_correctly(tmp_path):
    """Old uncompressed files stay readable after the snappy default lands.

    Parquet records the codec per column chunk inside each file, so a table containing a
    mix of codecs decodes correctly.
    """
    import glob

    import pyarrow.parquet as pq

    p = str(tmp_path)
    daft.from_pydict({"a": [1, 2], "s": ["x", "y"]}).write_deltalake(p, compression="none")
    daft.from_pydict({"a": [3, 4], "s": ["z", "w"]}).write_deltalake(
        p, mode="append", compression="snappy"
    )
    daft.from_pydict({"a": [5], "s": ["q"]}).write_deltalake(
        p, mode="append", compression="zstd"
    )

    codecs = {
        pq.ParquetFile(f).metadata.row_group(0).column(0).compression
        for f in glob.glob(f"{p}/*.parquet")
    }
    assert codecs == {"UNCOMPRESSED", "SNAPPY", "ZSTD"}

    from_delta_rs = deltalake.DeltaTable(p).to_pyarrow_table().sort_by("a")
    assert from_delta_rs.column("a").to_pylist() == [1, 2, 3, 4, 5]
    from_daft = daft.read_deltalake(p).sort("a").to_pydict()
    assert from_daft["a"] == [1, 2, 3, 4, 5]
    assert from_daft["s"] == ["x", "y", "z", "w", "q"]


def _codecs_on_disk(path: str) -> set[str]:
    """Compression codec of every parquet data file under ``path``, recursively."""
    import glob

    import pyarrow.parquet as pq

    return {
        pq.ParquetFile(f).metadata.row_group(0).column(0).compression
        for f in glob.glob(f"{path}/**/*.parquet", recursive=True)
    }


def test_every_accepted_codec_coexists_in_one_table(tmp_path):
    """A Delta table holding all six accepted codecs reads back intact through both readers.

    Each append writes its own file with its own codec. Parquet stores the codec in each
    file's own metadata, so no reader needs to know the table is heterogeneous -- which is
    what makes the snappy default safe for tables written by older Daft.
    """
    p = str(tmp_path)
    # (codec, on-disk label, rows). "none" is spelled UNCOMPRESSED in parquet metadata.
    plan = [
        ("none", "UNCOMPRESSED", (1, "a")),
        ("snappy", "SNAPPY", (2, "b")),
        ("gzip", "GZIP", (3, "c")),
        ("brotli", "BROTLI", (4, "d")),
        ("lz4", "LZ4", (5, "e")),
        ("zstd", "ZSTD", (6, "f")),
    ]

    for i, (codec, _label, (n, s)) in enumerate(plan):
        mode = "append" if i else "error"
        daft.from_pydict({"a": [n], "s": [s]}).write_deltalake(p, mode=mode, compression=codec)

    assert _codecs_on_disk(p) == {label for _c, label, _r in plan}

    expected_a = [n for _c, _l, (n, _s) in plan]
    expected_s = [s for _c, _l, (_n, s) in plan]

    from_delta_rs = deltalake.DeltaTable(p).to_pyarrow_table().sort_by("a")
    assert from_delta_rs.column("a").to_pylist() == expected_a
    assert from_delta_rs.column("s").to_pylist() == expected_s

    from_daft = daft.read_deltalake(p).sort("a").to_pydict()
    assert from_daft["a"] == expected_a
    assert from_daft["s"] == expected_s


def test_mixed_codec_table_survives_projection_and_predicate(tmp_path):
    """Daft's own scan path decodes a heterogeneous table under pushdown.

    Reading every column of every file would hide a per-file decode bug behind the
    full-table read above; a projection + filter forces the scan to touch individual
    column chunks across files written with different codecs.
    """
    p = str(tmp_path)
    daft.from_pydict({"a": [1, 2], "s": ["x", "y"]}).write_deltalake(p, compression="none")
    daft.from_pydict({"a": [3, 4], "s": ["z", "w"]}).write_deltalake(p, mode="append", compression="gzip")
    daft.from_pydict({"a": [5, 6], "s": ["p", "q"]}).write_deltalake(p, mode="append", compression="zstd")

    assert _codecs_on_disk(p) == {"UNCOMPRESSED", "GZIP", "ZSTD"}

    # Projection only, and a predicate that straddles all three files.
    got = daft.read_deltalake(p).where(daft.col("a") >= 2).select("s").sort("s").to_pydict()
    assert got["s"] == ["p", "q", "w", "y", "z"]
    assert "a" not in got


def test_partitioned_table_mixes_codecs_across_partitions(tmp_path):
    """Codec is a per-file property, so partitions may disagree and still read correctly."""
    p = str(tmp_path)
    daft.from_pydict({"part": ["a", "a"], "v": [1, 2]}).write_deltalake(
        p, partition_cols=["part"], compression="none"
    )
    daft.from_pydict({"part": ["b", "b"], "v": [3, 4]}).write_deltalake(
        p, mode="append", partition_cols=["part"], compression="zstd"
    )

    assert _codecs_on_disk(p) == {"UNCOMPRESSED", "ZSTD"}

    from_delta_rs = deltalake.DeltaTable(p).to_pyarrow_table().sort_by("v")
    assert from_delta_rs.column("v").to_pylist() == [1, 2, 3, 4]
    assert from_delta_rs.column("part").to_pylist() == ["a", "a", "b", "b"]

    from_daft = daft.read_deltalake(p).sort("v").to_pydict()
    assert from_daft["v"] == [1, 2, 3, 4]
    assert from_daft["part"] == ["a", "a", "b", "b"]
