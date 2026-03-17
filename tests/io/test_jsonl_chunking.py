from __future__ import annotations

import gzip
import os

import daft


def _write_fixed_width_jsonl(path: str, *, rows: int, line_bytes: int, id_width: int) -> None:
    prefix = '{"id":"'
    mid = '","payload":"'
    suffix = '"}\n'
    min_bytes = len(prefix) + id_width + len(mid) + len(suffix)
    if line_bytes < min_bytes:
        raise ValueError(f"line_bytes too small: {line_bytes} < {min_bytes}")
    payload_len = line_bytes - min_bytes
    payload = "x" * payload_len
    with open(path, "w", encoding="utf-8", newline="") as f:
        for i in range(rows):
            f.write(f"{prefix}{str(i).zfill(id_width)}{mid}{payload}{suffix}")


def _write_fixed_width_jsonl_gz(path: str, *, rows: int, line_bytes: int, id_width: int) -> None:
    prefix = '{"id":"'
    mid = '","payload":"'
    suffix = '"}\n'
    min_bytes = len(prefix) + id_width + len(mid) + len(suffix)
    if line_bytes < min_bytes:
        raise ValueError(f"line_bytes too small: {line_bytes} < {min_bytes}")
    payload_len = line_bytes - min_bytes
    payload = "x" * payload_len
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        for i in range(rows):
            f.write(f"{prefix}{str(i).zfill(id_width)}{mid}{payload}{suffix}")


def test_jsonl_chunk_size_controls_partitioning(tmp_path: os.PathLike[str]) -> None:
    file_path = os.fspath(tmp_path / "data.jsonl")
    rows = 20_000
    _write_fixed_width_jsonl(file_path, rows=rows, line_bytes=128, id_width=8)

    df_small = daft.read_json(file_path, _chunk_size=4096)
    partitions_small = list(df_small.iter_partitions())
    assert len(partitions_small) > 1
    assert df_small.count_rows() == rows

    df_default = daft.read_json(file_path)
    partitions_default = list(df_default.iter_partitions())
    assert len(partitions_default) > 1
    assert df_default.count_rows() == rows

    df_large = daft.read_json(file_path, _chunk_size=10 * 1024 * 1024)
    partitions_large = list(df_large.iter_partitions())
    assert len(partitions_large) == 1
    assert df_large.count_rows() == rows


def test_jsonl_chunk_size_one_reads_correctly(tmp_path: os.PathLike[str]) -> None:
    file_path = os.fspath(tmp_path / "small.jsonl")
    rows = [
        {"id": 1, "payload": "a"},
        {"id": 2, "payload": "bb"},
        {"id": 3, "payload": "ccc"},
    ]
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        for row in rows:
            f.write(f'{{"id":{row["id"]},"payload":"{row["payload"]}"}}\n')

    df = daft.read_json(file_path, _chunk_size=1)
    partitions = list(df.iter_partitions())
    assert len(partitions) > 1
    assert df.to_pylist() == rows


def test_jsonl_gzip_chunk_size_controls_partitioning(tmp_path: os.PathLike[str]) -> None:
    file_path = os.fspath(tmp_path / "data.jsonl.gz")
    rows = 20_000
    _write_fixed_width_jsonl_gz(file_path, rows=rows, line_bytes=128, id_width=8)

    df_small = daft.read_json(file_path, _chunk_size=4096)
    partitions_small = list(df_small.iter_partitions())
    assert len(partitions_small) > 1
    assert df_small.count_rows() == rows

    df_large = daft.read_json(file_path, _chunk_size=10 * 1024 * 1024)
    partitions_large = list(df_large.iter_partitions())
    assert len(partitions_large) == 1
    assert df_large.count_rows() == rows
