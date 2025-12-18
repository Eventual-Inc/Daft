from __future__ import annotations

import csv

import pyarrow.parquet as pq

import daft


def test_write_csv_default_delimiter(tmp_path):
    df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    out_dir = tmp_path / "out_default"
    df.write_csv(str(out_dir))

    files = list(out_dir.glob("*.csv"))
    assert len(files) > 0

    for f in files:
        with open(f) as csvfile:
            content = csvfile.read()
            assert "," in content

            csvfile.seek(0)
            reader = csv.reader(csvfile)
            header = next(reader)
            assert header == ["x", "y"]
            rows = list(reader)
            assert rows[0] == ["1", "a"]


def test_write_csv_custom_delimiter(tmp_path):
    df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    out_dir = tmp_path / "out_pipe"
    delimiter = "|"
    df.write_csv(str(out_dir), delimiter=delimiter)

    files = list(out_dir.glob("*.csv"))
    assert len(files) > 0

    for f in files:
        with open(f) as csvfile:
            content = csvfile.read()
            assert "|" in content
            assert "," not in content

            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter="|")
            header = next(reader)
            assert header == ["x", "y"]
            rows = list(reader)
            assert rows[0] == ["1", "a"]


def test_write_csv_tab_delimiter(tmp_path):
    df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    out_dir = tmp_path / "out_tab"
    delimiter = "\t"
    df.write_csv(str(out_dir), delimiter=delimiter)

    files = list(out_dir.glob("*.csv"))
    assert len(files) > 0

    for f in files:
        with open(f) as csvfile:
            content = csvfile.read()
            assert "\t" in content

            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter="\t")
            header = next(reader)
            assert header == ["x", "y"]
            rows = list(reader)
            assert rows[0] == ["1", "a"]


def test_write_parquet_config(tmp_path):
    df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    out_dir = tmp_path / "out_parquet"
    df.write_parquet(str(out_dir))

    files = list(out_dir.glob("*.parquet"))
    assert len(files) > 0

    # Verify we can read it back
    table = pq.read_table(files[0])
    assert table.column("x").to_pylist() == [1, 2, 3]
    assert table.column("y").to_pylist() == ["a", "b", "c"]


def test_write_json_config(tmp_path):
    # JSON write is only supported for native runner currently
    df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    out_dir = tmp_path / "out_json"
    df.write_json(str(out_dir))

    files = list(out_dir.glob("*.json"))
    assert len(files) > 0

    # Simple verification of content
    with open(files[0]) as f:
        lines = f.readlines()
        assert len(lines) == 3
        assert '"x":1' in lines[0]
        assert '"y":"a"' in lines[0]
