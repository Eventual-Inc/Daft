from __future__ import annotations

import pytest

import daft


def _make_skippable_dir(tmp_path, files):
    d = tmp_path / "json-data-skippable"
    d.mkdir()
    for name, content in files.items():
        (d / name).write_text(content, encoding="utf-8")
    return d


def test_read_json_skip_empty_files(tmp_path):
    d = _make_skippable_dir(
        tmp_path,
        {
            "valid1.json": '[{"a": 1, "b": 2}]',
            "valid2.json": '[{"a": 3, "b": 4}]',
            "empty.json": "",
        },
    )

    df = daft.read_json(str(d), skip_empty_files=True)
    # Only empty files are skipped; directory contains one empty and two valid files.
    assert len(df.schema()) == 2
    assert df.count_rows() == 2


def test_read_json_no_skip_empty_files(tmp_path):
    d = _make_skippable_dir(tmp_path, {"empty.json": ""})

    with pytest.raises(Exception, match="Empty JSON file"):
        daft.read_json(str(d / "empty.json"), skip_empty_files=False)


def test_read_json_no_skip_whitespace_files(tmp_path):
    d = _make_skippable_dir(tmp_path, {"whitespace.json": "   \n\t  "})

    with pytest.raises(Exception, match="Invalid JSON format"):
        daft.read_json(str(d / "whitespace.json"), skip_empty_files=False)

    with pytest.raises(Exception, match="Invalid JSON format"):
        daft.read_json(str(d / "whitespace.json"), skip_empty_files=True)


def test_read_json_skip_multiple_empty_files_in_dir(tmp_path):
    d = _make_skippable_dir(
        tmp_path,
        {
            "empty1.json": "",
            "empty2.json": "",
            "valid.json": '[{"a": 10}]',
            "empty3.json": "",
        },
    )

    df = daft.read_json(str(d), skip_empty_files=True)
    # Multiple empties must be skipped; only valid.json contributes rows and schema.
    assert len(df.schema()) == 1
    assert df.count_rows() == 1


def test_read_json_sparse_column_with_schema_hints(tmp_path):
    import json

    file_path = tmp_path / "sparse_data.jsonl"
    with file_path.open("w") as f:
        for i in range(50000):
            f.write(json.dumps({"name": f"Person{i}", "id": i}) + "\n")
        sparse_row = {"name": "Alice", "id": 99999, "sound": "hello", "complex_data": {"freq": 440, "type": "sine"}}
        f.write(json.dumps(sparse_row) + "\n")

    df_no_hint = daft.read_json(str(file_path))
    assert "sound" not in df_no_hint.column_names
    assert "complex_data" not in df_no_hint.column_names

    with pytest.raises(ValueError, match="FieldNotFound.*sound"):
        df_no_hint.where(daft.col("sound").not_null()).collect()

    schema_hints = {
        "sound": daft.DataType.string(),
        "complex_data": daft.DataType.struct({"freq": daft.DataType.int64(), "type": daft.DataType.string()}),
    }
    df_with_hint = daft.read_json(str(file_path), schema=schema_hints)
    assert "name" in df_with_hint.column_names
    assert "id" in df_with_hint.column_names
    assert "sound" in df_with_hint.column_names
    assert "complex_data" in df_with_hint.column_names

    res = df_with_hint.where(daft.col("sound").not_null()).collect()
    assert len(res) == 1
    assert res.to_pydict()["sound"][0] == "hello"
