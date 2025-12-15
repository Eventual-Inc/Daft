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
