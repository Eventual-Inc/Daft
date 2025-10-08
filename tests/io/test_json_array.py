from __future__ import annotations

import json

import pytest

import daft


def test_read_json_array():
    df = daft.read_json("tests/assets/json-data/sample1.json")
    with open("tests/assets/json-data/sample1.json") as f:
        expected = json.load(f)

    assert df.to_pylist() == expected


def test_read_json_multi():
    df = daft.read_json(["tests/assets/json-data/sample1.json", "tests/assets/json-data/sample2.json"])

    assert df.count_rows() == 25


def test_read_json_schema_is_correct():
    df = daft.read_json("tests/assets/json-data/sample1.json")
    expected_schema = daft.Schema.from_pydict(
        {"x": daft.DataType.int64(), "y": daft.DataType.string(), "z": daft.DataType.bool()}
    )

    assert df.schema() == expected_schema


def test_read_empty():
    with pytest.raises(Exception, match="Empty JSON file"):
        daft.read_json("tests/assets/json-data/empty")


def test_read_non_array():
    with pytest.raises(Exception, match="Invalid JSON format"):
        daft.read_json("tests/assets/json-data/baddata1.json")
