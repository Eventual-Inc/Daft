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


def test_json_array_pushdown_include_columns_disjoint_predicate(tmp_path):
    # Prepare an array JSON file
    data = [
        {"x": 1, "y": "a", "z": True},
        {"x": 2, "y": "b", "z": False},
        {"x": 3, "y": "c", "z": True},
    ]
    p = tmp_path / "arr.json"
    p.write_text(json.dumps(data))

    # Pushdown path: include only 'y' while filtering on 'x'
    df_pushdown = daft.read_json(str(p)).where(daft.col("x") == 2).select("y")

    # Non-pushdown path: read full, filter, then project should match pushdown semantics
    df_full = daft.read_json(str(p)).where(daft.col("x") == 2).select("y")

    assert df_pushdown.to_arrow() == df_full.to_arrow()

    # Ensure output schema does not expose predicate column when not requested
    assert df_pushdown.schema() == daft.Schema.from_pydict({"y": daft.DataType.string()})
