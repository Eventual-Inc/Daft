from daft.dataframe import dataframe
from daft.io._file import FileSource
from daft.io._json2 import JsonColumn, JsonColumns, JsonSource, JsonStrategy
from daft.schema import DataType

_RESOURCES = "tests/io/resources/test_json"

_COLUMNS = JsonColumns(
    [
        JsonColumn.path("a", DataType.int64()),
        JsonColumn.path("b", DataType.bool()),
    ]
)

_EXPECTED = [
    {"a": 1, "b": True},
    {"a": 2, "b": True},
    {"a": 3, "b": False},
    {"a": 4, "b": False},
]


def file_source(pattern: str):
    return FileSource(f"{_RESOURCES}/{pattern}")


def test_read_json__row_jsonl():
    """Single JSONL value with JSON strategy."""
    json_source = JsonSource(
        source=file_source("row_*.jsonl"),
        strategy=JsonStrategy.JSON,
        columns=_COLUMNS,
    )
    assert dataframe(json_source).sort("a").to_pylist() == _EXPECTED


def test_read_json__obj_json():
    """Single JSON object with JSON strategy."""
    json_source = JsonSource(
        source=FileSource("tests/io/resources/test_json/obj_*.json"),
        strategy=JsonStrategy.JSON,
        columns=_COLUMNS,
    )
    assert dataframe(json_source).sort("a").to_pylist() == _EXPECTED


def test_read_json__rows_jsonl():
    """Multiple JSONL objects with JSONL strategy."""
    json_source = JsonSource(
        source=FileSource("tests/io/resources/test_json/rows_*.jsonl"),
        strategy=JsonStrategy.JSONL,
        columns=_COLUMNS,
    )
    assert dataframe(json_source).sort("a").to_pylist() == _EXPECTED


def test_read_json__objs_jsons():
    """Multiple JSON objects with JSONS strategy."""
    json_source = JsonSource(
        source=FileSource("tests/io/resources/test_json/array_*.json"),
        strategy=JsonStrategy.JSONS,
        columns=_COLUMNS,
    )
    assert dataframe(json_source).sort("a").to_pylist() == _EXPECTED
