from __future__ import annotations

import pytest

import daft
from daft import col, lit
from daft.exceptions import DaftCoreException
from daft.functions import concat_ws


def test_basic_separator():
    df = daft.from_pydict({"a": ["foo"], "b": ["bar"]})
    result = df.select(concat_ws(",", col("a"), col("b"))).to_pydict()
    assert result["a"] == ["foo,bar"]


def test_null_skipping():
    # nulls are skipped, not propagated
    df = daft.from_pydict({"a": ["foo"], "b": [None], "c": ["bar"]})
    result = df.select(concat_ws(",", col("a"), col("b"), col("c"))).to_pydict()
    assert result["a"] == ["foo,bar"]


def test_all_nulls_returns_null():
    df = daft.from_pydict({"a": [None], "b": [None]})
    result = df.select(concat_ws(",", col("a"), col("b"))).to_pydict()
    assert result["a"] == [None]


def test_single_non_null_no_separator():
    # only one non-null: no leading/trailing separator
    df = daft.from_pydict({"a": [None], "b": ["only"]})
    result = df.select(concat_ws(",", col("a"), col("b"))).to_pydict()
    assert result["a"] == ["only"]


def test_mixed_literals_and_columns():
    df = daft.from_pydict({"id": ["42", "43"]})
    result = df.select(concat_ws("-", lit("prefix"), col("id"), lit("suffix")).alias("result")).to_pydict()
    assert result["result"] == ["prefix-42-suffix", "prefix-43-suffix"]


def test_multiple_rows():
    df = daft.from_pydict(
        {
            "first": ["Alice", "Bob", None],
            "last": ["Smith", None, "Jones"],
        }
    )
    result = df.select(concat_ws(" ", col("first"), col("last"))).to_pydict()
    assert result["first"] == ["Alice Smith", "Bob", "Jones"]


def test_file_path_building():
    df = daft.from_pydict({"bucket": ["my-bucket"], "prefix": ["data"], "filename": ["file.csv"]})
    result = df.select(concat_ws("/", col("bucket"), col("prefix"), col("filename"))).to_pydict()
    assert result["bucket"] == ["my-bucket/data/file.csv"]


def test_no_exprs_raises():
    with pytest.raises(DaftCoreException, match="concat_ws requires at least one expression argument"):
        daft.from_pydict({"a": ["x"]}).select(concat_ws(","))


def test_non_string_column_raises():
    df = daft.from_pydict({"a": [1, 2, 3]})
    with pytest.raises(DaftCoreException, match="expects string inputs"):
        df.select(concat_ws(",", col("a"))).collect()


def test_empty_separator():
    df = daft.from_pydict({"a": ["foo"], "b": ["bar"]})
    result = df.select(concat_ws("", col("a"), col("b"))).to_pydict()
    assert result["a"] == ["foobar"]
