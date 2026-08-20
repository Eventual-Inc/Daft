from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import DataType as dt
from daft import col, lit
from daft.functions import seq


def test_seq_basic():
    df = daft.from_pydict({"n": [3, 5, 0]})
    result = df.with_column("indices", seq(col("n"))).to_pydict()
    assert result["indices"] == [[0, 1, 2], [0, 1, 2, 3, 4], []]


def test_seq_return_dtype():
    df = daft.from_pydict({"n": [3]})
    out = df.select(seq(col("n")).alias("s"))
    assert out.schema()["s"].dtype == dt.list(dt.uint64())


def test_seq_single_element():
    df = daft.from_pydict({"n": [1]})
    assert df.with_column("s", seq(col("n"))).to_pydict()["s"] == [[0]]


def test_seq_all_zeros():
    df = daft.from_pydict({"n": [0, 0, 0]})
    assert df.with_column("s", seq(col("n"))).to_pydict()["s"] == [[], [], []]


def test_seq_null_propagation():
    df = daft.from_pydict({"n": [2, None, 4]})
    assert df.with_column("s", seq(col("n"))).to_pydict()["s"] == [[0, 1], None, [0, 1, 2, 3]]


def test_seq_all_nulls():
    df = daft.from_pydict({"n": [None, None]}).select(col("n").cast(dt.int64()))
    assert df.with_column("s", seq(col("n"))).to_pydict()["s"] == [None, None]


def test_seq_with_literal():
    df = daft.from_pydict({"x": [1, 2, 3]})
    result = df.select(seq(lit(3)).alias("s")).to_pydict()
    assert result["s"] == [[0, 1, 2], [0, 1, 2], [0, 1, 2]]


@pytest.mark.parametrize("arrow_type", [pa.int8(), pa.int16(), pa.int32(), pa.uint16(), pa.uint32()])
def test_seq_accepts_narrower_integers(arrow_type):
    df = daft.from_arrow(pa.table({"n": pa.array([1, 2, 3], type=arrow_type)}))
    assert df.with_column("s", seq(col("n"))).to_pydict()["s"] == [[0], [0, 1], [0, 1, 2]]


def test_seq_negative_errors():
    df = daft.from_pydict({"n": [3, -1]})
    with pytest.raises(Exception, match="seq.* requires non-negative n"):
        df.with_column("s", seq(col("n"))).collect()


def test_seq_rejects_float_input():
    df = daft.from_pydict({"n": [1.5, 2.0]})
    with pytest.raises(Exception, match="integer type"):
        df.with_column("s", seq(col("n"))).collect()


def test_seq_rejects_string_input():
    df = daft.from_pydict({"n": ["3"]})
    with pytest.raises(Exception, match="integer type"):
        df.with_column("s", seq(col("n"))).collect()


def test_seq_preserves_input_column_name():
    """The implementation names the output Field after the input column; aliasing overrides."""
    df = daft.from_pydict({"n": [2]})
    aliased = df.select(seq(col("n")).alias("custom")).to_pydict()
    assert list(aliased.keys()) == ["custom"]
    assert aliased["custom"] == [[0, 1]]
