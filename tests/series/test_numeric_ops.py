from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft.datatype import DataType
from daft.exceptions import DaftCoreException
from daft.functions import bround, rint
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_series_numeric_abs(dtype) -> None:
    if pa.types.is_unsigned_integer(dtype):
        pydata = list(range(10))
    else:
        pydata = list(range(-10, 10))

    data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)

    abs_s = abs(s)

    assert abs_s.datatype() == DataType.from_arrow_type(dtype)

    assert abs_s.to_pylist() == list(map(abs, pydata))


def test_table_abs_bad_input() -> None:
    series = Series.from_pylist(["a", "b", "c"])

    with pytest.raises(ValueError, match="abs not implemented"):
        abs(series)


def test_float16_log2() -> None:
    data = pa.array([1.0, 2.0, 4.0, 8.0], type=pa.float16())
    s = Series.from_arrow(data)
    result = s.log2()
    assert result.datatype() == DataType.float16()
    assert result.to_pylist() == [0.0, 1.0, 2.0, 3.0]


def test_float16_log10() -> None:
    data = pa.array([1.0, 10.0, 100.0], type=pa.float16())
    s = Series.from_arrow(data)
    result = s.log10()
    assert result.datatype() == DataType.float16()
    assert result.to_pylist() == [0.0, 1.0, 2.0]


def test_float16_ln() -> None:
    import math

    data = pa.array([1.0, math.e], type=pa.float16())
    s = Series.from_arrow(data)
    result = s.ln()
    assert result.datatype() == DataType.float16()
    assert result.to_pylist()[0] == 0.0
    assert abs(result.to_pylist()[1] - 1.0) < 0.01


def test_float16_pow() -> None:
    data = pa.array([1.0, 2.0, 3.0], type=pa.float16())
    s = Series.from_arrow(data)
    result = s.pow(2.0)
    assert result.datatype() == DataType.float16()
    assert result.to_pylist() == [1.0, 4.0, 9.0]


def test_rint_banker_rounding() -> None:
    df = daft.from_pydict({"values": [2.5, 3.5, 4.5, -1.5]})
    result = df.with_column("rint", rint(df["values"])).collect()
    assert result.to_pydict()["rint"] == [2.0, 4.0, 4.0, -2.0]


def test_rint_integers_passthrough() -> None:
    df = daft.from_pydict({"values": [1, 2, 3]})
    result = df.with_column("rint", rint(df["values"])).collect()
    assert result.to_pydict()["rint"] == [1.0, 2.0, 3.0]


def test_rint_bad_input() -> None:
    df = daft.from_pydict({"values": ["a", "b", "c"]})
    with pytest.raises(DaftCoreException):
        df.with_column("rint", rint(df["values"])).collect()


def test_bround_no_decimals() -> None:
    df = daft.from_pydict({"values": [2.5, 3.5, 4.5, -1.5]})
    result = df.with_column("bround", bround(df["values"])).collect()
    assert result.to_pydict()["bround"] == [2.0, 4.0, 4.0, -2.0]


def test_bround_with_decimals() -> None:
    df = daft.from_pydict({"values": [2.55, 2.45, 3.55, -1.55]})
    result = df.with_column("bround", bround(df["values"], 1)).collect()
    assert result.to_pydict()["bround"] == [2.6, 2.4, 3.6, -1.6]


def test_bround_bad_input() -> None:
    df = daft.from_pydict({"values": ["a", "b", "c"]})
    with pytest.raises(DaftCoreException):
        df.with_column("bround", bround(df["values"])).collect()

def test_rint_float16() -> None:
    import pyarrow as pa
    data = pa.array([2.5, 3.5, 4.5, -1.5], type=pa.float16())
    df = daft.from_pydict({"values": data.to_pylist()})
    df = df.with_column("values", df["values"].cast(daft.DataType.float16()))
    result = df.with_column("rint", rint(df["values"])).collect()
    assert result.schema()["rint"].dtype == daft.DataType.float16()


def test_bround_float16() -> None:
    import pyarrow as pa
    data = pa.array([2.5, 3.5, 4.5, -1.5], type=pa.float16())
    df = daft.from_pydict({"values": data.to_pylist()})
    df = df.with_column("values", df["values"].cast(daft.DataType.float16()))
    result = df.with_column("bround", bround(df["values"])).collect()
    assert result.schema()["bround"].dtype == daft.DataType.float16()
    