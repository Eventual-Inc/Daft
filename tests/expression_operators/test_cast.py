from __future__ import annotations

import datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pac
import pytest

from daft import DataFrame
from daft.errors import ExpressionTypeError


def assert_chunkedarray_eq(treatment, expected):
    assert pac.all(
        pac.equal(pac.is_null(treatment), pac.is_null(expected))
    ).as_py(), f"Mismatch in nulls: {treatment} vs {expected}"
    if pa.types.is_floating(expected.type):
        assert pac.all(
            pac.equal(pac.is_nan(treatment), pac.is_nan(expected))
        ).as_py(), f"Mismatch in nans: {treatment} vs {expected}"
    not_nans_or_nulls = pac.invert(pac.is_null(expected, nan_is_null=True))
    assert pac.all(
        pac.array_filter(pac.equal(treatment, expected), not_nans_or_nulls)
    ).as_py(), f"{treatment} != {expected}"


INT_DATA = [1, 2, None]
FLOAT_DATA = [1.9, float("nan"), None]
LOGICAL_DATA = [True, False, None]
STRING_DATA_INT = ["1", "2", None]
STRING_DATA_BOOL = ["true", "false", None]
STRING_DATA_DATE = ["1994-11-04", "1994-11-05", None]
DATE_DATA = [datetime.date(1994, 11, 4), datetime.date(1994, 11, 5), None]
BYTES_DATA = [b"a", b"b", None]


@pytest.mark.parametrize(
    ["before", "to_type", "expected"],
    [
        # STRING
        pytest.param(INT_DATA, str, pa.chunked_array([["1", "2", None]]), id="cast_int_to_str"),
        pytest.param(FLOAT_DATA, str, pa.chunked_array([["1.9", "nan", None]]), id="cast_float_to_str"),
        pytest.param(LOGICAL_DATA, str, pa.chunked_array([["true", "false", None]]), id="cast_logical_to_str"),
        pytest.param(STRING_DATA_INT, str, pa.chunked_array([STRING_DATA_INT]), id="cast_str_to_str"),
        pytest.param(DATE_DATA, str, None, id="cast_date_to_str"),  # should be handled with a .str.strftime instead
        pytest.param(
            BYTES_DATA, str, None, id="cast_bytes_to_str"
        ),  # should be handled with a .bytes.decode("utf-8") instead
        # INTEGER
        pytest.param(INT_DATA, int, pa.chunked_array([INT_DATA]), id="cast_int_to_int"),
        pytest.param(FLOAT_DATA, int, pa.chunked_array([[1, 0, None]]), id="cast_float_to_int"),
        pytest.param(LOGICAL_DATA, int, pa.chunked_array([[1, 0, None]]), id="cast_logical_to_int"),
        pytest.param(STRING_DATA_INT, int, None, id="cast_str_to_int"),  # should be handled with .str.to_int() instead
        pytest.param(DATE_DATA, int, None, id="cast_date_to_int"),  # should be handled with .dt.unix() instead
        pytest.param(BYTES_DATA, int, None, id="cast_bytes_to_int"),
        # FLOAT
        pytest.param(FLOAT_DATA, float, pa.chunked_array([FLOAT_DATA]), id="cast_float_to_float"),
        pytest.param(INT_DATA, float, pa.chunked_array([[1.0, 2.0, None]]), id="cast_int_to_float"),
        pytest.param(LOGICAL_DATA, float, None, id="cast_logical_to_float"),
        pytest.param(
            STRING_DATA_INT, float, None, id="cast_str_to_float"
        ),  # should be handled with .str.to_float() instead
        pytest.param(DATE_DATA, float, None, id="cast_date_to_float"),
        pytest.param(BYTES_DATA, float, None, id="cast_bytes_to_float"),
        # LOGICAL
        # "casts" for bool are usually achieved with comparison operators instead
        pytest.param(LOGICAL_DATA, bool, pa.chunked_array([LOGICAL_DATA]), id="cast_logical_to_logical"),
        pytest.param(INT_DATA, bool, None, id="cast_int_to_logical"),
        pytest.param(FLOAT_DATA, bool, None, id="cast_float_to_logical"),
        pytest.param(STRING_DATA_BOOL, bool, None, id="cast_str_to_logical"),
        pytest.param(DATE_DATA, bool, None, id="cast_date_to_logical"),
        pytest.param(BYTES_DATA, bool, None, id="cast_bytes_to_logical"),
        # DATE
        pytest.param(DATE_DATA, datetime.date, pa.chunked_array([DATE_DATA]), id="cast_date_to_date"),
        pytest.param(INT_DATA, datetime.date, None, id="cast_int_to_date"),
        pytest.param(FLOAT_DATA, datetime.date, None, id="cast_float_to_date"),
        pytest.param(LOGICAL_DATA, datetime.date, None, id="cast_logical_to_date"),
        pytest.param(
            STRING_DATA_DATE, datetime.date, None, id="cast_str_to_date"
        ),  # should be handled with .str.strptime() instead
        pytest.param(BYTES_DATA, datetime.date, None, id="cast_bytes_to_date"),
        # BYTES
        pytest.param(BYTES_DATA, bytes, pa.chunked_array([BYTES_DATA]), id="cast_bytes_to_bytes"),
        pytest.param(INT_DATA, bytes, None, id="cast_int_to_bytes"),
        pytest.param(FLOAT_DATA, bytes, None, id="cast_float_to_bytes"),
        pytest.param(LOGICAL_DATA, bytes, None, id="cast_logical_to_bytes"),
        pytest.param(
            STRING_DATA_DATE, bytes, None, id="cast_str_to_bytes"
        ),  # should be handled with .str.encode("utf-8") instead
        pytest.param(DATE_DATA, bytes, None, id="cast_date_to_bytes"),
    ],
)
def test_cast_primitives(before, to_type, expected):
    data = {"before": before}
    df = DataFrame.from_pydict(data)

    expect_not_implemented = expected is None
    if expect_not_implemented:
        with pytest.raises(ExpressionTypeError):
            df.with_column("after", df["before"].cast(to_type))
        return

    df = df.with_column("after", df["before"].cast(to_type))
    df.collect()
    collected = df.to_pydict()
    assert_chunkedarray_eq(collected["after"], expected)


class MyCastableObj:
    def get_string(self) -> str:
        return "foo"

    def get_integer(self) -> int:
        return 0

    def get_float(self) -> float:
        return 0.5

    def get_bool(self) -> bool:
        return True

    def get_bytes(self) -> bytes:
        return b"foo"

    def get_date(self) -> datetime.date:
        return datetime.date(1994, 1, 1)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, MyCastableObj)


PY_DATA = [MyCastableObj(), MyCastableObj(), None]


@pytest.mark.parametrize(
    ["before", "method_to_run", "to_type", "expected"],
    [
        # (PY_DATA, None, MyCastableObj, PY_DATA),
        (PY_DATA, "get_integer", int, pa.chunked_array([[0, 0, None]])),
        (PY_DATA, "get_float", float, pa.chunked_array([[0.5, 0.5, None]])),
        (PY_DATA, "get_bool", bool, pa.chunked_array([[True, True, None]])),
        (PY_DATA, "get_string", str, pa.chunked_array([["foo", "foo", None]])),
        (PY_DATA, "get_bytes", bytes, pa.chunked_array([[b"foo", b"foo", None]])),
        (
            PY_DATA,
            "get_date",
            datetime.date,
            pa.chunked_array([[datetime.date(1994, 1, 1), datetime.date(1994, 1, 1), None]]),
        ),
    ],
)
def test_cast_py(before, method_to_run, to_type, expected):
    data = {"before": before}
    df = DataFrame.from_pydict(data)

    if method_to_run is not None:
        df = df.with_column(
            "before",
            df["before"].apply(
                lambda obj: getattr(obj, method_to_run)() if obj is not None else None, return_type=object
            ),
        )

    df = df.with_column("after", df["before"].cast(to_type))
    df.collect()
    collected = df.to_pydict()

    if isinstance(expected, pa.ChunkedArray):
        assert_chunkedarray_eq(collected["after"], expected)
    else:
        assert collected["after"] == expected
