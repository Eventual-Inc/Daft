from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.series import Series
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda data, pat: data.str.contains(pat), id="contains"),
        pytest.param(lambda data, pat: data.str.startswith(pat), id="startswith"),
        pytest.param(lambda data, pat: data.str.endswith(pat), id="endswith"),
        pytest.param(lambda data, pat: data.str.endswith(pat), id="split"),
        pytest.param(lambda data, pat: data.str.endswith(pat), id="match"),
        pytest.param(lambda data, pat: data.str.concat(pat), id="concat"),
        pytest.param(lambda data, pat: data.str.extract(pat), id="extract"),
        pytest.param(lambda data, pat: data.str.extract_all(pat), id="extract_all"),
        pytest.param(lambda data, pat: data.str.find(pat), id="find"),
        pytest.param(lambda data, pat: data.str.replace(pat, pat), id="replace"),
        pytest.param(lambda data, pat: data.str.like(pat), id="like"),
        pytest.param(lambda data, pat: data.str.ilike(pat), id="ilike"),
    ],
)
def test_str_compares(binary_data_fixture, op, request):
    lhs, rhs = binary_data_fixture
    if "concat" in request.node.callspec.id and (
        lhs.datatype() != DataType.string() or rhs.datatype() != DataType.string()
    ):
        # Only test concat with strings, since other types will have their own semantics
        # for the underlying + operator.
        return
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=(lhs.datatype() == DataType.string()) and (rhs.datatype() == DataType.string()),
    )


def test_str_length():
    s = Series.from_arrow(pa.array(["1", "2", "3"]), name="arg")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col(s.name()).str.length(),
        run_kernel=s.str.length,
        resolvable=True,
    )


def test_str_lower():
    s = Series.from_arrow(pa.array(["Foo", "BarBaz", "QUUX"]), name="arg")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col(s.name()).str.lower(),
        run_kernel=s.str.lower,
        resolvable=True,
    )


def test_str_upper():
    s = Series.from_arrow(pa.array(["Foo", "BarBaz", "quux"]), name="arg")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col(s.name()).str.upper(),
        run_kernel=s.str.lower,
        resolvable=True,
    )


def test_str_lstrip():
    s = Series.from_arrow(pa.array(["\ta\t", "\nb\n", "\vc\t", " c\t"]), name="arg")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col(s.name()).str.lstrip(),
        run_kernel=s.str.lstrip,
        resolvable=True,
    )


def test_str_rstrip():
    s = Series.from_arrow(pa.array(["\ta\t", "\nb\n", "\vc\t", "\tc "]), name="arg")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col(s.name()).str.rstrip(),
        run_kernel=s.str.rstrip,
        resolvable=True,
    )


def test_str_reverse():
    s = Series.from_arrow(pa.array(["abc", "def", "ghi", None, ""]), name="arg")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col(s.name()).str.reverse(),
        run_kernel=s.str.reverse,
        resolvable=True,
    )


def test_str_capitalize():
    s = Series.from_arrow(pa.array(["foo", "Bar", "BUZZ"]), name="arg")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col(s.name()).str.capitalize(),
        run_kernel=s.str.capitalize,
        resolvable=True,
    )


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda data, pat: data.str.left(pat), id="left"),
        pytest.param(lambda data, pat: data.str.right(pat), id="right"),
        pytest.param(lambda data, pat: data.str.repeat(pat), id="repeat"),
        pytest.param(lambda data, pat: data.str.substr(pat, pat), id="susbtr"),
    ],
)
def test_str_int_compares(binary_data_fixture, op, request):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=(lhs.datatype() == DataType.string())
        and (
            rhs.datatype() == DataType.int64()
            or rhs.datatype() == DataType.int32()
            or rhs.datatype() == DataType.int16()
            or rhs.datatype() == DataType.int8()
            or rhs.datatype() == DataType.uint64()
            or rhs.datatype() == DataType.uint32()
            or rhs.datatype() == DataType.uint16()
            or rhs.datatype() == DataType.uint8()
        ),
    )


def test_str_rpad():
    s = Series.from_arrow(pa.array(["foo", "abcdef", "quux"]), name="col")
    zeroes = Series.from_arrow(pa.array([0, 0, 0]), name="zeroes")
    emptystrings = Series.from_arrow(pa.array(["", "", ""]), name="emptystrings")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s, zeroes, emptystrings],
        expr=col("col").str.rpad(col("zeroes"), col("emptystrings")),
        run_kernel=lambda: s.str.rpad(zeroes, emptystrings),
        resolvable=True,
    )


def test_str_lpad():
    s = Series.from_arrow(pa.array(["foo", "abcdef", "quux"]), name="col")
    zeroes = Series.from_arrow(pa.array([0, 0, 0]), name="zeroes")
    emptystrings = Series.from_arrow(pa.array(["", "", ""]), name="emptystrings")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s, zeroes, emptystrings],
        expr=col("col").str.lpad(col("zeroes"), col("emptystrings")),
        run_kernel=lambda: s.str.lpad(zeroes, emptystrings),
        resolvable=True,
    )


def test_str_to_date():
    s = Series.from_arrow(pa.array(["2021-01-01", None, "2021-01-02", "2021-01-03", "2021-01-04"]), name="col")
    format = "%Y-%m-%d"
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col("col").str.to_date(format),
        run_kernel=lambda: s.str.to_date(format),
        resolvable=True,
    )


def test_str_to_datetime():
    s = Series.from_arrow(
        pa.array(["2021-01-01 00:00:00", None, "2021-01-02 00:00:00", "2021-01-03 00:00:00", "2021-01-04 00:00:00"]),
        name="col",
    )
    format = "%Y-%m-%d %H:%M:%S"

    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col("col").str.to_datetime(format),
        run_kernel=lambda: s.str.to_datetime(format),
        resolvable=True,
    )


@pytest.mark.parametrize("remove_punct", [False, True])
@pytest.mark.parametrize("lowercase", [False, True])
@pytest.mark.parametrize("nfd_unicode", [False, True])
@pytest.mark.parametrize("white_space", [False, True])
def test_str_normalize(remove_punct, lowercase, nfd_unicode, white_space):
    s = Series.from_arrow(pa.array(["hello world", "Hello, world!", "Hêllø,   \nworłd!!"]), name="col")
    assert_typing_resolve_vs_runtime_behavior(
        data=[s],
        expr=col("col").str.normalize(
            remove_punct=remove_punct,
            lowercase=lowercase,
            nfd_unicode=nfd_unicode,
            white_space=white_space,
        ),
        run_kernel=lambda: s.str.normalize(
            remove_punct=remove_punct,
            lowercase=lowercase,
            nfd_unicode=nfd_unicode,
            white_space=white_space,
        ),
        resolvable=True,
    )
