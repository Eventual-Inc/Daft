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
    ],
)
def test_str_left(binary_data_fixture, op, request):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=(lhs.datatype() == DataType.string()) and (rhs.datatype() == DataType.uint32()),
    )
