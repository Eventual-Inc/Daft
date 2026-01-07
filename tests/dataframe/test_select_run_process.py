from __future__ import annotations

import pytest

import daft
from daft.functions import format, run_process


def test_run_process_tokens_echo():
    df = daft.from_pydict({"a": ["hello", "good"], "b": ["world", "day"]})
    expr = run_process(["echo", df["a"], df["b"]])
    out = df.select(expr.alias("out")).to_pylist()
    assert out == [{"out": "hello world"}, {"out": "good day"}]


@pytest.mark.parametrize("text,expected", [("abc", 4), ("", 1)])
def test_run_process_shell_pipeline_wc(text: str, expected: int):
    df = daft.from_pydict({"x": [text]})
    expr = run_process(format("echo {} | wc -c | tr -d ' '", df["x"]), shell=True, return_dtype=int)
    out = df.select(expr.alias("n")).to_pylist()
    assert out == [{"n": expected}]


def test_run_process_on_error_ignore_and_raise():
    df = daft.from_pydict({"x": [1]})
    expr_ignore = run_process(df["x"], on_error="ignore")
    out_ignore = df.select(expr_ignore.alias("out")).to_pylist()
    assert out_ignore == [{"out": None}]

    with pytest.raises(daft.exceptions.DaftCoreException):
        expr_raise = run_process(df["x"], on_error="raise")
        df.select(expr_raise).collect()


def test_run_process_stderr_only_no_data():
    df = daft.from_pydict({"x": ["a"]})
    expr = run_process(format('sh -c "echo {} 1>&2"', df["x"]), shell=True)
    out = df.select(expr.alias("out")).to_pylist()
    assert out == [{"out": ""}]


def test_run_process_return_dtype_failure_to_int_none():
    df = daft.from_pydict({"x": ["a", "b"]})
    expr = run_process(format("echo {}", df["x"]), shell=True, return_dtype=int)
    out = df.select(expr.alias("n")).to_pylist()
    assert out == [{"n": None}, {"n": None}]


@pytest.mark.parametrize("text,expected", [("42", 42), ("-1", -1)])
def test_run_process_return_dtype_daft_datatype_int32(text: str, expected: int) -> None:
    df = daft.from_pydict({"x": [text]})
    expr = run_process(
        format("echo {}", df["x"]),
        shell=True,
        return_dtype=daft.DataType.int32(),
    )
    out = df.select(expr.alias("value")).to_pylist()
    assert out == [{"value": expected}]
