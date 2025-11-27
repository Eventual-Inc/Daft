from __future__ import annotations

import os
import sys
import textwrap

import pandas as pd
import pytest

import daft
from daft.functions import shell_op
from daft.functions.shell import ShellOpOptions

SCRIPT_EMBEDDED = textwrap.dedent("""
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("positional", nargs="*")
    parser.add_argument("--start", type=int)
    parser.add_argument("--end", type=int)
    parser.add_argument("--flag", action="store_true")
    parser.add_argument("--sleep", type=int)
    parser.add_argument("--fail", action="store_true")
    args = parser.parse_args()

    start = args.start
    end = args.end
    if start is None and len(args.positional) >= 1:
        try:
            start = int(args.positional[0])
        except Exception:
            start = args.positional[0]
    if end is None and len(args.positional) >= 2:
        try:
            end = int(args.positional[1])
        except Exception:
            end = args.positional[1]

    # optional sleep
    if args.sleep:
        time.sleep(args.sleep)

    # print env var FOO if present
    foo = os.environ.get("FOO")
    if foo is not None:
        print(f"FOO={foo}")

    # stdout
    print(f"start={start},end={end}")
    # flag indication
    if args.flag:
        print("flag=true")
    # stderr for testing capture_stderr
    print(f"err={start}", file=sys.stderr)

    # fail with non-zero code if requested
    if args.fail:
        sys.exit(5)


if __name__ == "__main__":
    main()
""")


def _script_code() -> str:
    return SCRIPT_EMBEDDED


def _write_temp_script(tmp_path) -> str:
    script_path = str(tmp_path / "script.py")
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(_script_code())
    return script_path


def test_shell_op_positional_args(tmp_path):
    script_path = _write_temp_script(tmp_path)
    df = daft.from_pydict({"starts": [1, 3], "ends": [2, 4]})

    expr = shell_op(cmd=f"{sys.executable} {script_path}", params=[df["starts"], df["ends"]])
    res = df.select(expr.alias("out")).collect()

    out = res.to_pandas()
    expected = pd.DataFrame([{"out": "start=1,end=2"}, {"out": "start=3,end=4"}])
    pd.testing.assert_frame_equal(out, expected, check_dtype=False, check_names=False)


def test_shell_op_named_args_separate_and_equals(tmp_path):
    script_path = _write_temp_script(tmp_path)
    df = daft.from_pydict({"starts": [5, 7], "ends": [6, 8]})

    expr1 = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"]},
        options=ShellOpOptions(return_struct=False, arg_style="separate"),
    )
    rows1 = df.select(expr1.alias("out")).to_pandas()

    expected = pd.DataFrame([{"out": "start=5,end=6"}, {"out": "start=7,end=8"}])
    pd.testing.assert_frame_equal(rows1, expected, check_dtype=False, check_names=False)

    expr2 = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"]},
        options=ShellOpOptions(return_struct=False, arg_style="equals"),
    )
    rows2 = df.select(expr2.alias("out")).to_pandas()
    expected = pd.DataFrame([{"out": "start=5,end=6"}, {"out": "start=7,end=8"}])
    pd.testing.assert_frame_equal(rows2, expected, check_dtype=False, check_names=False)


def test_shell_op_boolean_flag(tmp_path):
    script_path = _write_temp_script(tmp_path)
    df = daft.from_pydict({"starts": [1], "ends": [2]})

    expr_true = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"], "--flag": True},
    )
    out_true = df.select(expr_true.alias("out")).to_pandas()
    assert any("flag=true" in row for row in out_true["out"])

    expr_false = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"], "--flag": False},
    )
    out_false = df.select(expr_false.alias("out")).collect().to_pandas()
    assert all("flag=true" not in row for row in out_false["out"])

    expr_none = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"], "--flag": None},
    )
    out_none = df.select(expr_none.alias("out")).collect().to_pandas()
    assert all("flag=true" not in row for row in out_none["out"])


def test_shell_op_timeout_ignore_struct(tmp_path):
    script_path = _write_temp_script(tmp_path)
    df = daft.from_pydict({"starts": [1], "ends": [2]})

    opts = ShellOpOptions(
        return_struct=True,
        capture_stderr=True,
        include_exit_code=True,
        include_duration_ms=True,
        timeout_seconds=0.01,
        max_concurrency=2,
        on_error="ignore",
        cwd=os.path.dirname(script_path),
    )
    expr = shell_op(
        cmd=f"{sys.executable} script.py",
        params={"--start": df["starts"], "--end": df["ends"], "--sleep": 1},
        options=opts,
    )

    res = df.select(expr).collect()
    rows = res.to_pandas()

    assert rows["stdout"][0] is None
    assert rows["stderr"][0] is not None and "timed out" in rows["stderr"][0]
    assert rows["exit_code"][0] == -1


def test_shell_op_on_error_handling(tmp_path):
    script_path = _write_temp_script(tmp_path)
    df = daft.from_pydict({"starts": [1], "ends": [2]})

    # raise
    opts_raise = ShellOpOptions(on_error="raise")
    expr_raise = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"], "--fail": True},
        options=opts_raise,
    )
    with pytest.raises(daft.exceptions.DaftCoreException):
        df.select(expr_raise.alias("out")).collect()

    # ignore: run_str returns None
    opts_ignore = ShellOpOptions(on_error="ignore")
    expr_ignore_str = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"], "--fail": True},
        options=opts_ignore,
    )
    rows_ignore = df.select(expr_ignore_str.alias("out")).collect().to_pandas()
    assert rows_ignore["out"][0] is None

    # ignore + struct: exit_code non-zero, stderr has content
    opts_struct = ShellOpOptions(on_error="ignore", return_struct=True, capture_stderr=True, include_exit_code=True)
    expr_ignore_struct = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"], "--fail": True},
        options=opts_struct,
    )
    res_struct = df.select(expr_ignore_struct).collect().to_pandas()
    assert res_struct["exit_code"][0] not in (None, 0)
    if res_struct["stderr"][0] is not None:
        assert isinstance(res_struct["stderr"][0], str)

    # with on_error='log': stdout None, exit_code non-zero, stderr is a string
    opts_log_struct = ShellOpOptions(on_error="log", return_struct=True, capture_stderr=True, include_exit_code=True)
    expr_log_struct = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params={"--start": df["starts"], "--end": df["ends"], "--fail": True},
        options=opts_log_struct,
    )
    res_log_struct = df.select(expr_log_struct).collect().to_pandas()
    assert res_log_struct["stdout"][0] is None
    assert res_log_struct["exit_code"][0] not in (None, 0)
    assert isinstance(res_log_struct["stderr"][0], str)


def test_shell_op_use_shell_pipeline():
    # Command through shell, with pipe
    cmd = "bash -c 'echo -n $0 | wc -c'"
    df = daft.from_pydict({"x": ["aaa", "bb", "c"]})
    expr = shell_op(cmd=cmd, params=[df["x"]], options=ShellOpOptions(use_shell=True))
    res = df.select(expr).collect().to_pandas()
    assert res["x"].tolist() == ["3", "2", "1"]


def test_shell_op_env_passing(tmp_path):
    script_path = _write_temp_script(tmp_path)
    df = daft.from_pydict({"starts": [1], "ends": [2]})
    opts = ShellOpOptions(env={"FOO": "BAR"})
    expr = shell_op(cmd=f"{sys.executable} {script_path}", params=[df["starts"], df["ends"]], options=opts)
    res = df.select(expr.alias("out")).collect().to_pandas()
    assert any("FOO=BAR" in row for row in res["out"])


def test_shell_op_use_shell_quotes_spaces(tmp_path):
    # Pass two positional args containing spaces; ensure they stay as single args when use_shell=True
    script_path = _write_temp_script(tmp_path)
    df = daft.from_pydict({"a": ["hello world"], "b": ["x y"]})
    expr = shell_op(
        cmd=f"{sys.executable} {script_path}",
        params=[df["a"], df["b"]],
        options=ShellOpOptions(use_shell=True),
    )
    res = df.select(expr.alias("out")).collect().to_pandas()
    assert res["out"].tolist() == ["start=hello world,end=x y"]
