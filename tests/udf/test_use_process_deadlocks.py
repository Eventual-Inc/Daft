"""Regression tests for use_process=True UDF subprocess deadlocks.

See issue #6762 for the full root-cause analysis.

Two distinct deadlocks live in UdfHandle's stdout plumbing
(daft/execution/udf.py). Both stem from the parent blocking on
handle_conn.recv() before draining the child's stdout pipe.

- Large stderr output (>64 KiB per batch) fills the OS pipe buffer; the child
  blocks in write() before reaching conn.send(_SUCCESS), while the parent
  waits forever on recv() without draining the pipe.
- Stderr output without a trailing newline fuses with the _OUTPUT_DIVIDER
  sentinel into a single readline() result that never matches the exact
  equality check, so trace_output() loops on readline() forever.

Both tests use @pytest.mark.timeout — a hang here means a regression, and
pytest fails the test cleanly via SIGALRM rather than wedging CI.
"""

from __future__ import annotations

import sys

import pytest

import daft


@pytest.mark.timeout(30)
def test_large_stderr_does_not_deadlock_on_pipe_buffer():
    STDERR_BYTES_PER_ROW = 100_000  # well over any OS pipe buffer (~64 KiB)

    @daft.func(use_process=True)
    def noisy(x: int) -> int:
        sys.stderr.buffer.write(b"x" * STDERR_BYTES_PER_ROW + b"\n")
        sys.stderr.flush()
        return x + 1

    df = daft.from_pydict({"x": list(range(4))})
    actual = df.with_column("y", noisy(df["x"])).to_pydict()
    assert actual == {"x": [0, 1, 2, 3], "y": [1, 2, 3, 4]}


@pytest.mark.timeout(30)
def test_stderr_without_trailing_newline_does_not_deadlock_on_divider_merge():
    @daft.func(use_process=True)
    def no_newline(x: int) -> int:
        sys.stderr.buffer.write(b"hello-no-newline")
        sys.stderr.flush()
        return x + 1

    df = daft.from_pydict({"x": list(range(4))})
    actual = df.with_column("y", no_newline(df["x"])).to_pydict()
    assert actual == {"x": [0, 1, 2, 3], "y": [1, 2, 3, 4]}


@pytest.mark.timeout(30)
def test_large_stderr_without_trailing_newline_does_not_deadlock():
    r"""Combines both deadlock pathologies: pipe-buffer fill AND no trailing newline.

    The earlier two tests cover each pathology in isolation:
    - Large stderr WITH newline (covered by `mp.connection.wait()`-based drain).
    - Small stderr WITHOUT newline (covered by the prepended-`\\n` divider fix).

    Per a review concern: even with the drain in place, if the child writes
    >64 KiB without ever emitting a newline, the parent's `readline()`-based
    scan in `trace_output()` could still block waiting for a delimiter while
    the child writes are stalled against a full OS pipe buffer.
    """
    STDERR_BYTES_PER_ROW = 100_000  # well over any OS pipe buffer (~64 KiB)

    @daft.func(use_process=True)
    def noisy_no_newline(x: int) -> int:
        sys.stderr.buffer.write(b"x" * STDERR_BYTES_PER_ROW)
        sys.stderr.flush()
        return x + 1

    df = daft.from_pydict({"x": list(range(4))})
    actual = df.with_column("y", noisy_no_newline(df["x"])).to_pydict()
    assert actual == {"x": [0, 1, 2, 3], "y": [1, 2, 3, 4]}


@pytest.mark.timeout(30)
def test_silent_udf_produces_no_spurious_stdout_lines(monkeypatch):
    r"""trace_output() must not surface the divider-padding newline as a line.

    The divider-merge fix prepends `\n` to `_OUTPUT_DIVIDER` so the divider
    always lands on its own line. Without filtering that padding in
    trace_output(), every batch of every use_process=True UDF returns an
    extra empty line, which the Rust caller then prints as `[`udf` Worker #N]`
    noise to the terminal.

    Intercept trace_output() to capture what it returns and assert the
    silent-UDF case yields an empty list.
    """
    from daft.execution import udf as udf_mod

    captured: list[list[str]] = []
    original = udf_mod.UdfHandle.trace_output

    def spy(self: object) -> list[str]:
        result = original(self)  # type: ignore[arg-type]
        captured.append(result)
        return result

    monkeypatch.setattr(udf_mod.UdfHandle, "trace_output", spy)

    @daft.func(use_process=True)
    def silent_udf(x: int) -> int:
        return x + 1

    df = daft.from_pydict({"x": list(range(4))})
    actual = df.with_column("y", silent_udf(df["x"])).to_pydict()
    assert actual == {"x": [0, 1, 2, 3], "y": [1, 2, 3, 4]}

    # Every trace_output() call for a silent UDF should return exactly [].
    non_empty_results = [r for r in captured if r]
    assert non_empty_results == [], f"Silent UDF produced spurious stdout lines: {non_empty_results!r}"
