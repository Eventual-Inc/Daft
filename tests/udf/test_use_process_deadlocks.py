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
