"""Regression tests for issue #6805.

When a UDF expression has no column references (e.g. its only input is a
literal column that gets folded away by the optimizer), two related bugs
appear in the AsyncUdfSink path:

1. The sink panics with `index out of bounds: the len is 0 but the index
   is 0` when projection pushdown also narrows the upstream batch to
   zero columns.
2. Even when no panic fires, the UDF is invoked once on a length-1
   literal input and the result is broadcast to N rows. This is wrong
   for non-pure UDFs (random sampling, external API calls, anything
   stateful) — the UDF must be invoked across all N upstream rows.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel, Field

import daft
from daft import DataType, Series, col, lit
from tests.conftest import get_tests_daft_runner_name

# These property tests rely on closure-captured Python state mutated inside
# the UDF body. That works on the native runner (UDF runs in-process) but not
# on Ray (UDF runs in an actor process). The Ray actor UDF path is exercised
# by tests/integration/ray/test_async_udf_literal_input.py instead.
_skip_on_ray_closure_state = pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Closure-captured state is not observable across Ray actor processes",
)


def test_issue_6805_verbatim_repro():
    """Verbatim repro from issue #6805.

    Originally panicked with `index out of bounds: the len is 0 but the
    index is 0`.
    """

    class MyOutput(BaseModel):
        text: str
        tags: list[str] = Field(default_factory=list)

    @daft.cls(on_error="raise", max_retries=0, max_concurrency=1)
    class MyAsyncUDF:
        def __init__(self):
            pass

        @daft.method.batch(return_dtype=DataType.infer_from_type(MyOutput))
        async def run(self, inputs: daft.Series) -> daft.Series:
            results = []
            for val in inputs.to_pylist():
                await asyncio.sleep(0.01)
                results.append({"text": f"processed {val}", "tags": ["a", "b"]})
            return daft.Series.from_pylist(results, dtype=DataType.infer_from_type(MyOutput))

    df = daft.from_pydict({"id": [1, 2, 3, 4]}).with_column("msg", lit("hello"))
    udf = MyAsyncUDF()
    df = df.with_column("result", udf.run(df["msg"]))

    actual = df.select("result").to_pydict()
    assert actual == {"result": [{"text": "processed hello", "tags": ["a", "b"]}] * 4}


@_skip_on_ray_closure_state
def test_async_batch_udf_with_literal_input_receives_n_row_series():
    """A non-pure UDF must be invoked with the upstream row count.

    Even when its input expression has no column references, the UDF must
    see a Series of length N — not a length-1 literal that gets broadcast.
    """
    seen_lengths: list[int] = []

    @daft.cls(max_concurrency=1)
    class LengthRecorder:
        @daft.method.batch(return_dtype=DataType.int64())
        async def run(self, inputs: Series) -> Series:
            seen_lengths.append(len(inputs))
            return Series.from_pylist([len(inputs)] * len(inputs))

    udf = LengthRecorder()
    n = 8
    df = (
        daft.from_pydict({"id": list(range(n))}).with_column("msg", lit("x")).with_column("result", udf.run(col("msg")))
    )
    df.select("result").to_pydict()

    assert sum(seen_lengths) == n, (
        f"UDF received total {sum(seen_lengths)} rows across {len(seen_lengths)} "
        f"invocation(s); expected {n}. seen_lengths={seen_lengths!r}"
    )


@_skip_on_ray_closure_state
def test_sync_batch_udf_with_literal_input_receives_n_row_series():
    """Same property as the async case, on the sync UDF eval path.

    Sync and async Python UDFs are dispatched through different branches
    of the RecordBatch eval pipeline; the broadcast fix must apply to both.
    """
    seen_lengths: list[int] = []

    @daft.func.batch(return_dtype=DataType.int64())
    def length_recorder(inputs: Series) -> Series:
        seen_lengths.append(len(inputs))
        return Series.from_pylist([len(inputs)] * len(inputs))

    n = 8
    df = (
        daft.from_pydict({"id": list(range(n))})
        .with_column("msg", lit("x"))
        .with_column("result", length_recorder(col("msg")))
    )
    df.select("result").to_pydict()

    assert sum(seen_lengths) == n, (
        f"UDF received total {sum(seen_lengths)} rows across {len(seen_lengths)} "
        f"invocation(s); expected {n}. seen_lengths={seen_lengths!r}"
    )


@_skip_on_ray_closure_state
def test_row_wise_udf_with_literal_input_runs_per_row():
    """Row-wise (`@daft.func`) UDFs land on PyScalarFn::RowWise.

    The same `Expr::ScalarFn(ScalarFn::Python)` branch handles both batch
    and row-wise variants, but row-wise has its own dispatch in the UDF
    impl. Verify the broadcast fix applies here too: the UDF must be
    invoked once per row, not once-and-broadcast.
    """
    call_count = [0]

    @daft.func(return_dtype=DataType.int64())
    def row_wise_counter(_x: str) -> int:
        call_count[0] += 1
        return call_count[0]

    n = 6
    df = (
        daft.from_pydict({"id": list(range(n))})
        .with_column("msg", lit("x"))
        .with_column("result", row_wise_counter(col("msg")))
    )
    df.select("result").to_pydict()

    assert call_count[0] == n, f"Row-wise UDF was invoked {call_count[0]} time(s); expected {n}."


def test_async_batch_udf_with_literal_input_on_empty_dataframe():
    """Empty input batch with literal-only UDF input must not panic.

    Guards the broadcast guard: when row_count is 0, the broadcast path
    should be a no-op, not crash on `Series::broadcast(0)`.
    """

    @daft.cls(max_concurrency=1)
    class EchoUDF:
        @daft.method.batch(return_dtype=DataType.string())
        async def run(self, inputs: Series) -> Series:
            return Series.from_pylist([f"got: {v}" for v in inputs.to_pylist()])

    udf = EchoUDF()
    df = daft.from_pydict({"id": []}).with_column("msg", lit("x")).with_column("result", udf.run(col("msg")))
    actual = df.select("result").to_pydict()
    assert actual == {"result": []}
