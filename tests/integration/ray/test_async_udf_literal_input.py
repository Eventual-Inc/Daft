"""Ray integration coverage for issue #6805.

The actor UDF path (`src/daft-distributed/src/pipeline_node/actor_udf.rs`)
calls `remap_used_cols` and feeds the result downstream to a Ray actor.
This test exercises the same literal-only-input scenario as the unit
tests but on the Ray runner, with multi-partition input to confirm the
panic-free + per-row-invocation behavior holds across actors.
"""

from __future__ import annotations

import pytest

import daft
from daft import DataType, Series, col, lit
from tests.conftest import get_tests_daft_runner_name

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        get_tests_daft_runner_name() != "ray",
        reason="Actor UDF path is only exercised on the Ray runner",
    ),
]


def test_async_batch_udf_with_literal_input_on_ray_multi_partition():
    """Async batch UDF with a literal-only input must run correctly on Ray.

    Must not panic, and must process all N rows when split across multiple
    partitions.
    """

    @daft.cls(on_error="raise", max_retries=0, max_concurrency=1)
    class LengthEcho:
        @daft.method.batch(return_dtype=DataType.int64())
        async def run(self, inputs: Series) -> Series:
            # Encode the per-call input length into each output row.
            # If the UDF is invoked once with a length-1 literal and the
            # result is broadcast to N, every output is 1; the assert
            # below catches that.
            return Series.from_pylist([len(inputs)] * len(inputs))

    udf = LengthEcho()
    n = 8
    df = (
        daft.from_pydict({"id": list(range(n))})
        .into_partitions(2)
        .with_column("msg", lit("x"))
        .with_column("result", udf.run(col("msg")))
    )
    result = df.select("result").to_pydict()

    assert len(result["result"]) == n
    assert max(result["result"]) > 1, (
        f"UDF appears to have been invoked once on a length-1 literal and "
        f"broadcast across all rows (every output is 1). result={result!r}"
    )
