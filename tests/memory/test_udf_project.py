import uuid

import pyarrow as pa
import pytest
from memray._memray import compute_statistics

import daft
from daft.execution.execution_step import ExpressionsProjection, Project
from tests.memory.utils import run_wrapper_build_partitions


def format_bytes(bytes_value):
    """Format bytes into human readable string with appropriate unit."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024
    return f"{bytes_value:.2f} GB"


@daft.udf(return_dtype=str)
def to_arrow_identity(s):
    data = s.to_arrow()
    return data


@daft.udf(return_dtype=str)
def to_pylist_identity(s):
    data = s.to_pylist()
    return data


@daft.udf(return_dtype=str, batch_size=128)
def to_arrow_identity_batched(s):
    data = s.to_arrow()
    return data


@daft.udf(return_dtype=str, batch_size=128)
def to_pylist_identity_batched(s):
    data = s.to_pylist()
    return data


@daft.udf(return_dtype=str, batch_size=128)
def to_pylist_identity_batched_arrow_return(s):
    data = s.to_pylist()
    return pa.array(data)


@pytest.mark.parametrize(
    "udf",
    [
        to_arrow_identity,
        to_pylist_identity,
        to_arrow_identity_batched,
        to_pylist_identity_batched,
        to_pylist_identity_batched_arrow_return,
    ],
)
def test_short_string_identity_projection(udf):
    instructions = [Project(ExpressionsProjection([udf(daft.col("a"))]))]
    inputs = [{"a": [str(uuid.uuid4()) for _ in range(62500)]}]
    _, memray_file = run_wrapper_build_partitions(inputs, instructions)
    stats = compute_statistics(memray_file)

    expected_peak_bytes = 100
    assert stats.peak_memory_allocated < expected_peak_bytes, (
        f"Peak memory ({format_bytes(stats.peak_memory_allocated)}) "
        f"exceeded threshold ({format_bytes(expected_peak_bytes)})"
    )


@pytest.mark.parametrize(
    "udf",
    [
        to_arrow_identity,
        to_pylist_identity,
        to_arrow_identity_batched,
        to_pylist_identity_batched,
        to_pylist_identity_batched_arrow_return,
    ],
)
def test_long_string_identity_projection(udf):
    instructions = [Project(ExpressionsProjection([udf(daft.col("a"))]))]
    inputs = [{"a": [str(uuid.uuid4()) for _ in range(625000)]}]
    _, memray_file = run_wrapper_build_partitions(inputs, instructions)
    stats = compute_statistics(memray_file)

    expected_peak_bytes = 100
    assert stats.peak_memory_allocated < expected_peak_bytes, (
        f"Peak memory ({format_bytes(stats.peak_memory_allocated)}) "
        f"exceeded threshold ({format_bytes(expected_peak_bytes)})"
    )
