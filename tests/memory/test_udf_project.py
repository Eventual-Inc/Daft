import uuid

import pytest
from memray._memray import compute_statistics

import daft
from daft.execution.execution_step import ExpressionsProjection, Project
from tests.memory.utils import run_wrapper_build_partitions


@daft.udf(return_dtype=str)
def to_arrow_identity(s):
    data = s.to_arrow()
    return data


@daft.udf(return_dtype=str)
def to_pylist_identity(s):
    data = s.to_pylist()
    return data


@pytest.mark.parametrize(
    "udf",
    [
        to_arrow_identity,
        to_pylist_identity,
    ],
)
def test_string_identity_projection(udf):
    instructions = [Project(ExpressionsProjection([udf(daft.col("a"))]))]
    inputs = [{"a": [str(uuid.uuid4()) for _ in range(62500)]}]
    _, memray_file = run_wrapper_build_partitions(inputs, instructions)
    stats = compute_statistics(memray_file)

    assert stats.peak_memory_allocated < 100
