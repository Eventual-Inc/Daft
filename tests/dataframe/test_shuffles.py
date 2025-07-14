from __future__ import annotations

import random
import tempfile
from contextlib import contextmanager
from functools import partial
from typing import Callable

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft.context import get_context
from daft.io._generator import read_generator
from daft.recordbatch.recordbatch import RecordBatch
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray"
    or get_context().daft_execution_config.use_experimental_distributed_engine is True,
    reason="shuffle tests are not yet supported for flotilla",
)


def generate(num_rows: int, bytes_per_row: int):
    data = {
        "ints": np.random.randint(0, num_rows, num_rows, dtype=np.uint64),
        "bytes": pa.array(
            [np.random.bytes(bytes_per_row) for _ in range(num_rows)],
            type=pa.binary(bytes_per_row),
        ),
    }
    yield RecordBatch.from_pydict(data)


def generator(
    num_partitions: int,
    num_rows_fn: Callable[[], int],
    bytes_per_row_fn: Callable[[], int],
):
    for _ in range(num_partitions):
        num_rows = num_rows_fn()
        bytes_per_row = bytes_per_row_fn()
        yield partial(generate, num_rows, bytes_per_row)


@pytest.fixture(scope="function")
def pre_shuffle_merge_ctx():
    """Fixture that provides a context manager for pre-shuffle merge testing."""

    def _ctx(threshold: int | None = None):
        return daft.execution_config_ctx(shuffle_algorithm="pre_shuffle_merge", pre_shuffle_merge_threshold=threshold)

    return _ctx


@pytest.fixture(scope="function")
def flight_shuffle_ctx():
    """Fixture that provides a context manager for flight shuffle testing with a temporary directory."""

    @contextmanager
    def _ctx():
        # Create a temporary directory that automatically cleans up
        with tempfile.TemporaryDirectory() as temp_dir:
            # Use the temporary directory for flight shuffle
            with daft.execution_config_ctx(shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[temp_dir]) as ctx:
                yield ctx

    return _ctx


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_pre_shuffle_merge_small_partitions(pre_shuffle_merge_ctx, input_partitions, output_partitions):
    """Test that pre-shuffle merge is working for small partitions less than the memory threshold."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return 1

    threshold = None

    with pre_shuffle_merge_ctx(threshold):
        df = (
            read_generator(
                generator(input_partitions, num_rows_fn, bytes_per_row_fn),
                schema=daft.Schema._from_field_name_and_types(
                    [
                        ("ints", daft.DataType.uint64()),
                        ("bytes", daft.DataType.binary()),
                    ]
                ),
            )
            .repartition(output_partitions, "ints")
            .collect()
        )
        assert len(df) == input_partitions * output_partitions


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_pre_shuffle_merge_big_partitions(pre_shuffle_merge_ctx, input_partitions, output_partitions):
    """Test that pre-shuffle merge is working for big partitions greater than the threshold."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return 200

    threshold = 1

    with pre_shuffle_merge_ctx(threshold):
        df = (
            read_generator(
                generator(input_partitions, num_rows_fn, bytes_per_row_fn),
                schema=daft.Schema._from_field_name_and_types(
                    [
                        ("ints", daft.DataType.uint64()),
                        ("bytes", daft.DataType.binary()),
                    ]
                ),
            )
            .repartition(output_partitions, "ints")
            .collect()
        )
        assert len(df) == input_partitions * output_partitions


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_pre_shuffle_merge_randomly_sized_partitions(pre_shuffle_merge_ctx, input_partitions, output_partitions):
    """Test that pre-shuffle merge is working for randomly sized partitions."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return random.randint(1, output_partitions // 2 + 1)

    # We want some partitions that are small, and some that are big. We want to cap the big ones to be around half of the threshold.
    threshold = output_partitions * (8 + output_partitions)

    with pre_shuffle_merge_ctx(threshold):
        df = (
            read_generator(
                generator(input_partitions, num_rows_fn, bytes_per_row_fn),
                schema=daft.Schema._from_field_name_and_types(
                    [
                        ("ints", daft.DataType.uint64()),
                        ("bytes", daft.DataType.binary()),
                    ]
                ),
            )
            .repartition(output_partitions, "ints")
            .collect()
        )
        assert len(df) == input_partitions * output_partitions


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_flight_shuffle(flight_shuffle_ctx, input_partitions, output_partitions):
    """Test that flight shuffle is working."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return 200

    with flight_shuffle_ctx():
        df = (
            read_generator(
                generator(input_partitions, num_rows_fn, bytes_per_row_fn),
                schema=daft.Schema._from_field_name_and_types(
                    [
                        ("ints", daft.DataType.uint64()),
                        ("bytes", daft.DataType.fixed_size_binary(200)),
                    ]
                ),
            )
            .repartition(output_partitions, "ints")
            .collect()
        )
        assert len(df) == input_partitions * output_partitions
