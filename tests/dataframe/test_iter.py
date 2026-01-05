from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft.errors import UDFException
from tests.conftest import get_tests_daft_runner_name
import time


class MockException(Exception):
    pass


@pytest.mark.parametrize("materialized", [False, True])
@pytest.mark.parametrize("dynamic_batching", [True, False])
def test_iter_rows(make_df, materialized, dynamic_batching):
    # Test that df.__iter__ produces the correct rows in the correct order.
    # It should work regardless of whether the dataframe has already been materialized or not.
    with daft.execution_config_ctx(enable_dynamic_batching=dynamic_batching):
        df = make_df({"a": list(range(10))}).into_partitions(5).with_column("b", daft.col("a") + 100)
        if materialized:
            df = df.collect()

        rows = list(iter(df))
        assert rows == [{"a": x, "b": x + 100} for x in range(10)]


@pytest.mark.parametrize(
    "format, data, expected",
    [
        ### Ints
        pytest.param("python", [1, 2, 3], [1, 2, 3], id="python_ints"),
        pytest.param(
            "arrow",
            [1, 2, 3],
            [pa.scalar(1), pa.scalar(2), pa.scalar(3)],
            id="arrow_ints",
        ),
        ### Strings
        pytest.param("python", ["a", "b", "c"], ["a", "b", "c"], id="python_strs"),
        pytest.param(
            "arrow",
            ["a", "b", "c"],
            [
                pa.scalar("a", pa.large_string()),
                pa.scalar("b", pa.large_string()),
                pa.scalar("c", pa.large_string()),
            ],
            id="arrow_strs",
        ),
        ### Lists
        pytest.param("python", [[1, 2], [3, 4]], [[1, 2], [3, 4]], id="python_lists"),
        pytest.param(
            "arrow",
            [[1, 2], [3, 4]],
            [
                pa.scalar([1, 2], pa.large_list(pa.int64())),
                pa.scalar([3, 4], pa.large_list(pa.int64())),
            ],
            id="arrow_lists",
        ),
        ### Structs
        pytest.param(
            "python",
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            id="python_structs",
        ),
        pytest.param(
            "arrow",
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            [
                pa.scalar(
                    {"a": 1, "b": 2},
                    pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.int64())]),
                ),
                pa.scalar(
                    {"a": 3, "b": 4},
                    pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.int64())]),
                ),
            ],
            id="arrow_structs",
        ),
    ],
)
@pytest.mark.parametrize("dynamic_batching", [True, False])
def test_iter_rows_column_formats(make_df, format, data, expected, dynamic_batching):
    with daft.execution_config_ctx(enable_dynamic_batching=dynamic_batching):
        df = make_df({"a": data})
        rows = list(df.iter_rows(column_format=format))

        # Compare each row
        assert len(rows) == len(expected)
        for actual_row, expected_row in zip(rows, [{"a": e} for e in expected]):
            assert actual_row == expected_row


@pytest.mark.parametrize(
    "data, expected",
    [
        pytest.param(
            [[1, 2], [3, 4]],
            [
                np.array([1, 2], dtype=np.int64),
                np.array([3, 4], dtype=np.int64),
            ],
            id="list_of_ints",
        ),
        pytest.param(
            [[1.0, 2.0], [3.0, 4.0]],
            [
                np.array([1.0, 2.0], dtype=np.float64),
                np.array([3.0, 4.0], dtype=np.float64),
            ],
            id="list_of_floats",
        ),
    ],
)
@pytest.mark.parametrize("dynamic_batching", [True, False])
def test_iter_rows_lists_to_numpy(make_df, data, expected, dynamic_batching):
    with daft.execution_config_ctx(enable_dynamic_batching=dynamic_batching):
        df = make_df({"a": data})

        rows = list(df.iter_rows(column_format="arrow"))

        # Compare each row
        assert len(rows) == len(expected)
        for actual_row, expected_row in zip(rows, [{"a": e} for e in expected]):
            np_data = actual_row["a"].values.to_numpy()
            np.testing.assert_array_equal(np_data, expected_row["a"])


@pytest.mark.parametrize("materialized", [False, True])
@pytest.mark.parametrize("dynamic_batching", [True, False])
def test_iter_partitions(make_df, materialized, dynamic_batching):
    # Test that df.iter_partitions() produces partitions in the correct order.
    # It should work regardless of whether the dataframe has already been materialized or not.

    with daft.execution_config_ctx(default_morsel_size=2, enable_dynamic_batching=dynamic_batching):
        df = make_df({"a": list(range(10))}).into_partitions(5).with_column("b", daft.col("a") + 100)

        if materialized:
            df = df.collect()

        parts = list(df.iter_partitions())
        if get_tests_daft_runner_name() == "ray":
            import ray

            parts = ray.get(parts)
        parts = [_.to_pydict() for _ in parts]

        # Sort partitions by first value of 'a' to handle non-deterministic ordering
        parts = sorted(parts, key=lambda p: p["a"][0] if p["a"] else float("inf"))

        assert parts == [
            {"a": [0, 1], "b": [100, 101]},
            {"a": [2, 3], "b": [102, 103]},
            {"a": [4, 5], "b": [104, 105]},
            {"a": [6, 7], "b": [106, 107]},
            {"a": [8, 9], "b": [108, 109]},
        ]


def test_iter_exception(make_df):
    # Test that df.__iter__ actually returns results before completing execution.
    # We test this by raising an exception in a UDF if too many partitions are executed.

    @daft.udf(return_dtype=daft.DataType.int64())
    def echo_or_trigger(s):
        trigger = max(s.to_pylist())
        if trigger >= 199:
            raise MockException(trigger)
        else:
            return s

    with daft.execution_config_ctx(default_morsel_size=2):
        df = make_df({"a": list(range(200))}).into_partitions(100).with_column("b", echo_or_trigger(daft.col("a")))

        it = iter(df)
        assert next(it) == {"a": 0, "b": 0}

        # Ensure the exception does trigger if execution continues.
        with pytest.raises(UDFException) as exc_info:
            list(it)

        # Ray's wrapping of the exception loses information about the `.cause`, but preserves it in the string error message
        if get_tests_daft_runner_name() == "ray":
            assert "MockException" in str(exc_info.value)
        else:
            assert isinstance(exc_info.value.__cause__, MockException)

        assert str(exc_info.value).endswith("failed when executing on inputs:\n  - a (Int64, length=2)")


@pytest.mark.parametrize("dynamic_batching", [True, False])
def test_iter_partitions_exception(make_df, dynamic_batching):
    # Test that df.iter_partitions actually returns results before completing execution.
    # We test this by raising an exception in a UDF if too many partitions are executed.

    @daft.udf(return_dtype=daft.DataType.int64())
    def echo_or_trigger(s):
        trigger = max(s.to_pylist())
        if trigger >= 199:
            raise MockException(trigger)
        else:
            return s

    with daft.execution_config_ctx(default_morsel_size=2, enable_dynamic_batching=dynamic_batching):
        df = daft.range(200, partitions=100).with_column("b", echo_or_trigger(daft.col("id")))

        it = df.iter_partitions()
        part = next(it)
        if get_tests_daft_runner_name() == "ray":
            import ray

            part = ray.get(part)
        part = part.to_pydict()

        # Check that we get a valid partition (order may be non-deterministic)
        assert len(part["id"]) == 2 and len(part["b"]) == 2
        assert part["id"] == part["b"]  # b should equal a since echo_or_trigger returns the input
        assert all(0 <= x < 200 for x in part["id"])

        # Ensure the exception does trigger if execution continues.
        with pytest.raises(UDFException) as exc_info:
            res = list(it)
            if get_tests_daft_runner_name() == "ray":
                ray.get(res)

        # Ray's wrapping of the exception loses information about the `.cause`, but preserves it in the string error message
        if get_tests_daft_runner_name() == "ray":
            assert "MockException" in str(exc_info.value)
        else:
            assert isinstance(exc_info.value.__cause__, MockException)
        assert str(exc_info.value).endswith("failed when executing on inputs:\n  - id (Int64, length=2)")
