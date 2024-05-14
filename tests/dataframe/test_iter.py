from __future__ import annotations

import os
import time

import pytest

import daft


class MockException(Exception):
    pass


@pytest.mark.parametrize("materialized", [False, True])
def test_iter_rows(make_df, materialized):
    # Test that df.__iter__ produces the correct rows in the correct order.
    # It should work regardless of whether the dataframe has already been materialized or not.

    df = make_df({"a": list(range(10))}).into_partitions(5).with_column("b", daft.col("a") + 100)
    if materialized:
        df = df.collect()

    rows = list(iter(df))
    assert rows == [{"a": x, "b": x + 100} for x in range(10)]


@pytest.mark.parametrize("materialized", [False, True])
def test_iter_partitions(make_df, materialized):
    # Test that df.iter_partitions() produces partitions in the correct order.
    # It should work regardless of whether the dataframe has already been materialized or not.

    df = make_df({"a": list(range(10))}).into_partitions(5).with_column("b", daft.col("a") + 100)

    if materialized:
        df = df.collect()

    parts = list(df.iter_partitions())
    if daft.context.get_context().runner_config.name == "ray":
        import ray

        parts = ray.get(parts)
    parts = [_.to_pydict() for _ in parts]

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

    df = make_df({"a": list(range(200))}).into_partitions(100).with_column("b", echo_or_trigger(daft.col("a")))

    it = iter(df)
    assert next(it) == {"a": 0, "b": 0}

    # Ensure the exception does trigger if execution continues.
    with pytest.raises(MockException):
        list(it)


def test_iter_partitions_exception(make_df):
    # Test that df.iter_partitions actually returns results before completing execution.
    # We test this by raising an exception in a UDF if too many partitions are executed.

    @daft.udf(return_dtype=daft.DataType.int64())
    def echo_or_trigger(s):
        trigger = max(s.to_pylist())
        if trigger >= 199:
            raise MockException(trigger)
        else:
            return s

    df = make_df({"a": list(range(200))}).into_partitions(100).with_column("b", echo_or_trigger(daft.col("a")))

    it = df.iter_partitions()
    part = next(it)
    if daft.context.get_context().runner_config.name == "ray":
        import ray

        part = ray.get(part)
    part = part.to_pydict()

    assert part == {"a": [0, 1], "b": [0, 1]}

    # Ensure the exception does trigger if execution continues.
    with pytest.raises(MockException):
        res = list(it)
        if daft.context.get_context().runner_config.name == "ray":
            ray.get(res)


def test_iter_partitions_with_buffer_limit_1(tmp_path, make_df):
    files_location = tmp_path / "files"
    files_location.mkdir(exist_ok=True)

    @daft.udf(return_dtype=daft.DataType.int64())
    def write_file(s):
        num = max(s.to_pylist())
        (files_location / f"{num}.txt").touch()
        return s

    df = make_df({"a": list(range(8))}).into_partitions(4).with_column("b", write_file(daft.col("a")))
    it = df.iter_partitions(results_buffer_size=1)

    data = []

    assert os.listdir(files_location) == [], "Directory should have no files initially"

    data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 2
    ), "First iteration should trigger 2 calls of write_file, one for the iterator result and one for the buffer"

    data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 3
    ), "Subsequent iteration should trigger 1 call of write_file to fill the buffer"

    data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 4
    ), "Subsequent iteration should trigger 1 call of write_file to fill the buffer"

    data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 4
    ), "Last iteration should not trigger anymore writes, and just consume the last item"

    with pytest.raises(StopIteration):
        next(it)


def test_iter_partitions_with_buffer_limit_3(tmp_path, make_df):
    files_location = tmp_path / "files"
    files_location.mkdir(exist_ok=True)

    @daft.udf(return_dtype=daft.DataType.int64())
    def write_file(s):
        num = max(s.to_pylist())
        (files_location / f"{num}.txt").touch()
        return s

    df = make_df({"a": list(range(8))}).into_partitions(7).with_column("b", write_file(daft.col("a")))
    it = df.iter_partitions(results_buffer_size=3)

    data = []

    assert os.listdir(files_location) == [], "Directory should have no files initially"

    data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 4
    ), "First iteration should trigger 4 calls of write_file: 1 for the iterator result, 3 buffered"

    data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 5
    ), "Subsequent single iteration should trigger 1 call of write_file to refill the buffer"

    for _ in range(2):
        data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 7
    ), "Subsequent rapid iteration twice in a row should trigger 2 calls to refill buffer"

    for _ in range(3):
        data.append(next(it))
    time.sleep(0.5)
    assert (
        len(os.listdir(files_location)) == 7
    ), "Last 3 iterations to empty everything should not trigger any more writes"

    with pytest.raises(StopIteration):
        next(it)
