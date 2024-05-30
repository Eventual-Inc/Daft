from __future__ import annotations

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
    with pytest.raises(RuntimeError) as exc_info:
        list(it)

    # Ray's wrapping of the exception loses information about the `.cause`, but preserves it in the string error message
    if daft.context.get_context().runner_config.name == "ray":
        assert "MockException" in str(exc_info.value)
    else:
        assert isinstance(exc_info.value.__cause__, MockException)


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
    with pytest.raises(RuntimeError) as exc_info:
        res = list(it)
        if daft.context.get_context().runner_config.name == "ray":
            ray.get(res)

    # Ray's wrapping of the exception loses information about the `.cause`, but preserves it in the string error message
    if daft.context.get_context().runner_config.name == "ray":
        assert "MockException" in str(exc_info.value)
    else:
        assert isinstance(exc_info.value.__cause__, MockException)
