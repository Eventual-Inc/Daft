from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from typing import Any

import pytest

import daft
from daft import DataType, Series
from daft.recordbatch import RecordBatch
from daft.udf import metrics
from tests.conftest import get_tests_daft_runner_name

VALUES = list(range(1, 11))
VALUES_SUM = sum(VALUES)


def _find_udf_stats(metrics: RecordBatch | None, metric_name: str) -> float | None:
    assert metrics is not None
    metrics_list = metrics.to_pylist()
    return next((metric[1]["value"] for op in metrics_list for metric in op["stats"] if metric[0] == metric_name), None)


def _wrap_async(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    async def async_fn(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(0)
        return fn(*args, **kwargs)

    return async_fn


def _make_scalar_udf(
    *,
    counter_name: str,
    factor: int,
    use_process: bool,
    is_async: bool,
) -> Callable[[Any], Any]:
    def body(
        value: int,
        *,
        _factor=factor,
    ) -> int:
        metrics.increment_counter(counter_name, amount=value * _factor)
        return value + _factor

    impl = _wrap_async(body) if is_async else body
    return daft.func(use_process=use_process)(impl)


def _make_batch_udf(
    *,
    counter_name: str,
    factor: int,
    use_process: bool,
    is_async: bool,
) -> Callable[[Any], Any]:
    def body(
        values: Series,
    ) -> Series:
        py_values = values.to_pylist()
        for v in py_values:
            metrics.increment_counter(counter_name, amount=v * factor)
        return Series.from_pylist([v + factor for v in py_values])

    impl = _wrap_async(body) if is_async else body
    return daft.func.batch(return_dtype=DataType.int64(), use_process=use_process)(impl)


def _make_cls_udf(
    *,
    factor: int,
    counter_name: str,
    concurrency: int | None,
    is_async: bool,
) -> Callable[[Any], Any]:
    if is_async:

        async def call_impl(self: Any, value: int) -> int:
            await asyncio.sleep(0)  # Make it async but don't slow down tests
            metrics.increment_counter(self.counter_name, amount=value * factor)
            return value + self.addend

    else:

        def call_impl(self: Any, value: int) -> int:
            metrics.increment_counter(self.counter_name, amount=value * factor)
            return value + self.addend

    @daft.cls(max_concurrency=concurrency)
    class MetricUdf:
        def __init__(
            self,
            addend: int,
            counter_name: str,
        ) -> None:
            self.addend = addend
            self.counter_name = counter_name

        __call__ = call_impl  # type: ignore[assignment]

    return MetricUdf(factor, counter_name)


def test_udf_metrics_increment_outside_context() -> None:
    with pytest.warns(
        RuntimeWarning,
        match="Custom UDF metrics will only be recorded during execution within a UDF function or class method.",
    ):
        metrics.increment_counter("outside counter")


def test_udf_metrics_direct_udf_call_warns() -> None:
    @daft.func
    def direct_udf(value: int, *, _counter_name: str = "direct counter") -> int:
        metrics.increment_counter(_counter_name)
        return value + 1

    with pytest.warns(
        RuntimeWarning,
        match="Custom UDF metrics will only be recorded during execution within a UDF function or class method.",
    ):
        result = direct_udf(1)

    assert result == 2


def test_increment_counter_metadata_propagation() -> None:
    with metrics._metrics_context() as operator_metrics:
        metrics.increment_counter(
            "custom counter",
            amount=5,
            description="Counts custom things",
            attributes={"kind": "test"},
        )

    counters = operator_metrics.snapshot()
    assert counters["custom counter"] == [
        {
            "value": 5,
            "description": "Counts custom things",
            "attributes": {"kind": "test"},
        }
    ]


def test_increment_counter_same_name_different_attributes() -> None:
    with metrics._metrics_context() as operator_metrics:
        metrics.increment_counter("custom counter", amount=1, attributes={"kind": "test-a"})
        metrics.increment_counter("custom counter", amount=2, attributes={"kind": "test-b"})

    counters = operator_metrics.snapshot()
    assert counters["custom counter"] == [
        {"value": 1, "description": None, "attributes": {"kind": "test-a"}},
        {"value": 2, "description": None, "attributes": {"kind": "test-b"}},
    ]


def test_increment_counter_rejects_non_string_description() -> None:
    with pytest.raises(ValueError, match="Metric description must be a string"):
        metrics.increment_counter("bad description", description=123)  # type: ignore[arg-type]


def test_increment_counter_rejects_non_mapping_attributes() -> None:
    with pytest.raises(ValueError, match="Metric attributes must be a mapping"):
        metrics.increment_counter("bad attrs", attributes=[("k", "v")])  # type: ignore[arg-type]


def test_increment_counter_rejects_invalid_attribute_key() -> None:
    with pytest.raises(ValueError, match="attribute keys"):
        metrics.increment_counter("bad key", attributes={"": "value"})


def test_increment_counter_rejects_invalid_attribute_value() -> None:
    with pytest.raises(ValueError, match="attribute values"):
        metrics.increment_counter("bad value", attributes={"key": 1})  # type: ignore[arg-type]


@pytest.mark.parametrize("num_udfs", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
@pytest.mark.parametrize("use_process", [False, True])
@pytest.mark.parametrize("is_async", [False, True])
def test_udf_custom_metrics_func(num_udfs: int, batch_size: int | None, use_process: bool, is_async: bool) -> None:
    df = daft.from_pydict({"value": VALUES})
    if batch_size is not None:
        df = df.into_batches(batch_size)

    cases = []
    for i in range(num_udfs):
        factor = i + 1
        counter_name = f"udf{i} counter"

        udf = _make_scalar_udf(
            counter_name=counter_name,
            factor=factor,
            use_process=use_process,
            is_async=is_async,
        )
        df = df.with_column(f"out_{i}", udf(daft.col("value")))

        cases.append((counter_name, factor * VALUES_SUM))

    df.collect()

    for counter_name, expected_counter in cases:
        assert _find_udf_stats(df.metrics, counter_name) == expected_counter


@pytest.mark.parametrize("num_udfs", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
@pytest.mark.parametrize("use_process", [False, True])
@pytest.mark.parametrize("is_async", [False, True])
def test_udf_custom_metrics_batch(num_udfs: int, batch_size: int | None, use_process: bool, is_async: bool) -> None:
    df = daft.from_pydict({"value": VALUES})
    if batch_size is not None:
        df = df.into_batches(batch_size)

    cases = []
    for i in range(num_udfs):
        factor = i + 1
        counter_name = f"udf{i} counter"

        udf = _make_batch_udf(
            counter_name=counter_name,
            factor=factor,
            use_process=use_process,
            is_async=is_async,
        )
        df = df.with_column(f"out_{i}", udf(daft.col("value")))

        cases.append(
            {
                "counter_name": counter_name,
                "expected_counter": factor * VALUES_SUM,
            }
        )

    df.collect()

    for case in cases:
        stats = _find_udf_stats(df.metrics, case["counter_name"])
        assert stats == case["expected_counter"]


@pytest.mark.parametrize("use_process", [False, True])
def test_udf_custom_metrics_shared_counter(use_process: bool) -> None:
    df = daft.from_pydict({"value": VALUES})

    shared_counter_name = "shared counter"

    @daft.func(use_process=use_process)
    def udf_increment_by_value(
        value: int,
        *,
        _counter_name: str = shared_counter_name,
    ) -> int:
        metrics.increment_counter(_counter_name, amount=value)
        return value + 1

    @daft.func(use_process=use_process)
    def udf_increment_by_double(
        value: int,
        *,
        _counter_name: str = shared_counter_name,
    ) -> int:
        metrics.increment_counter(_counter_name, amount=value * 2)
        return value + 2

    df = df.with_column("out_0", udf_increment_by_value(daft.col("value")))
    df = df.with_column("out_1", udf_increment_by_double(daft.col("value")))

    df.collect()

    counter_values = []
    for operator in df.metrics.to_pylist():
        for stat_name, stat_value_unit in operator["stats"]:
            if stat_name == shared_counter_name:
                counter_values.append(stat_value_unit["value"])

    assert counter_values, "Shared counter metric not found in operator stats"
    assert sum(counter_values) == 3 * VALUES_SUM


@pytest.mark.parametrize("num_udfs", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
@pytest.mark.parametrize("concurrency", [None, 1])
@pytest.mark.parametrize("is_async", [False, True])
def test_udf_custom_metrics_cls(num_udfs: int, batch_size: int | None, concurrency: int | None, is_async: bool) -> None:
    if concurrency is not None and get_tests_daft_runner_name() == "ray":
        pytest.skip("Ray runner does not support UDF metrics for actor-based UDFs")

    df = daft.from_pydict({"value": VALUES})
    if batch_size is not None:
        df = df.into_batches(batch_size)

    cases = []
    for i in range(num_udfs):
        factor = i + 1
        counter_name = f"udf{i} counter"

        instance = _make_cls_udf(
            factor=factor,
            counter_name=counter_name,
            concurrency=concurrency,
            is_async=is_async,
        )

        cases.append(
            {
                "counter_name": counter_name,
                "expected_counter": factor * VALUES_SUM,
            }
        )

        df = df.with_column(f"out_{i}", instance(daft.col("value")))

    df.collect()

    for case in cases:
        stats = _find_udf_stats(df.metrics, case["counter_name"])
        assert stats == case["expected_counter"]
