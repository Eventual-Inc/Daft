from __future__ import annotations

import asyncio
import functools
from collections import defaultdict
from collections.abc import Mapping
from typing import Any, Callable

import pytest

import daft
from daft import DataType, Series
from daft.subscribers import StatType, Subscriber
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="Custom UDF metrics tests require the native runner with subscriber support",
)


class MetricsSubscriber(Subscriber):
    def __init__(self) -> None:
        self.query_ids: list[str] = []
        self.node_stats: defaultdict[str, defaultdict[int, dict[str, tuple[StatType, Any]]]] = defaultdict(
            lambda: defaultdict(dict)
        )

    def on_query_start(self, query_id: str, metadata: Any) -> None:
        self.query_ids.append(query_id)

    def on_query_end(self, query_id: str) -> None:
        pass

    def on_result_out(self, query_id: str, result: Any) -> None:
        pass

    def on_optimization_start(self, query_id: str) -> None:
        pass

    def on_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        pass

    def on_exec_start(self, query_id: str, node_infos: list[Any]) -> None:
        pass

    def on_exec_operator_start(self, query_id: str, node_id: int) -> None:
        pass

    def on_exec_emit_stats(
        self,
        query_id: str,
        all_stats: Mapping[int, Mapping[str, tuple[StatType, Any]]],
    ) -> None:  # type: ignore[override]
        for node_id, stats in all_stats.items():
            self.node_stats[query_id][node_id] = {
                name: (stat_type, value) for name, (stat_type, value) in stats.items()
            }

    def on_exec_operator_end(self, query_id: str, node_id: int) -> None:
        pass

    def on_exec_end(self, query_id: str) -> None:
        pass


def _find_udf_stats(subscriber: MetricsSubscriber, metric_name: str) -> dict[str, tuple[StatType, Any]]:
    assert subscriber.query_ids, "No queries recorded by subscriber"
    query_id = subscriber.query_ids[-1]
    for stats in subscriber.node_stats[query_id].values():
        if metric_name in stats:
            return stats
    raise AssertionError(f"Metric '{metric_name}' not found in emitted stats")


VALUES = list(range(1, 11))
VALUES_SUM = sum(VALUES)


def _wrap_async(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    async def async_fn(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(0)
        return fn(*args, **kwargs)

    return async_fn


def _make_scalar_udf(
    *,
    counter_metric: Any,
    factor: int,
    use_process: bool,
    is_async: bool,
) -> Callable[[Any], Any]:
    def body(
        value: int,
        *,
        _ctr=counter_metric,
        _factor=factor,
    ) -> int:
        _ctr.increment(amount=value * _factor)
        return value + _factor

    impl = _wrap_async(body) if is_async else body
    return daft.func(use_process=use_process)(impl)


def _make_batch_udf(
    *,
    counter_metric: Any,
    factor: int,
    use_process: bool,
    is_async: bool,
) -> Callable[[Any], Any]:
    def body(
        values: Series,
        *,
        _ctr=counter_metric,
        _factor=factor,
    ) -> Series:
        py_values = values.to_pylist()
        for v in py_values:
            _ctr.increment(amount=v * _factor)
        return Series.from_pylist([v + _factor for v in py_values])

    impl = _wrap_async(body) if is_async else body
    return daft.func.batch(return_dtype=DataType.int64(), use_process=use_process)(impl)


def _make_cls_udf(
    *,
    metrics_module: Any,
    factor: int,
    counter_name: str,
    concurrency: int | None,
    is_async: bool,
) -> Callable[[Any], Any]:
    if is_async:

        async def call_impl(self: Any, value: int) -> int:
            await asyncio.sleep(0)  # Make it async but don't slow down tests
            self.counter.increment(amount=value * self.addend)
            return value + self.addend

    else:

        def call_impl(self: Any, value: int) -> int:
            self.counter.increment(amount=value * self.addend)
            return value + self.addend

    @daft.cls(max_concurrency=concurrency)
    class MetricUdf:
        def __init__(
            self,
            addend: int,
            counter_name: str,
        ) -> None:
            self.addend = addend
            self.counter = metrics_module.counter(counter_name)

        __call__ = call_impl  # type: ignore[assignment]

    return MetricUdf(factor, counter_name)


@pytest.mark.parametrize("num_udfs", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
@pytest.mark.parametrize("use_process", [False, True])
@pytest.mark.parametrize("is_async", [False, True])
def test_udf_custom_metrics_func(num_udfs: int, batch_size: int | None, use_process: bool, is_async: bool) -> None:
    ctx = daft.context.get_context()
    subscriber = MetricsSubscriber()
    sub_name = (
        f"udf-metrics-func-{'multi' if num_udfs == 2 else 'single'}-"
        f"{batch_size}-{'proc' if use_process else 'inline'}-{'async' if is_async else 'sync'}"
    )
    ctx.attach_subscriber(sub_name, subscriber)

    try:
        df = daft.from_pydict({"value": VALUES})
        if batch_size is not None:
            df = df.into_batches(batch_size)

        cases = []
        for i in range(num_udfs):
            from daft.udf import metrics

            factor = i + 1
            counter_name = f"udf{i} counter"

            counter_metric = metrics.counter(counter_name)

            udf = _make_scalar_udf(
                counter_metric=counter_metric,
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
            stats = _find_udf_stats(subscriber, case["counter_name"])
            _, counter_value = stats[case["counter_name"]]
            assert counter_value == case["expected_counter"]
    finally:
        ctx.detach_subscriber(sub_name)


@pytest.mark.parametrize("num_udfs", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
@pytest.mark.parametrize("use_process", [False, True])
@pytest.mark.parametrize("is_async", [False, True])
def test_udf_custom_metrics_batch(num_udfs: int, batch_size: int | None, use_process: bool, is_async: bool) -> None:
    ctx = daft.context.get_context()
    subscriber = MetricsSubscriber()
    sub_name = (
        f"udf-metrics-batch-{'multi' if num_udfs == 2 else 'single'}-"
        f"{batch_size}-{'proc' if use_process else 'inline'}-{'async' if is_async else 'sync'}"
    )
    ctx.attach_subscriber(sub_name, subscriber)

    try:
        df = daft.from_pydict({"value": VALUES})
        if batch_size is not None:
            df = df.into_batches(batch_size)

        cases = []
        for i in range(num_udfs):
            from daft.udf import metrics

            factor = i + 1
            counter_name = f"udf{i} counter"

            counter_metric = metrics.counter(counter_name)

            udf = _make_batch_udf(
                counter_metric=counter_metric,
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
            stats = _find_udf_stats(subscriber, case["counter_name"])
            _, counter_value = stats[case["counter_name"]]
            assert counter_value == case["expected_counter"]
    finally:
        ctx.detach_subscriber(sub_name)


@pytest.mark.parametrize("num_udfs", [1, 2])
@pytest.mark.parametrize("batch_size", [None, 1, 4])
@pytest.mark.parametrize("concurrency", [None, 1])
@pytest.mark.parametrize("is_async", [False, True])
def test_udf_custom_metrics_cls(num_udfs: int, batch_size: int | None, concurrency: int | None, is_async: bool) -> None:
    ctx = daft.context.get_context()
    subscriber = MetricsSubscriber()
    sub_name = (
        f"udf-metrics-cls-{'multi' if num_udfs == 2 else 'single'}-"
        f"{batch_size}-{concurrency}-{'async' if is_async else 'sync'}"
    )
    ctx.attach_subscriber(sub_name, subscriber)

    try:
        df = daft.from_pydict({"value": VALUES})
        if batch_size is not None:
            df = df.into_batches(batch_size)

        cases = []
        for i in range(num_udfs):
            from daft.udf import metrics

            factor = i + 1
            counter_name = f"udf{i} counter"

            instance = _make_cls_udf(
                metrics_module=metrics,
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
            stats = _find_udf_stats(subscriber, case["counter_name"])
            _, counter_value = stats[case["counter_name"]]
            assert counter_value == case["expected_counter"]
    finally:
        ctx.detach_subscriber(sub_name)
