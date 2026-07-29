from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import torch

import daft
from daft import DataType, col
from daft.context import get_context
from daft.daft import LocalPhysicalPlan
from daft.execution.context_protocol import (
    ContextChangeKind,
    ContextProtocolConfig,
    RowTransformRequest,
    SourceSnapshot,
)
from daft.execution.native_executor import NativeContextSession
from daft.recordbatch import MicroPartition
from daft.runners import get_or_create_runner
from tests.conftest import get_tests_daft_runner_name

if TYPE_CHECKING:
    from collections.abc import Mapping

    from daft.context import DaftContext
    from daft.daft import Input, PyMicroPartition
    from daft.dataframe import DataFrame


def _partition_sets() -> dict[str, list[PyMicroPartition]]:
    runner = get_or_create_runner()
    return {
        key: [value.micropartition()._micropartition for value in values.values()]
        for key, values in runner._part_set_cache.get_all_partition_sets().items()
    }


def _compile(df: DataFrame) -> tuple[LocalPhysicalPlan, Mapping[int, Input | list[PyMicroPartition]], DaftContext]:
    ctx = get_context()
    builder = df._builder.optimize(ctx.daft_execution_config)
    plan, inputs = LocalPhysicalPlan.from_logical_plan_builder(builder._builder, _partition_sets())
    return plan, inputs, ctx


def _collect_arrow(result_round: object) -> pa.Table:
    parts = [result.partition() for result in result_round]
    assert parts
    return MicroPartition.concat(parts).to_arrow()


def _arrow_values_to_torch(values: daft.Series) -> tuple[torch.Tensor, int]:
    arrow_values = values.to_arrow()
    assert isinstance(arrow_values, pa.Array)
    assert arrow_values.null_count == 0
    value_buffer = arrow_values.buffers()[1]
    assert value_buffer is not None
    byte_offset = arrow_values.offset * 8
    torch_values = torch.frombuffer(
        memoryview(value_buffer)[byte_offset:],
        dtype=torch.float64,
        count=len(arrow_values),
    )
    return torch_values, value_buffer.address + byte_offset


def _torch_to_arrow(values: torch.Tensor, dtype: pa.DataType) -> pa.Array:
    values = values.contiguous()
    value_buffer = pa.foreign_buffer(values.data_ptr(), values.numel() * values.element_size(), base=values)
    return pa.Array.from_buffers(dtype, len(values), [None, value_buffer])


def _build_scored_candidates(
    seed_ids: pa.Array,
    scorer: object,
    *,
    feedback: pa.Table | None = None,
    first_order_amount: float = 50.0,
    first_order_version: int = 1,
) -> DataFrame:
    if feedback is None:
        feedback = pa.table(
            {
                "order_id": pa.array([101, 102, 103, 104, 105], type=pa.int64()),
                "keep": pa.array([True, True, True, True, True]),
            }
        )
    seeds = daft.from_arrow(
        pa.table(
            {
                "customer_id": seed_ids,
                "as_of": pa.array([20, 20], type=pa.int64()),
            }
        )
    )
    customers = daft.from_arrow(
        pa.table(
            {
                "customer_id": pa.array([1, 2, 3], type=pa.int64()),
                "tier": pa.array([1.0, 2.0, 0.5], type=pa.float64()),
            }
        )
    )
    orders = daft.from_arrow(
        pa.table(
            {
                "order_id": pa.array([101, 102, 103, 104, 105], type=pa.int64()),
                "customer_id": pa.array([1, 1, 2, 2, 3], type=pa.int64()),
                "product_id": pa.array([10, 11, 10, 12, 11], type=pa.int64()),
                "order_ts": pa.array([5, 21, 15, 18, 9], type=pa.int64()),
                "amount": pa.array(
                    [first_order_amount, 90.0, 80.0, 70.0, 20.0],
                    type=pa.float64(),
                ),
                "row_version": pa.array(
                    [first_order_version, 1, 1, 1, 1],
                    type=pa.int64(),
                ),
            }
        )
    )
    products = daft.from_arrow(
        pa.table(
            {
                "product_id": pa.array([10, 11, 12], type=pa.int64()),
                "valid_from": pa.array([0, 0, 19], type=pa.int64()),
                "valid_to": pa.array([100, 20, 100], type=pa.int64()),
                "product_bias": pa.array([0.5, 0.2, 0.1], type=pa.float64()),
            }
        )
    )

    candidates = (
        seeds.join(customers, on="customer_id")
        .join(orders, on="customer_id")
        .join(products, on="product_id")
        .where(
            (col("order_ts") <= col("as_of"))
            & (col("valid_from") <= col("order_ts"))
            & (col("order_ts") < col("valid_to"))
        )
    )
    scored = candidates.select(
        "order_id",
        "customer_id",
        "product_id",
        "order_ts",
        "amount",
        "row_version",
        scorer.score(candidates["amount"], candidates["tier"], candidates["product_bias"]),
    )
    return (
        scored.join(daft.from_arrow(feedback), on="order_id")
        .where(col("keep"))
        .sort("order_id")
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "order_ts",
            "amount",
            "row_version",
            "score",
            "model_call",
            "model_instance",
            "arrow_value_ptr",
            "tensor_value_ptr",
            "score_tensor_ptr",
            "score_arrow_ptr",
        )
    )


def _make_scorer() -> object:
    output_type = DataType.struct(
        {
            "score": DataType.float64(),
            "model_call": DataType.int64(),
            "model_instance": DataType.int64(),
            "arrow_value_ptr": DataType.int64(),
            "tensor_value_ptr": DataType.int64(),
            "score_tensor_ptr": DataType.int64(),
            "score_arrow_ptr": DataType.int64(),
        }
    )

    @daft.cls(use_process=False)
    class DeterministicTorchScorer:
        def __init__(self) -> None:
            self.calls = 0

        @daft.method.batch(return_dtype=output_type, unnest=True)
        def score(self, amount: daft.Series, tier: daft.Series, product_bias: daft.Series) -> daft.Series:
            amount_tensor, amount_arrow_ptr = _arrow_values_to_torch(amount)
            tier_tensor, _ = _arrow_values_to_torch(tier)
            bias_tensor, _ = _arrow_values_to_torch(product_bias)

            self.calls += 1
            scores = amount_tensor * 0.01 + tier_tensor + bias_tensor
            score_ptr = scores.data_ptr()
            score_array = _torch_to_arrow(scores, pa.float64())
            score_buffer = score_array.buffers()[1]
            assert score_buffer is not None
            row_count = len(scores)
            result = pa.StructArray.from_arrays(
                [
                    score_array,
                    _torch_to_arrow(torch.full((row_count,), self.calls, dtype=torch.int64), pa.int64()),
                    _torch_to_arrow(torch.full((row_count,), id(self), dtype=torch.int64), pa.int64()),
                    _torch_to_arrow(torch.full((row_count,), amount_arrow_ptr, dtype=torch.int64), pa.int64()),
                    _torch_to_arrow(torch.full((row_count,), amount_tensor.data_ptr(), dtype=torch.int64), pa.int64()),
                    _torch_to_arrow(torch.full((row_count,), score_ptr, dtype=torch.int64), pa.int64()),
                    _torch_to_arrow(torch.full((row_count,), score_buffer.address, dtype=torch.int64), pa.int64()),
                ],
                names=[
                    "score",
                    "model_call",
                    "model_instance",
                    "arrow_value_ptr",
                    "tensor_value_ptr",
                    "score_tensor_ptr",
                    "score_arrow_ptr",
                ],
            )
            return daft.Series.from_arrow(result)

    return DeterministicTorchScorer()


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires the native runner")
def test_retained_context_rounds_feedback_and_fetch_more() -> None:
    scorer = _make_scorer()
    first_df = _build_scored_candidates(pa.array([1, 3], type=pa.int64()), scorer)
    baseline = _build_scored_candidates(
        pa.array([1, 3], type=pa.int64()),
        _make_scorer(),
    ).to_arrow()
    plan, first_inputs, ctx = _compile(first_df)
    initial_snapshot = SourceSnapshot.observed_facts(
        "generated",
        "customer-order-product",
        {"dataset_version": 1, "schema": "v1"},
    )
    session = NativeContextSession(
        plan,
        ctx,
        {"query_id": "host-context-session"},
        protocol_config=ContextProtocolConfig(
            seed_column="customer_id",
            key_column="order_id",
            version_column="row_version",
            source_snapshot=initial_snapshot,
        ),
    )
    transformed: list[tuple[int, int]] = []
    row_transform = RowTransformRequest(
        table="orders",
        columns=("amount", "product_id"),
        transform_version="model-v1",
        dtype="float64",
        layout="contiguous",
        device="cpu",
        transform=lambda row_key, version: (
            transformed.append((int(row_key), int(version))) or {"encoding": f"encoded-{row_key}-v{version}"},
            16,
        ),
    )

    with session.run_context_round(
        first_inputs,
        replace_seeds=[1, 3],
        row_transform=row_transform,
    ) as first_batch:
        first = first_batch.table
        assert first_batch.session_id == session.session_id
        assert first_batch.plan_fingerprint == session.fingerprint
        assert first_batch.revision == 1
        assert first_batch.query_plan
        retained_query_plan = first_batch.query_plan
        assert {change.kind for change in first_batch.changes} == {ContextChangeKind.ADD}
        assert {change.row_key for change in first_batch.changes} == {101, 105}
        assert first_batch.cache_snapshot.misses == 2
        first_transform_101 = first_batch.row_transforms[0]
        assert first_transform_101 == {"encoding": "encoded-101-v1"}
        assert first_batch.lease_snapshot.live_count == 1
        assert baseline["order_id"].to_pylist() == [101, 105]
        assert first["order_id"].to_pylist() == baseline["order_id"].to_pylist()
        assert first["model_call"].to_pylist() == [1, 1]
        assert first["arrow_value_ptr"].to_pylist() == first["tensor_value_ptr"].to_pylist()
        assert first["score_arrow_ptr"].to_pylist() == first["score_tensor_ptr"].to_pylist()
        first_model_instance = first["model_instance"].to_pylist()[0]

        keep = pc.greater_equal(first["score"], 1.5)
        guided_feedback = pa.table({"order_id": first["order_id"], "keep": keep})
        feedback_lease = session.protocol.lease_feedback(guided_feedback)

    with feedback_lease:
        guided_df = _build_scored_candidates(
            pa.array([1, 3], type=pa.int64()),
            scorer,
            feedback=feedback_lease.value,
        )
        _, guided_inputs, _ = _compile(guided_df)
        assert set(first_inputs) == set(guided_inputs)
        with session.run_context_round(
            guided_inputs,
            replace_seeds=[1, 3],
            row_transform=row_transform,
        ) as guided_batch:
            guided = guided_batch.table
            assert guided["order_id"].to_pylist() == [101]
            assert guided["model_call"].to_pylist() == [2]
            assert guided["model_instance"].to_pylist()[0] == first_model_instance
            assert guided_batch.query_plan == retained_query_plan
            assert guided_batch.lease_snapshot.live_count == 2
            assert [(change.kind, change.row_key) for change in guided_batch.changes] == [
                (ContextChangeKind.REMOVE, 105)
            ]
            assert guided_batch.cache_snapshot.hits == 1
            assert guided_batch.row_transforms[0] is first_transform_101

    fetch_more_df = _build_scored_candidates(
        pa.array([2, 999], type=pa.int64()),
        scorer,
        feedback=pa.table(
            {
                "order_id": pa.array([103], type=pa.int64()),
                "keep": pa.array([True]),
            }
        ),
    )
    _, fetch_more_inputs, _ = _compile(fetch_more_df)
    assert set(first_inputs) == set(fetch_more_inputs)
    with session.run_context_round(
        fetch_more_inputs,
        replace_seeds=[2, 999],
        row_transform=row_transform,
    ) as fetched_batch:
        fetched = fetched_batch.table
        assert fetched["order_id"].to_pylist() == [103]
        assert fetched["model_call"].to_pylist() == [3]
        assert fetched["model_instance"].to_pylist()[0] == first_model_instance
        assert fetched_batch.query_plan == retained_query_plan
        assert [(change.kind, change.row_key) for change in fetched_batch.changes] == [(ContextChangeKind.ADD, 103)]
        assert fetched_batch.cache_snapshot.misses == 3
        assert fetched_batch.row_transforms == ({"encoding": "encoded-103-v1"},)

    updated_snapshot = SourceSnapshot.observed_facts(
        "generated",
        "customer-order-product",
        {"dataset_version": 2, "schema": "v1"},
    )
    assert session.update_source(updated_snapshot, {"orders": [101]}) == 1
    updated_df = _build_scored_candidates(
        pa.array([1, 999], type=pa.int64()),
        scorer,
        feedback=pa.table(
            {
                "order_id": pa.array([101], type=pa.int64()),
                "keep": pa.array([True]),
            }
        ),
        first_order_amount=60.0,
        first_order_version=2,
    )
    _, updated_inputs, _ = _compile(updated_df)
    with session.run_context_round(
        updated_inputs,
        replace_seeds=[1, 999],
        row_transform=row_transform,
    ) as updated_batch:
        assert updated_batch.table["order_id"].to_pylist() == [101]
        assert updated_batch.table["model_call"].to_pylist() == [4]
        assert updated_batch.table["model_instance"].to_pylist()[0] == first_model_instance
        assert updated_batch.query_plan == retained_query_plan
        assert updated_batch.source_snapshot == updated_snapshot
        assert [(change.kind, change.row_key) for change in updated_batch.changes] == [(ContextChangeKind.UPDATE, 101)]
        assert updated_batch.cache_snapshot.invalidated == 1
        assert updated_batch.cache_snapshot.misses == 4
        assert updated_batch.row_transforms[0] == {"encoding": "encoded-101-v2"}
        assert updated_batch.row_transforms[0] is not first_transform_101

    assert transformed == [(101, 1), (105, 1), (103, 1), (101, 2)]
    assert session.protocol.lease_snapshot.live_count == 0
    assert session.protocol.lease_snapshot.acquired == 5
    assert session.protocol.lease_snapshot.released == 5

    session.close()
    assert session.closed
    assert session.active_plan_count == 0


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires the native runner")
def test_context_session_bounds_submission_and_cancels() -> None:
    scorer = _make_scorer()
    df = _build_scored_candidates(pa.array([1, 3], type=pa.int64()), scorer)
    plan, inputs, ctx = _compile(df)
    session = NativeContextSession(plan, ctx)

    result_round = session.run_round(inputs)
    with pytest.raises(RuntimeError, match="active input round"):
        session.run_round(inputs)
    assert session.active_plan_count == 1

    result_round.close()
    assert session.closed
    assert session.active_plan_count == 0


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires the native runner")
def test_context_session_cancel_releases_protocol_batch() -> None:
    scorer = _make_scorer()
    df = _build_scored_candidates(pa.array([1, 3], type=pa.int64()), scorer)
    plan, inputs, ctx = _compile(df)
    session = NativeContextSession(
        plan,
        ctx,
        protocol_config=ContextProtocolConfig(
            seed_column="customer_id",
            key_column="order_id",
            version_column="row_version",
            source_snapshot=SourceSnapshot.transactional("generated", "context", "v1"),
        ),
    )

    batch = session.run_context_round(inputs, replace_seeds=[1, 3])
    assert not batch.rows.released
    started = time.perf_counter()
    session.cancel()
    assert time.perf_counter() - started < 0.5
    assert batch.rows.released
    assert session.protocol.lease_snapshot.live_count == 0
    assert session.active_plan_count == 0


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires the native runner")
def test_context_session_reserves_credit_before_model_work() -> None:
    scorer = _make_scorer()
    df = _build_scored_candidates(pa.array([1, 3], type=pa.int64()), scorer)
    plan, inputs, ctx = _compile(df)
    session = NativeContextSession(
        plan,
        ctx,
        protocol_config=ContextProtocolConfig(
            seed_column="customer_id",
            key_column="order_id",
            version_column="row_version",
            source_snapshot=SourceSnapshot.transactional("generated", "context", "v1"),
            max_live_batches=1,
        ),
    )

    first_batch = session.run_context_round(inputs, replace_seeds=[1, 3])
    assert first_batch.table["model_call"].to_pylist() == [1, 1]

    with ThreadPoolExecutor(max_workers=1) as pool:
        blocked_round = pool.submit(
            session.run_context_round,
            inputs,
            replace_seeds=[1, 3],
            lease_timeout=2.0,
        )
        deadline = time.perf_counter() + 1.0
        while session.protocol.lease_snapshot.wait_count == 0 and time.perf_counter() < deadline:
            time.sleep(0.001)
        assert session.protocol.lease_snapshot.wait_count == 1
        assert not blocked_round.done()
        assert first_batch.table["model_call"].to_pylist() == [1, 1]

        first_batch.release()
        second_batch = blocked_round.result(timeout=2.0)

    assert second_batch.table["model_call"].to_pylist() == [2, 2]
    second_batch.release()
    session.close()


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires the native runner")
def test_context_session_cancels_during_active_model_work(tmp_path: Path) -> None:
    started_path = str(tmp_path / "worker-started")
    exit_path = str(tmp_path / "allow-worker-exit")

    @daft.func.batch(return_dtype=DataType.int64(), use_process=False)
    def wait_for_cancel(values: daft.Series) -> daft.Series:
        import time as wait_time
        from pathlib import Path

        Path(started_path).touch()
        deadline = wait_time.perf_counter() + 5.0
        while not Path(exit_path).exists() and wait_time.perf_counter() < deadline:
            wait_time.sleep(0.001)
        return values

    source = daft.from_arrow(
        pa.table(
            {
                "customer_id": pa.array([1], type=pa.int64()),
                "order_id": pa.array([101], type=pa.int64()),
                "row_version": pa.array([1], type=pa.int64()),
                "amount": pa.array([50], type=pa.int64()),
            }
        )
    )
    df = source.select(
        "customer_id",
        "order_id",
        "row_version",
        wait_for_cancel(source["amount"]).alias("amount"),
    )
    plan, inputs, ctx = _compile(df)
    session = NativeContextSession(
        plan,
        ctx,
        protocol_config=ContextProtocolConfig(
            seed_column="customer_id",
            key_column="order_id",
            version_column="row_version",
            source_snapshot=SourceSnapshot.transactional("generated", "context", "v1"),
            max_live_batches=1,
        ),
    )
    pending = session.start_context_round(inputs, replace_seeds=[1])

    with ThreadPoolExecutor(max_workers=1) as pool:
        collecting = pool.submit(pending.collect)
        deadline = time.perf_counter() + 2.0
        while not Path(started_path).exists() and time.perf_counter() < deadline:
            time.sleep(0.001)
        assert Path(started_path).exists()
        started = time.perf_counter()
        session.cancel()
        elapsed = time.perf_counter() - started
        Path(exit_path).touch()
        assert collecting.exception(timeout=2.0) is not None

    assert elapsed < 0.5
    assert session.protocol.lease_snapshot.live_count == 0
    assert session.protocol.lease_snapshot.acquired == 1
    assert session.protocol.lease_snapshot.released == 1
    assert session.active_plan_count == 0


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires the native runner")
def test_context_session_error_removes_cached_plan() -> None:
    @daft.func.batch(return_dtype=DataType.int64(), use_process=False)
    def fail(values: daft.Series) -> daft.Series:
        raise RuntimeError("expected retained-round failure")

    df = daft.from_arrow(pa.table({"value": pa.array([1, 2], type=pa.int64())})).select(fail(col("value")))
    plan, inputs, ctx = _compile(df)
    session = NativeContextSession(plan, ctx)

    with pytest.raises(Exception, match="expected retained-round failure"):
        list(session.run_round(inputs))
    assert session.closed
    assert session.active_plan_count == 0


def test_context_session_rejects_non_native_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("daft.runners.get_or_infer_runner_type", lambda: "ray")
    with pytest.raises(RuntimeError, match="requires the native runner"):
        NativeContextSession(object(), get_context())  # type: ignore[arg-type]
