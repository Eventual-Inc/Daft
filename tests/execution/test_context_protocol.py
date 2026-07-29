from __future__ import annotations

import pyarrow as pa
import pytest

from daft.execution.context_protocol import (
    BatchLease,
    ColumnRepresentation,
    ContextChangeKind,
    ContextStateTracker,
    LeaseLedger,
    RowTransformCache,
    RowTransformKey,
    SourceSnapshot,
    model_column,
    retrieval_recall,
)


def test_model_column_keeps_complex_layouts_explicit() -> None:
    nullable = pa.array([10, None, 30], type=pa.int64())
    nullable_model = model_column(nullable)
    value_buffer = nullable.buffers()[1]
    assert value_buffer is not None
    assert nullable_model.representation == ColumnRepresentation.DENSE_TENSOR_WITH_VALIDITY
    assert nullable_model.borrowed_read_only
    assert nullable_model.tensor is not None
    assert nullable_model.tensor.data_ptr() == value_buffer.address
    validity_buffer = nullable.buffers()[0]
    assert validity_buffer is not None
    assert nullable_model.validity is not None
    assert nullable_model.validity.address == validity_buffer.address
    adjusted = nullable_model.tensor + 1
    assert [adjusted[0].item(), adjusted[2].item()] == [11, 31]
    assert nullable.to_pylist() == [10, None, 30]

    dictionary = pa.array(["gold", "silver", "gold"]).dictionary_encode()
    dictionary_model = model_column(dictionary)
    index_buffer = dictionary.indices.buffers()[1]
    assert index_buffer is not None
    assert dictionary_model.representation == ColumnRepresentation.DICTIONARY_CODES
    assert dictionary_model.tensor is not None
    assert dictionary_model.tensor.data_ptr() == index_buffer.address
    assert dictionary_model.dictionary.equals(dictionary.dictionary)

    for arrow_only in (
        pa.array(["alpha", "beta"]),
        pa.array([[1, 2], [3]]),
        pa.array([True, False]),
    ):
        converted = model_column(arrow_only)
        assert converted.representation == ColumnRepresentation.ARROW_ONLY
        assert converted.tensor is None
        assert converted.reason is not None

    with pytest.raises(ValueError, match="one Arrow chunk"):
        model_column(pa.chunked_array([[1, 2], [3, 4]]))


def test_batch_leases_release_once_on_success_error_and_cancel() -> None:
    ledger = LeaseLedger()
    released: list[str] = []

    with ledger.acquire(
        "success",
        byte_count=10,
        device="cpu",
        release_callback=lambda: released.append("success"),
    ) as success:
        assert success.value == "success"
    success.release()

    with (
        pytest.raises(RuntimeError, match="model failed"),
        ledger.acquire(
            "error",
            byte_count=20,
            device="cpu",
            release_callback=lambda: released.append("error"),
        ),
    ):
        raise RuntimeError("model failed")

    cancelled: BatchLease[str] = ledger.acquire(
        "cancelled",
        byte_count=30,
        device="cuda:0",
        release_callback=lambda: released.append("cancelled"),
    )
    cancelled.release()
    cancelled.release()

    assert released == ["success", "error", "cancelled"]
    assert ledger.snapshot().acquired == 3
    assert ledger.snapshot().released == 3
    ledger.assert_clear()


def test_batch_lease_credit_bounds_live_batches_and_records_wait() -> None:
    ledger = LeaseLedger(max_live_count=3)
    leases = [ledger.acquire(value, byte_count=10, device="cpu") for value in ("queued-1", "queued-2", "executing")]
    assert ledger.snapshot().peak_count == 3

    with pytest.raises(TimeoutError, match="batch lease credit"):
        ledger.acquire("blocked", byte_count=10, device="cpu", timeout=0.01)
    assert ledger.snapshot().wait_count == 1
    assert ledger.snapshot().wait_seconds >= 0.01
    assert ledger.snapshot().peak_count == 3

    leases.pop().release()
    resumed = ledger.acquire("resumed", byte_count=10, device="cpu")
    for lease in leases:
        lease.release()
    resumed.release()
    ledger.assert_clear()


def test_context_state_emits_add_remove_update_and_reorder() -> None:
    tracker = ContextStateTracker()
    first = pa.table(
        {
            "seed": ["A", "A", "B"],
            "row_id": [101, 102, 201],
            "row_version": [1, 1, 1],
        }
    )
    assert [
        change.kind
        for change in tracker.update(
            first,
            seed_column="seed",
            key_column="row_id",
            version_column="row_version",
        )
    ] == [ContextChangeKind.ADD, ContextChangeKind.ADD, ContextChangeKind.ADD]

    changed = pa.table(
        {
            "seed": ["A", "A", "A"],
            "row_id": [102, 101, 103],
            "row_version": [1, 2, 1],
        }
    )
    changes = tracker.update(
        changed,
        seed_column="seed",
        key_column="row_id",
        version_column="row_version",
    )
    observed = {(change.kind, change.seed_key, change.row_key) for change in changes}
    assert observed == {
        (ContextChangeKind.REMOVE, "B", 201),
        (ContextChangeKind.REORDER, "A", 101),
        (ContextChangeKind.UPDATE, "A", 101),
        (ContextChangeKind.REORDER, "A", 102),
        (ContextChangeKind.ADD, "A", 103),
    }


def test_context_state_fetch_more_changes_only_requested_seeds() -> None:
    tracker = ContextStateTracker()
    initial = pa.table(
        {
            "seed": ["A", "B"],
            "row_id": [101, 201],
            "row_version": [1, 1],
        }
    )
    tracker.update(
        initial,
        seed_column="seed",
        key_column="row_id",
        version_column="row_version",
    )

    fetch_more = pa.table(
        {
            "seed": ["A", "A"],
            "row_id": [101, 103],
            "row_version": [1, 1],
        }
    )
    changes = tracker.update(
        fetch_more,
        seed_column="seed",
        key_column="row_id",
        version_column="row_version",
        replace_seeds=["A"],
    )
    assert [(change.kind, change.seed_key, change.row_key) for change in changes] == [(ContextChangeKind.ADD, "A", 103)]

    with pytest.raises(ValueError, match="outside its replacement scope"):
        tracker.update(
            fetch_more,
            seed_column="seed",
            key_column="row_id",
            version_column="row_version",
            replace_seeds=["B"],
        )


def test_row_transform_cache_reuses_overlap_and_invalidates_one_row() -> None:
    cache: RowTransformCache[str] = RowTransformCache()

    def key(row_id: int, version: int = 1) -> RowTransformKey:
        return RowTransformKey(
            table="orders",
            row_key=row_id,
            row_version=version,
            columns=("amount", "product_id"),
            transform_version="model-v1",
            dtype="float32",
            layout="contiguous",
            device="cuda:0",
        )

    computed: list[int] = []

    def transform(row_id: int) -> tuple[str, int]:
        computed.append(row_id)
        return f"encoded-{row_id}", 16

    for row_id in range(10):
        assert cache.get_or_compute(key(row_id), lambda row_id=row_id: transform(row_id)) == f"encoded-{row_id}"
    for row_id in [*range(8), 10, 11]:
        assert cache.get_or_compute(key(row_id), lambda row_id=row_id: transform(row_id)) == f"encoded-{row_id}"

    overlap_snapshot = cache.snapshot()
    assert overlap_snapshot.hits == 8
    assert overlap_snapshot.misses == 12
    assert overlap_snapshot.reused_bytes == 8 * 16

    assert cache.invalidate_rows("orders", [3]) == 1
    assert cache.get_or_compute(key(3, 2), lambda: transform(3)) == "encoded-3"
    assert cache.get_or_compute(key(4), lambda: transform(4)) == "encoded-4"
    final = cache.snapshot()
    assert final.hits == 9
    assert final.misses == 13
    assert final.invalidated == 1
    assert final.entries == 12


def test_source_snapshot_and_first_quality_metric() -> None:
    first = SourceSnapshot.observed_facts(
        "parquet",
        "orders/",
        {"file_count": 2, "total_bytes": 4096, "max_mtime_ns": 100, "schema": "v1"},
    )
    same = SourceSnapshot.observed_facts(
        "parquet",
        "orders/",
        {"schema": "v1", "max_mtime_ns": 100, "total_bytes": 4096, "file_count": 2},
    )
    changed = SourceSnapshot.observed_facts(
        "parquet",
        "orders/",
        {"file_count": 2, "total_bytes": 4104, "max_mtime_ns": 101, "schema": "v1"},
    )
    assert first.version == same.version
    assert first.version != changed.version
    assert first.method == "observed-facts"

    with pytest.raises(ValueError, match="observed source facts"):
        SourceSnapshot.observed_facts("parquet", "orders/", {})

    relevant = [101, 103, 107, 109]
    baseline = retrieval_recall([101, 102, 104, 105], relevant)
    model_guided = retrieval_recall([101, 103, 107, 108], relevant)
    assert baseline == 0.25
    assert model_guided == 0.75
