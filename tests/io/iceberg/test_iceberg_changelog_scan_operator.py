from __future__ import annotations

import pickle
import threading

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.types import LongType, NestedField, StringType

from daft.daft import IOConfig, StorageConfig
from daft.io.iceberg import _changelog_schema
from daft.io.iceberg.iceberg_changes_scan import IcebergChangesScanOperator


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
        warehouse=f"file://{tmpdir}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


@pytest.fixture(scope="function")
def cow_history_table(local_catalog):
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.operator_cache", schema=schema)
    table.append(pa.table({"id": [1, 2, 3], "data": ["a", "b", "c"]}))
    table.append(pa.table({"id": [4, 5], "data": ["d", "e"]}))
    table.delete("id = 1")
    table.refresh()
    return table


@pytest.fixture(scope="function")
def picklable_cow_history_table(cow_history_table):
    """A `StaticTable` view of `cow_history_table`, loaded straight from its metadata JSON.

    `SqlCatalog`-backed `Table` objects hold a live SQLAlchemy engine/connection (used for
    catalog lookups), which is fundamentally unpicklable and specific to this test's local
    catalog setup -- not a property of `IcebergChangesScanOperator` itself. `StaticTable`
    (what `read_iceberg`/`read_iceberg_changes` actually construct for path-based tables,
    see `daft/io/iceberg/_iceberg.py`) has no such live connection and is picklable, which
    is what actually needs testing here: whether `IcebergChangesScanOperator` itself
    survives a pickle round-trip.
    """
    return StaticTable.from_metadata(cow_history_table.metadata_location)


def _make_operator(table) -> IcebergChangesScanOperator:
    return IcebergChangesScanOperator(
        table,
        start_snapshot_id=None,
        end_snapshot_id=None,
        start_timestamp_ms=None,
        end_timestamp_ms=None,
        storage_config=StorageConfig(True, IOConfig()),
    )


def test_concurrent_to_scan_tasks_reads_each_footer_only_once(cow_history_table, monkeypatch):
    call_count = 0
    lock = threading.Lock()
    real = _changelog_schema._read_footer_arrow_schema

    def counting(path, storage_config):
        nonlocal call_count
        with lock:
            call_count += 1
        return real(path, storage_config)

    monkeypatch.setattr(_changelog_schema, "_read_footer_arrow_schema", counting)

    operator = _make_operator(cow_history_table)

    results: list[list] = []
    errors: list[Exception] = []

    def call_to_scan_tasks():
        try:
            results.append(list(operator.to_scan_tasks(_empty_pushdowns())))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=call_to_scan_tasks) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    # All 8 concurrent calls must see the same, fully-planned task set.
    assert all(len(r) == len(results[0]) for r in results)

    # There are 4 distinct data files across this table's history (file A, file B, and the
    # rewritten file A' from the COW delete, plus the deleted-but-still-listed old file A);
    # regardless of the exact count, the key property is: it must equal the number of
    # *distinct file paths* referenced by the plan, not 8x that (one read per thread) or
    # more.
    tasks = operator._cached_tasks
    expected_distinct_files = len({t.data_file.file_path for t in tasks})
    assert call_count == expected_distinct_files


def test_failed_planning_retries_from_scratch_on_next_call(cow_history_table, monkeypatch):
    attempt = 0
    real = _changelog_schema._read_footer_arrow_schema

    def flaky(path, storage_config):
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            raise ConnectionError("simulated transient I/O failure")
        return real(path, storage_config)

    monkeypatch.setattr(_changelog_schema, "_read_footer_arrow_schema", flaky)

    operator = _make_operator(cow_history_table)

    # First call: the very first footer read fails -> the whole planning attempt fails,
    # and _cached_tasks must remain None (back to UNPLANNED), not a partial/poisoned state.
    with pytest.raises(ConnectionError):
        list(operator.to_scan_tasks(_empty_pushdowns()))
    assert operator._cached_tasks is None

    # Second call: retries planning from scratch (re-reads manifests and footers) and
    # succeeds this time.
    tasks = list(operator.to_scan_tasks(_empty_pushdowns()))
    assert len(tasks) > 0
    assert operator._cached_tasks is not None


def test_operator_pickle_roundtrip_before_planning(picklable_cow_history_table):
    operator = _make_operator(picklable_cow_history_table)
    restored: IcebergChangesScanOperator = pickle.loads(pickle.dumps(operator))

    assert restored._cached_tasks is None
    assert restored._lock is not None
    # Must still work after unpickling: a fresh lock, lazy planning triggered on first use.
    tasks = list(restored.to_scan_tasks(_empty_pushdowns()))
    assert len(tasks) > 0


def test_operator_pickle_roundtrip_after_planning_carries_cache(picklable_cow_history_table, monkeypatch):
    call_count = 0
    real = _changelog_schema._read_footer_arrow_schema

    def counting(path, storage_config):
        nonlocal call_count
        call_count += 1
        return real(path, storage_config)

    monkeypatch.setattr(_changelog_schema, "_read_footer_arrow_schema", counting)

    operator = _make_operator(picklable_cow_history_table)
    first_tasks = list(operator.to_scan_tasks(_empty_pushdowns()))
    calls_before_pickle = call_count

    restored: IcebergChangesScanOperator = pickle.loads(pickle.dumps(operator))
    assert restored._cached_tasks is not None  # cache carried across the pickle boundary
    second_tasks = list(restored.to_scan_tasks(_empty_pushdowns()))

    assert len(first_tasks) == len(second_tasks)
    # No new footer reads should have happened: the receiver reused the sender's
    # already-validated planning result instead of redoing it.
    assert call_count == calls_before_pickle


def _empty_pushdowns():
    from daft.daft import PyPushdowns

    return PyPushdowns()
