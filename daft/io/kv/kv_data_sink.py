from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.dependencies import pa
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator


class KVDataSink(DataSink[dict[str, Any]]):
    """Basic DataSink implementation for writing key-value pairs into a KV store.

    This is a minimal, generic sink intended as an integration point for KV-backed
    writes and lifecycle management. It does not depend on any concrete KV backend
    and can use an in-memory dictionary as a default store.

    The sink expects each input MicroPartition to contain an ID column and a value
    column. For each row, the pair (id, value) is written into the underlying
    KV store. Per-micropartition statistics are returned via :class:`WriteResult`,
    and :meth:`finalize` aggregates these into a single summary MicroPartition.

    Finalization is best-effort: if the underlying KV store has already been
    released and reports a "store not found" style error during cleanup, the
    error is treated as a no-op and swallowed. Normal read/write operations
    continue to propagate errors to the caller.
    """

    def __init__(
        self,
        store_handle: Any | None = None,
        *,
        id_column: str = "id",
        value_column: str = "value",
    ) -> None:
        """Create a new KVDataSink.

        Args:
            store_handle: Abstract reference to an underlying KV store. The
                handle is expected to support one of the following write
                interfaces:

                * ``store.put(key, value)``
                * ``store.set(key, value)``
                * ``store[key] = value`` (mapping protocol)

                If not provided, a simple in-memory dictionary is used so that
                importing this module never fails due to missing dependencies.
            id_column: Name of the column in each input MicroPartition that
                contains the key/identifier.
            value_column: Name of the column in each input MicroPartition that
                contains the value to be written for the corresponding key.
        """

        # Underlying KV handle (user-provided or in-memory fallback).
        self._store_handle: Any | None = store_handle if store_handle is not None else {}
        # Strong reference kept for the lifetime of the sink once ``start`` is
        # called, to avoid the KV handle being garbage-collected before
        # ``finalize`` runs.
        self._store_ref: Any | None = None

        self._id_column = id_column
        self._value_column = value_column

        # Result schema for the summary MicroPartition returned from ``finalize``.
        self._result_schema = Schema._from_field_name_and_types(
            [
                ("bytes_written", DataType.int64()),
                ("rows_written", DataType.int64()),
                ("failed_rows", DataType.int64()),
            ]
        )

    def name(self) -> str:
        return "KV Data Sink"

    def schema(self) -> Schema:
        return self._result_schema

    def start(self) -> None:
        """Initialize the sink and hold a strong reference to the store handle.

        This prevents the underlying store from being garbage-collected before
        :meth:`finalize` has a chance to run.
        """

        if self._store_handle is None:
            # Lazily create an in-memory backend if none was provided.
            self._store_handle = {}
        # Strong reference for the duration of this sink instance.
        self._store_ref = self._store_handle

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _get_store(self) -> Any:
        """Return the active store handle, creating a default one if needed."""

        if self._store_ref is not None:
            return self._store_ref
        if self._store_handle is not None:
            return self._store_handle
        # As a last resort, create an in-memory dictionary backend.
        self._store_handle = {}
        self._store_ref = self._store_handle
        return self._store_handle

    def _write_to_store(self, store: Any, key: Any, value: Any) -> None:
        """Write a single (key, value) pair into the underlying store.

        The store is expected to expose either ``put``/``set`` methods or the
        mapping protocol. Other interfaces are not supported in this minimal
        implementation.
        """

        if hasattr(store, "put"):
            store.put(key, value)
        elif hasattr(store, "set"):
            store.set(key, value)
        elif hasattr(store, "__setitem__"):
            store[key] = value
        else:
            raise TypeError(
                "KVDataSink store_handle must support item assignment, `put`, or `set`."
            )

    @staticmethod
    def _estimate_row_bytes(key: Any, value: Any) -> int:
        """Best-effort estimate of the size of a written row in bytes."""

        import sys

        return sys.getsizeof(key) + sys.getsizeof(value)

    @staticmethod
    def _is_store_not_found_error(exc: Exception) -> bool:
        """Heuristically detect a "store not found" style error.

        This is intentionally conservative and string-based so that it works
        across different KV/catalog implementations.
        """

        message = str(exc)
        lower = message.lower()
        if "kv store" in message and "not found" in message:
            return True
        if "store" in lower and "not found" in lower:
            return True
        if getattr(exc, "code", None) == "KV_STORE_NOT_FOUND":
            return True
        return False

    # ---------------------------------------------------------------------
    # DataSink interface
    # ---------------------------------------------------------------------
    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict[str, Any]]]:
        """Write (id, value) pairs from each MicroPartition into the KV store.

        For each input MicroPartition, this method attempts to write all rows
        to the underlying store and yields a :class:`WriteResult` containing
        per-partition statistics. Row-level failures are captured and reported
        in the result, but do not abort the write unless the error indicates
        that the entire store is missing.
        """

        store = self._get_store()

        for micropartition in micropartitions:
            if len(micropartition) == 0:
                continue

            rows_written = 0
            bytes_written = 0
            failed_rows = 0
            error_messages: list[str] = []

            for row in micropartition.to_pylist():
                key = row.get(self._id_column)
                value = row.get(self._value_column)

                try:
                    self._write_to_store(store, key, value)
                    rows_written += 1
                    bytes_written += self._estimate_row_bytes(key, value)
                except Exception as e:  # noqa: BLE001 - we intentionally record all failures
                    # For normal reads/writes, propagate "store not found"-style
                    # errors so that callers can see them.
                    if self._is_store_not_found_error(e):
                        raise

                    failed_rows += 1
                    error_messages.append(str(e))

            result: dict[str, Any] = {
                "bytes_written": bytes_written,
                "rows_written": rows_written,
                "failed_rows": failed_rows,
            }
            if error_messages:
                result["errors"] = error_messages

            yield WriteResult(
                result=result,
                bytes_written=bytes_written,
                rows_written=rows_written,
            )

    def finalize(self, write_results: list[WriteResult[dict[str, Any]]]) -> MicroPartition:
        """Aggregate write statistics into a single summary MicroPartition.

        During finalization, any "KV store not found" style errors from a
        backend-specific finalize/flush method are treated as best-effort
        cleanup and silently ignored. This ensures that cleanup does not fail
        even if the store has already been released elsewhere.
        """

        total_bytes_written = 0
        total_rows_written = 0
        total_failed_rows = 0

        for write_result in write_results:
            total_bytes_written += write_result.bytes_written
            total_rows_written += write_result.rows_written

            result_payload = write_result.result
            if isinstance(result_payload, dict):
                failed = result_payload.get("failed_rows", 0) or 0
                try:
                    total_failed_rows += int(failed)
                except (TypeError, ValueError):
                    # Ignore non-integer values gracefully.
                    pass

        # Best-effort backend finalize/flush with no-op behavior when the
        # underlying store is already gone.
        store = self._store_ref or self._store_handle
        if store is not None:
            finalize_fn = getattr(store, "finalize", None)
            if not callable(finalize_fn):
                finalize_fn = getattr(store, "flush", None)

            if callable(finalize_fn):
                try:
                    finalize_fn()
                except Exception as e:  # noqa: BLE001 - we classify and handle below
                    if not self._is_store_not_found_error(e):
                        # Non-"store not found" errors are still surfaced.
                        raise
                    # Otherwise, treat as best-effort cleanup and continue.

        if not write_results:
            # No writes happened; return an empty result with the expected schema.
            return MicroPartition.empty(self._result_schema)

        summary = MicroPartition.from_pydict(
            {
                "bytes_written": pa.array([total_bytes_written], pa.int64()),
                "rows_written": pa.array([total_rows_written], pa.int64()),
                "failed_rows": pa.array([total_failed_rows], pa.int64()),
            }
        )
        return summary
