from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Generic, Self, TypeVar, cast

from daft.dependencies import pa, torch

T = TypeVar("T")
_UNSET = object()


class ColumnRepresentation(StrEnum):
    DENSE_TENSOR = "dense_tensor"
    DENSE_TENSOR_WITH_VALIDITY = "dense_tensor_with_validity"
    DICTIONARY_CODES = "dictionary_codes"
    ARROW_ONLY = "arrow_only"


@dataclass(frozen=True)
class ModelColumn:
    """One borrowed Arrow column and its trusted read-only model view.

    PyTorch cannot enforce read-only storage for ``frombuffer`` tensors.
    Model adapters must not use in-place operations on this borrowed view.
    Callers that require mutable input must allocate and report a private copy.
    """

    representation: ColumnRepresentation
    arrow: pa.Array
    tensor: object | None = None
    validity: pa.Buffer | None = None
    dictionary: pa.Array | None = None
    reason: str | None = None
    borrowed_read_only: bool = True


_TORCH_DTYPES: dict[pa.DataType, str] = {
    pa.int8(): "int8",
    pa.int16(): "int16",
    pa.int32(): "int32",
    pa.int64(): "int64",
    pa.uint8(): "uint8",
    pa.float16(): "float16",
    pa.float32(): "float32",
    pa.float64(): "float64",
}


def _one_array(column: pa.Array | pa.ChunkedArray) -> pa.Array:
    if isinstance(column, pa.Array):
        return column
    if column.num_chunks != 1:
        raise ValueError("Model-column conversion requires one Arrow chunk; rechunking would copy data")
    return column.chunk(0)


def _primitive_tensor(array: pa.Array) -> object:
    dtype_name = _TORCH_DTYPES[array.type]
    dtype = getattr(torch, dtype_name)
    value_buffer = array.buffers()[1]
    if value_buffer is None:
        return torch.empty(0, dtype=dtype)
    byte_width = array.type.bit_width // 8
    byte_offset = array.offset * byte_width
    return torch.frombuffer(
        memoryview(value_buffer)[byte_offset:],
        dtype=dtype,
        count=len(array),
    )


def model_column(column: pa.Array | pa.ChunkedArray) -> ModelColumn:
    """Expose compatible Arrow values as tensor views without broad conversion.

    Primitive numeric values become dense tensor views. Their optional Arrow
    validity bitmap stays separate because it is bit-packed. Dictionary
    indices become a tensor view while dictionary values remain Arrow. Strings,
    lists, Boolean values, and other layouts remain Arrow until an explicit
    model adapter encodes them.
    """
    array = _one_array(column)
    validity = array.buffers()[0]

    if array.type in _TORCH_DTYPES:
        representation = (
            ColumnRepresentation.DENSE_TENSOR_WITH_VALIDITY if array.null_count else ColumnRepresentation.DENSE_TENSOR
        )
        return ModelColumn(
            representation=representation,
            arrow=array,
            tensor=_primitive_tensor(array),
            validity=validity,
        )

    if pa.types.is_dictionary(array.type):
        indices = array.indices
        if indices.type not in _TORCH_DTYPES:
            return ModelColumn(
                representation=ColumnRepresentation.ARROW_ONLY,
                arrow=array,
                validity=validity,
                reason=f"unsupported dictionary index type: {indices.type}",
            )
        return ModelColumn(
            representation=ColumnRepresentation.DICTIONARY_CODES,
            arrow=array,
            tensor=_primitive_tensor(indices),
            validity=validity,
            dictionary=array.dictionary,
        )

    return ModelColumn(
        representation=ColumnRepresentation.ARROW_ONLY,
        arrow=array,
        validity=validity,
        reason=f"{array.type} requires explicit model encoding",
    )


@dataclass(frozen=True)
class LeaseSnapshot:
    live_count: int
    live_bytes: int
    peak_count: int
    peak_bytes: int
    acquired: int
    released: int
    wait_count: int
    wait_seconds: float


class BatchLease(Generic[T]):
    """Explicit ownership of one model-facing batch."""

    def __init__(
        self,
        ledger: LeaseLedger,
        lease_id: int,
        value: T | object,
        byte_count: int,
        device: str,
        release_callback: Callable[[], None] | None,
    ) -> None:
        self._ledger = ledger
        self._lease_id = lease_id
        self._value = value
        self._byte_count = byte_count
        self._device = device
        self._release_callback = release_callback
        self._released = False

    @property
    def value(self) -> T:
        if self._released:
            raise RuntimeError("Batch lease has been released")
        if self._value is _UNSET:
            raise RuntimeError("Batch lease is reserved but has no result yet")
        return cast("T", self._value)

    @property
    def byte_count(self) -> int:
        return self._byte_count

    @property
    def device(self) -> str:
        return self._device

    @property
    def released(self) -> bool:
        return self._released

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._ledger._release(self._lease_id, self._byte_count, self._release_callback)

    def _fill(
        self,
        value: T,
        *,
        byte_count: int,
        device: str,
        release_callback: Callable[[], None] | None = None,
    ) -> None:
        self._ledger._fill(self, value, byte_count, device, release_callback)

    def __enter__(self) -> BatchLease[T]:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()


class LeaseLedger:
    """Counts live batch ownership and calls each release callback once."""

    def __init__(self, max_live_count: int | None = None) -> None:
        if max_live_count is not None and max_live_count <= 0:
            raise ValueError("max_live_count must be positive")
        self._condition = threading.Condition()
        self._max_live_count = max_live_count
        self._next_id = 0
        self._live_ids: set[int] = set()
        self._live_leases: dict[int, BatchLease[object]] = {}
        self._live_bytes = 0
        self._peak_count = 0
        self._peak_bytes = 0
        self._acquired = 0
        self._released = 0
        self._wait_count = 0
        self._wait_seconds = 0.0
        self._cancelled = False

    def acquire(
        self,
        value: T,
        *,
        byte_count: int,
        device: str,
        release_callback: Callable[[], None] | None = None,
        timeout: float | None = None,
    ) -> BatchLease[T]:
        if byte_count < 0:
            raise ValueError("byte_count must be non-negative")
        return self._acquire(value, byte_count, device, release_callback, timeout)

    def reserve(self, *, timeout: float | None = None) -> BatchLease[T]:
        """Reserve one live-batch slot before producer execution begins."""
        return self._acquire(_UNSET, 0, "pending", None, timeout)

    def _acquire(
        self,
        value: T | object,
        byte_count: int,
        device: str,
        release_callback: Callable[[], None] | None,
        timeout: float | None,
    ) -> BatchLease[T]:
        with self._condition:
            started_waiting: float | None = None
            while self._max_live_count is not None and len(self._live_ids) >= self._max_live_count:
                if self._cancelled:
                    raise RuntimeError("Batch lease acquisition was cancelled")
                if started_waiting is None:
                    started_waiting = time.perf_counter()
                    self._wait_count += 1
                remaining = None
                if timeout is not None:
                    remaining = timeout - (time.perf_counter() - started_waiting)
                    if remaining <= 0:
                        self._wait_seconds += time.perf_counter() - started_waiting
                        raise TimeoutError("Timed out waiting for batch lease credit")
                self._condition.wait(remaining)
            if started_waiting is not None:
                self._wait_seconds += time.perf_counter() - started_waiting
            if self._cancelled:
                raise RuntimeError("Batch lease acquisition was cancelled")
            lease_id = self._next_id
            self._next_id += 1
            self._live_ids.add(lease_id)
            self._live_bytes += byte_count
            self._acquired += 1
            self._peak_count = max(self._peak_count, len(self._live_ids))
            self._peak_bytes = max(self._peak_bytes, self._live_bytes)
            lease = BatchLease(self, lease_id, value, byte_count, device, release_callback)
            self._live_leases[lease_id] = lease
        return lease

    def snapshot(self) -> LeaseSnapshot:
        with self._condition:
            return LeaseSnapshot(
                live_count=len(self._live_ids),
                live_bytes=self._live_bytes,
                peak_count=self._peak_count,
                peak_bytes=self._peak_bytes,
                acquired=self._acquired,
                released=self._released,
                wait_count=self._wait_count,
                wait_seconds=self._wait_seconds,
            )

    def cancel_waiters(self) -> None:
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()

    def release_all(self) -> None:
        with self._condition:
            leases = list(self._live_leases.values())
        for lease in leases:
            lease.release()

    def assert_clear(self) -> None:
        snapshot = self.snapshot()
        if snapshot.live_count or snapshot.live_bytes:
            raise RuntimeError(f"{snapshot.live_count} batch leases still own {snapshot.live_bytes} bytes")

    def _release(
        self,
        lease_id: int,
        byte_count: int,
        release_callback: Callable[[], None] | None,
    ) -> None:
        with self._condition:
            if lease_id not in self._live_ids:
                return
            self._live_ids.remove(lease_id)
            self._live_leases.pop(lease_id, None)
            self._live_bytes -= byte_count
            self._released += 1
            self._condition.notify()
        if release_callback is not None:
            release_callback()

    def _fill(
        self,
        lease: BatchLease[T],
        value: T,
        byte_count: int,
        device: str,
        release_callback: Callable[[], None] | None,
    ) -> None:
        if byte_count < 0:
            raise ValueError("byte_count must be non-negative")
        with self._condition:
            if lease._lease_id not in self._live_ids or lease._released:
                raise RuntimeError("Reserved batch lease was released before the result arrived")
            if lease._value is not _UNSET:
                raise RuntimeError("Reserved batch lease already has a result")
            lease._value = value
            lease._byte_count = byte_count
            lease._device = device
            lease._release_callback = release_callback
            self._live_bytes += byte_count
            self._peak_bytes = max(self._peak_bytes, self._live_bytes)


class ContextChangeKind(StrEnum):
    ADD = "ADD"
    REMOVE = "REMOVE"
    UPDATE = "UPDATE"
    REORDER = "REORDER"


@dataclass(frozen=True)
class ContextChange:
    kind: ContextChangeKind
    seed_key: Hashable
    row_key: Hashable
    old_rank: int | None
    new_rank: int | None
    old_version: Hashable | None
    new_version: Hashable | None


@dataclass(frozen=True)
class _ContextMember:
    version: Hashable
    rank: int


class ContextStateTracker:
    """Turns complete context selections into row-keyed change records."""

    def __init__(self) -> None:
        self._members: dict[tuple[Hashable, Hashable], _ContextMember] = {}

    def update(
        self,
        table: pa.Table,
        *,
        seed_column: str,
        key_column: str,
        version_column: str,
        replace_seeds: Sequence[Hashable] | None = None,
    ) -> list[ContextChange]:
        seeds = table[seed_column].to_pylist()
        keys = table[key_column].to_pylist()
        versions = table[version_column].to_pylist()
        current: dict[tuple[Hashable, Hashable], _ContextMember] = {}
        ranks: dict[Hashable, int] = {}
        for seed, key, version in zip(seeds, keys, versions, strict=True):
            rank = ranks.get(seed, 0)
            ranks[seed] = rank + 1
            identity = (seed, key)
            if identity in current:
                raise ValueError(f"Duplicate context row key for seed: {identity!r}")
            current[identity] = _ContextMember(version=version, rank=rank)

        if replace_seeds is None:
            previous = self._members
            next_members = current
        else:
            scope = set(replace_seeds)
            unexpected = {seed for seed, _ in current} - scope
            if unexpected:
                raise ValueError(f"Context update contains seeds outside its replacement scope: {unexpected!r}")
            previous = {identity: member for identity, member in self._members.items() if identity[0] in scope}
            next_members = {identity: member for identity, member in self._members.items() if identity[0] not in scope}
            next_members.update(current)

        changes: list[ContextChange] = []
        for (seed, key), previous_member in previous.items():
            if (seed, key) not in current:
                changes.append(
                    ContextChange(
                        ContextChangeKind.REMOVE,
                        seed,
                        key,
                        previous_member.rank,
                        None,
                        previous_member.version,
                        None,
                    )
                )

        for (seed, key), member in current.items():
            previous_member = previous.get((seed, key))
            if previous_member is None:
                changes.append(
                    ContextChange(
                        ContextChangeKind.ADD,
                        seed,
                        key,
                        None,
                        member.rank,
                        None,
                        member.version,
                    )
                )
                continue
            if previous_member.version != member.version:
                changes.append(
                    ContextChange(
                        ContextChangeKind.UPDATE,
                        seed,
                        key,
                        previous_member.rank,
                        member.rank,
                        previous_member.version,
                        member.version,
                    )
                )
            if previous_member.rank != member.rank:
                changes.append(
                    ContextChange(
                        ContextChangeKind.REORDER,
                        seed,
                        key,
                        previous_member.rank,
                        member.rank,
                        previous_member.version,
                        member.version,
                    )
                )

        self._members = next_members
        return changes


@dataclass(frozen=True)
class RowTransformKey:
    table: str
    row_key: Hashable
    row_version: Hashable
    columns: tuple[str, ...]
    transform_version: str
    dtype: str
    layout: str
    device: str


@dataclass(frozen=True)
class CacheSnapshot:
    hits: int
    misses: int
    invalidated: int
    reused_bytes: int
    entries: int


class RowTransformCache(Generic[T]):
    """Row/version-scoped model transform reuse with targeted invalidation."""

    def __init__(self) -> None:
        self._entries: dict[RowTransformKey, tuple[T, int]] = {}
        self._hits = 0
        self._misses = 0
        self._invalidated = 0
        self._reused_bytes = 0

    def get_or_compute(
        self,
        key: RowTransformKey,
        compute: Callable[[], tuple[T, int]],
    ) -> T:
        cached = self._entries.get(key)
        if cached is not None:
            value, byte_count = cached
            self._hits += 1
            self._reused_bytes += byte_count
            return value
        value, byte_count = compute()
        if byte_count < 0:
            raise ValueError("cache byte_count must be non-negative")
        self._entries[key] = (value, byte_count)
        self._misses += 1
        return value

    def invalidate_rows(self, table: str, row_keys: Sequence[Hashable]) -> int:
        targets = set(row_keys)
        matching = [key for key in self._entries if key.table == table and key.row_key in targets]
        for key in matching:
            del self._entries[key]
        self._invalidated += len(matching)
        return len(matching)

    def snapshot(self) -> CacheSnapshot:
        return CacheSnapshot(
            hits=self._hits,
            misses=self._misses,
            invalidated=self._invalidated,
            reused_bytes=self._reused_bytes,
            entries=len(self._entries),
        )


@dataclass(frozen=True)
class SourceSnapshot:
    connector: str
    source: str
    version: str
    method: str

    @classmethod
    def transactional(cls, connector: str, source: str, version: str) -> SourceSnapshot:
        if not version:
            raise ValueError("transactional source version must not be empty")
        return cls(connector, source, version, "connector-version")

    @classmethod
    def observed_facts(
        cls,
        connector: str,
        source: str,
        facts: Mapping[str, str | int],
    ) -> SourceSnapshot:
        if not facts:
            raise ValueError("non-transactional snapshot requires observed source facts")
        payload = json.dumps(
            {"connector": connector, "source": source, "facts": dict(facts)},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return cls(connector, source, hashlib.sha256(payload).hexdigest(), "observed-facts")


@dataclass(frozen=True)
class ContextProtocolConfig:
    seed_column: str
    key_column: str
    version_column: str
    source_snapshot: SourceSnapshot
    max_live_batches: int = 3


@dataclass(frozen=True)
class RowTransformRequest:
    table: str
    columns: tuple[str, ...]
    transform_version: str
    dtype: str
    layout: str
    device: str
    transform: Callable[[Hashable, Hashable], tuple[object, int]]


@dataclass(frozen=True)
class ContextBatch:
    session_id: str
    plan_fingerprint: int
    input_id: int
    revision: int
    query_plan: str
    rows: BatchLease[pa.Table]
    row_transforms: tuple[object, ...]
    changes: tuple[ContextChange, ...]
    source_snapshot: SourceSnapshot
    lease_snapshot: LeaseSnapshot
    cache_snapshot: CacheSnapshot

    @property
    def table(self) -> pa.Table:
        return self.rows.value

    def release(self) -> None:
        self.rows.release()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()


class ContextProtocol:
    """Session-owned context rows, changes, leases, and row-transform reuse."""

    def __init__(self, session_id: str, config: ContextProtocolConfig) -> None:
        self.session_id = session_id
        self.config = config
        self._source_snapshot = config.source_snapshot
        self._revision = 0
        self._tracker = ContextStateTracker()
        self._leases = LeaseLedger(max_live_count=config.max_live_batches)
        self._row_cache: RowTransformCache[object] = RowTransformCache()

    @property
    def source_snapshot(self) -> SourceSnapshot:
        return self._source_snapshot

    @property
    def lease_snapshot(self) -> LeaseSnapshot:
        return self._leases.snapshot()

    @property
    def cache_snapshot(self) -> CacheSnapshot:
        return self._row_cache.snapshot()

    def lease_feedback(
        self,
        table: pa.Table,
        *,
        timeout: float | None = None,
    ) -> BatchLease[pa.Table]:
        """Keep row-keyed feedback owned by this session until plan submission."""
        return self._leases.acquire(
            table,
            byte_count=table.nbytes,
            device="cpu",
            timeout=timeout,
        )

    def reserve_result(self, *, timeout: float | None = None) -> BatchLease[pa.Table]:
        """Reserve flow credit before a context round starts producing rows."""
        return self._leases.reserve(timeout=timeout)

    def accept(
        self,
        table: pa.Table,
        *,
        plan_fingerprint: int,
        input_id: int,
        query_plan: str,
        replace_seeds: Sequence[Hashable] | None = None,
        row_transform: RowTransformRequest | None = None,
        lease_timeout: float | None = None,
        reserved_lease: BatchLease[pa.Table] | None = None,
    ) -> ContextBatch:
        if reserved_lease is None:
            lease = self._leases.acquire(
                table,
                byte_count=table.nbytes,
                device="cpu",
                timeout=lease_timeout,
            )
        else:
            lease = reserved_lease
            lease._fill(table, byte_count=table.nbytes, device="cpu")
        try:
            row_transforms = () if row_transform is None else self._cache_rows(table, row_transform)
            changes = self._tracker.update(
                table,
                seed_column=self.config.seed_column,
                key_column=self.config.key_column,
                version_column=self.config.version_column,
                replace_seeds=replace_seeds,
            )
        except BaseException:
            lease.release()
            raise

        self._revision += 1
        return ContextBatch(
            session_id=self.session_id,
            plan_fingerprint=plan_fingerprint,
            input_id=input_id,
            revision=self._revision,
            query_plan=query_plan,
            rows=lease,
            row_transforms=row_transforms,
            changes=tuple(changes),
            source_snapshot=self._source_snapshot,
            lease_snapshot=self._leases.snapshot(),
            cache_snapshot=self._row_cache.snapshot(),
        )

    def update_source(
        self,
        source_snapshot: SourceSnapshot,
        changed_rows: Mapping[str, Sequence[Hashable]],
    ) -> int:
        if (
            source_snapshot.connector != self._source_snapshot.connector
            or source_snapshot.source != self._source_snapshot.source
        ):
            raise ValueError("Updated source identity does not match the context session")
        removed = sum(self._row_cache.invalidate_rows(table, row_keys) for table, row_keys in changed_rows.items())
        self._source_snapshot = source_snapshot
        return removed

    def close(self) -> None:
        self._leases.assert_clear()

    def cancel(self) -> None:
        self._leases.cancel_waiters()
        self._leases.release_all()

    def _cache_rows(self, table: pa.Table, request: RowTransformRequest) -> tuple[object, ...]:
        row_keys = table[self.config.key_column].to_pylist()
        versions = table[self.config.version_column].to_pylist()
        values: list[object] = []
        for row_key, version in zip(row_keys, versions, strict=True):
            cache_key = RowTransformKey(
                table=request.table,
                row_key=row_key,
                row_version=version,
                columns=request.columns,
                transform_version=request.transform_version,
                dtype=request.dtype,
                layout=request.layout,
                device=request.device,
            )
            values.append(
                self._row_cache.get_or_compute(
                    cache_key,
                    lambda row_key=row_key, version=version: request.transform(row_key, version),
                )
            )
        return tuple(values)


def retrieval_recall(selected_keys: Sequence[Hashable], relevant_keys: Sequence[Hashable]) -> float:
    """Measure how much known-relevant context a fixed row budget retrieves."""
    relevant = set(relevant_keys)
    if not relevant:
        raise ValueError("retrieval recall requires at least one relevant row")
    return len(set(selected_keys) & relevant) / len(relevant)
