from __future__ import annotations

import threading
import weakref
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, Union
from uuid import uuid4

from daft.daft import PyMicroPartitionSet
from daft.datatype import TimeUnit
from daft.recordbatch import MicroPartition

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from ray import ObjectRef

    from daft.expressions.expressions import Expression
    from daft.logical.schema import Schema

PartID = int


@dataclass(frozen=True)
class TableReadOptions:
    """Options for reading a vPartition.

    Args:
        num_rows: Number of rows to read, or None to read all rows
        column_names: Column names to include when reading, or None to read all columns
    """

    num_rows: int | None = None
    column_names: list[str] | None = None


@dataclass(frozen=True)
class TableParseCSVOptions:
    """Options for parsing CSVs.

    Args:
        delimiter: The delimiter to use when parsing CSVs, defaults to ","
        header_index: Index of the header row, or None if no header
        double_quote: Whether to support escaping quotes by doubling them, defaults to True
        buffer_size: Size of the buffer (in bytes) used by the streaming reader.
        chunk_size: Size of the chunks (in bytes) deserialized in parallel by the streaming reader.
        allow_variable_columns: Whether to allow for variable number of columns in the CSV, defaults to False.
    """

    delimiter: str | None = None
    header_index: int | None = 0
    double_quote: bool = True
    quote: str | None = None
    allow_variable_columns: bool = False
    escape_char: str | None = None
    comment: str | None = None
    buffer_size: int | None = None
    chunk_size: int | None = None


@dataclass(frozen=True)
class TableParseParquetOptions:
    """Options for parsing Parquet files.

    Args:
        coerce_int96_timestamp_unit: TimeUnit to use when parsing Int96 fields
    """

    coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns()


@dataclass(frozen=True)
class PartialPartitionMetadata:
    num_rows: None | int
    size_bytes: None | int
    boundaries: None | Boundaries = None


@dataclass(frozen=True)
class PartitionMetadata(PartialPartitionMetadata):
    num_rows: int
    size_bytes: int | None
    boundaries: Boundaries | None = None

    @classmethod
    def from_table(cls, table: MicroPartition) -> PartitionMetadata:
        return PartitionMetadata(
            num_rows=len(table),
            size_bytes=table.size_bytes(),
            boundaries=None,
        )

    def merge_with_partial(self, partial_metadata: PartialPartitionMetadata) -> PartitionMetadata:
        num_rows = self.num_rows
        size_bytes = self.size_bytes
        boundaries = self.boundaries
        if boundaries is None:
            boundaries = partial_metadata.boundaries
        return PartitionMetadata(num_rows, size_bytes, boundaries)

    def downcast_to_partial(self) -> PartialPartitionMetadata:
        return PartialPartitionMetadata(self.num_rows, self.size_bytes, self.boundaries)


def _is_bound_null(bound_row: list[Any | None]) -> bool:
    return all(bound is None for bound in bound_row)


# TODO(Clark): Port this to the Rust side.
@dataclass(frozen=True)
class Boundaries:
    sort_by: list[Expression]
    bounds: MicroPartition

    def __post_init__(self) -> None:
        assert len(self.sort_by) > 0
        assert len(self.bounds) == 2
        assert self.bounds.column_names() == [e.name() for e in self.sort_by]

    def intersects(self, other: Boundaries) -> bool:
        if self.is_trivial_bounds() or other.is_trivial_bounds():
            return True
        self_bounds = self.bounds.to_pylist()
        other_bounds = other.bounds.to_pylist()
        self_lower = list(self_bounds[0].values())
        self_upper = list(self_bounds[1].values())
        other_lower = list(other_bounds[0].values())
        other_upper = list(other_bounds[1].values())
        if _is_bound_null(self_lower):
            return _is_bound_null(other_lower) or other_lower <= self_upper
        if _is_bound_null(other_lower):
            return self_lower <= other_upper
        if _is_bound_null(self_upper):
            return _is_bound_null(other_upper) or other_upper >= self_lower
        if _is_bound_null(other_upper):
            return self_upper >= other_lower
        return (self_lower <= other_lower and self_upper >= other_lower) or (
            self_lower > other_lower and other_upper >= self_lower
        )

    def is_disjointly_bounded_above_by(self, other: Boundaries) -> bool:
        # Check that upper of self is less than lower of other.
        self_upper = list(self.bounds.to_pylist()[1].values())
        if _is_bound_null(self_upper):
            return False
        other_lower = list(other.bounds.to_pylist()[0].values())
        if _is_bound_null(other_lower):
            return False
        return self_upper < other_lower

    def is_trivial_bounds(self) -> bool:
        bounds = self.bounds.to_pylist()
        lower = list(bounds[0].values())
        upper = list(bounds[1].values())
        return _is_bound_null(lower) and _is_bound_null(upper)

    def is_strictly_bounded_above_by(self, other: Boundaries) -> bool:
        # Check that upper of self is less than upper of other.
        self_upper = list(self.bounds.to_pylist()[1].values())
        if _is_bound_null(self_upper):
            return False
        other_upper = list(other.bounds.to_pylist()[1].values())
        if _is_bound_null(other_upper):
            return True
        return self_upper < other_upper


PartitionT = TypeVar("PartitionT", bound="Union[ObjectRef, MicroPartition]")


class MaterializedResult(Generic[PartitionT]):
    """A protocol for accessing the result partition of a PartitionTask.

    Different Runners can fill in their own implementation here.
    """

    @abstractmethod
    def partition(self) -> PartitionT:
        """Get the result data as a generic PartitionT, which is an internal backend-specific representation of the result."""
        ...

    @abstractmethod
    def micropartition(self) -> MicroPartition:
        """Get the result data as an in-memory MicroPartition."""
        ...

    @abstractmethod
    def metadata(self) -> PartitionMetadata:
        """Get the metadata of the partition in this result."""
        ...

    @abstractmethod
    def cancel(self) -> None:
        """If possible, cancel execution of this PartitionTask."""
        ...

    @abstractmethod
    def _noop(self, _: PartitionT) -> None:
        """Implement this as a no-op.

        https://peps.python.org/pep-0544/#overriding-inferred-variance-of-protocol-classes.
        """
        ...


class PartitionSet(Generic[PartitionT]):
    def _get_merged_micropartition(self, schema: Schema) -> MicroPartition:
        raise NotImplementedError()

    def _get_preview_micropartitions(self, num_rows: int) -> list[MicroPartition]:
        raise NotImplementedError()

    def to_pydict(self, schema: Schema) -> dict[str, list[Any]]:
        """Retrieves all the data in a PartitionSet as a Python dictionary. Values are the raw data from each Block."""
        merged_partition = self._get_merged_micropartition(schema)
        return merged_partition.to_pydict()

    def to_pandas(
        self,
        schema: Schema,
        coerce_temporal_nanoseconds: bool = False,
    ) -> pd.DataFrame:
        merged_partition = self._get_merged_micropartition(schema)
        return merged_partition.to_pandas(
            schema=schema,
            coerce_temporal_nanoseconds=coerce_temporal_nanoseconds,
        )

    def to_arrow(self, schema: Schema) -> pa.Table:
        merged_partition = self._get_merged_micropartition(schema)
        return merged_partition.to_arrow()

    def items(self) -> list[tuple[PartID, MaterializedResult[PartitionT]]]:
        """Returns all (partition id, partition) in this PartitionSet ordered by partition ID."""
        raise NotImplementedError()

    def values(self) -> list[MaterializedResult[PartitionT]]:
        return [value for _, value in self.items()]

    @abstractmethod
    def get_partition(self, idx: PartID) -> MaterializedResult[PartitionT]:
        raise NotImplementedError()

    @abstractmethod
    def set_partition(self, idx: PartID, part: MaterializedResult[PartitionT]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete_partition(self, idx: PartID) -> None:
        raise NotImplementedError()

    @abstractmethod
    def has_partition(self, idx: PartID) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def size_bytes(self) -> int | None:
        raise NotImplementedError()

    @abstractmethod
    def num_partitions(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def wait(self) -> None:
        raise NotImplementedError()


class LocalPartitionSet(PartitionSet[MicroPartition]):
    _pset: PyMicroPartitionSet

    def __init__(self) -> None:
        super().__init__()
        self._pset = PyMicroPartitionSet()

    @classmethod
    def _from_micropartition_set(cls, pset: PyMicroPartitionSet) -> LocalPartitionSet:
        s = cls()
        s._pset = pset
        return s

    def _get_merged_micropartition(self, _schema: Schema) -> MicroPartition:
        return MicroPartition._from_pymicropartition(self._pset.get_merged_micropartition())

    def _get_preview_micropartitions(self, num_rows: int) -> list[MicroPartition]:
        return [
            MicroPartition._from_pymicropartition(part) for part in self._pset.get_preview_micropartitions(num_rows)
        ]

    def items(self) -> list[tuple[PartID, MaterializedResult[MicroPartition]]]:
        return [
            (
                idx,
                LocalMaterializedResult(
                    MicroPartition._from_pymicropartition(part),
                    PartitionMetadata.from_table(MicroPartition._from_pymicropartition(part)),
                ),
            )
            for idx, part in self._pset.items()
        ]

    def get_partition(self, idx: PartID) -> MaterializedResult[MicroPartition]:
        part = MicroPartition._from_pymicropartition(self._pset.get_partition(idx))
        return LocalMaterializedResult(part, PartitionMetadata.from_table(part))

    def set_partition(self, idx: PartID, part: MaterializedResult[MicroPartition]) -> None:
        self._pset.set_partition(idx, part.partition()._micropartition)

    def set_partition_from_table(self, idx: PartID, part: MicroPartition) -> None:
        self._pset.set_partition(idx, part._micropartition)

    def delete_partition(self, idx: PartID) -> None:
        self._pset.delete_partition(idx)

    def has_partition(self, idx: PartID) -> bool:
        return self._pset.has_partition(idx)

    def __len__(self) -> int:
        return len(self._pset)

    def size_bytes(self) -> int | None:
        return self._pset.size_bytes()

    def num_partitions(self) -> int:
        return self._pset.num_partitions()

    def wait(self) -> None:
        pass


@dataclass
class LocalMaterializedResult(MaterializedResult[MicroPartition]):
    _partition: MicroPartition
    _metadata: PartitionMetadata | None = None

    def partition(self) -> MicroPartition:
        return self._partition

    def micropartition(self) -> MicroPartition:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        if self._metadata is None:
            self._metadata = PartitionMetadata.from_table(self._partition)
        return self._metadata

    def cancel(self) -> None:
        return None

    def _noop(self, _: MicroPartition) -> None:
        return None


@dataclass(eq=False, repr=False)
class PartitionCacheEntry:
    key: str
    value: PartitionSet[Any] | None

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PartitionCacheEntry) and self.key == other.key

    def __hash__(self) -> int:
        return hash(self.key)

    def __repr__(self) -> str:
        return f"PartitionCacheEntry: {self.key}"

    def __getstate__(self) -> str:
        return self.key

    def __setstate__(self, key: str) -> None:
        self.key = key
        self.value = None

    def num_partitions(self) -> int | None:
        return self.value.num_partitions() if self.value is not None else None

    def size_bytes(self) -> int | None:
        return self.value.size_bytes() if self.value is not None else None

    def num_rows(self) -> int | None:
        return len(self.value) if self.value is not None else None


class PartitionSetCache:
    def __init__(self) -> None:
        self.__uuid_to_partition_set: weakref.WeakValueDictionary[str, PartitionCacheEntry] = (
            weakref.WeakValueDictionary()
        )
        self._lock = threading.Lock()

    def get_partition_set(self, pset_id: str) -> PartitionCacheEntry:
        with self._lock:
            assert pset_id in self.__uuid_to_partition_set
            return self.__uuid_to_partition_set[pset_id]

    def get_all_partition_sets(self) -> dict[str, PartitionSet[Any]]:
        with self._lock:
            return {key: entry.value for key, entry in self.__uuid_to_partition_set.items() if entry.value is not None}

    def put_partition_set(self, pset: PartitionSet[Any]) -> PartitionCacheEntry:
        pset_id = uuid4().hex
        part_entry = PartitionCacheEntry(pset_id, pset)
        with self._lock:
            self.__uuid_to_partition_set[pset_id] = part_entry
            return part_entry

    def rm(self, pset_id: str) -> None:
        with self._lock:
            if pset_id in self.__uuid_to_partition_set:
                del self.__uuid_to_partition_set[pset_id]

    def clear(self) -> None:
        with self._lock:
            del self.__uuid_to_partition_set
            self.__uuid_to_partition_set = weakref.WeakValueDictionary()
