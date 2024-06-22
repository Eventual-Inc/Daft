from __future__ import annotations

import threading
import weakref
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from uuid import uuid4

import pyarrow as pa

from daft.datatype import TimeUnit
from daft.expressions.expressions import Expression
from daft.logical.schema import Schema
from daft.table import MicroPartition

if TYPE_CHECKING:
    import pandas as pd

PartID = int


@dataclass(frozen=True)
class TableReadOptions:
    """Options for reading a vPartition

    Args:
        num_rows: Number of rows to read, or None to read all rows
        column_names: Column names to include when reading, or None to read all columns
    """

    num_rows: int | None = None
    column_names: list[str] | None = None


@dataclass(frozen=True)
class TableParseCSVOptions:
    """Options for parsing CSVs

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
    """Options for parsing Parquet files

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

    def __post_init__(self):
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


PartitionT = TypeVar("PartitionT")


class MaterializedResult(Generic[PartitionT]):
    """A protocol for accessing the result partition of a PartitionTask.

    Different Runners can fill in their own implementation here.
    """

    @abstractmethod
    def partition(self) -> PartitionT:
        """Get the partition of this result."""
        ...

    @abstractmethod
    def vpartition(self) -> MicroPartition:
        """Get the vPartition of this result."""
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
        https://peps.python.org/pep-0544/#overriding-inferred-variance-of-protocol-classes
        """
        ...


class PartitionSet(Generic[PartitionT]):
    def _get_merged_vpartition(self) -> MicroPartition:
        raise NotImplementedError()

    def _get_preview_vpartition(self, num_rows: int) -> list[MicroPartition]:
        raise NotImplementedError()

    def to_pydict(self) -> dict[str, list[Any]]:
        """Retrieves all the data in a PartitionSet as a Python dictionary. Values are the raw data from each Block."""
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_pydict()

    def to_pandas(
        self,
        schema: Schema | None = None,
        cast_tensors_to_ray_tensor_dtype: bool = False,
        coerce_temporal_nanoseconds: bool = False,
    ) -> pd.DataFrame:
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_pandas(
            schema=schema,
            cast_tensors_to_ray_tensor_dtype=cast_tensors_to_ray_tensor_dtype,
            coerce_temporal_nanoseconds=coerce_temporal_nanoseconds,
        )

    def to_arrow(self, cast_tensors_to_ray_tensor_dtype: bool = False) -> pa.Table:
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_arrow(cast_tensors_to_ray_tensor_dtype)

    def items(self) -> list[tuple[PartID, MaterializedResult[PartitionT]]]:
        """
        Returns all (partition id, partition) in this PartitionSet,
        ordered by partition ID.
        """
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


@dataclass(eq=False, repr=False)
class PartitionCacheEntry:
    key: str
    value: PartitionSet | None

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PartitionCacheEntry) and self.key == other.key

    def __hash__(self) -> int:
        return hash(self.key)

    def __repr__(self) -> str:
        return f"PartitionCacheEntry: {self.key}"

    def __getstate__(self):
        return self.key

    def __setstate__(self, key):
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

    def get_all_partition_sets(self) -> dict[str, PartitionSet]:
        with self._lock:
            return {key: entry.value for key, entry in self.__uuid_to_partition_set.items() if entry.value is not None}

    def put_partition_set(self, pset: PartitionSet) -> PartitionCacheEntry:
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
