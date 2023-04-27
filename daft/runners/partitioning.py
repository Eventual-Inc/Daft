from __future__ import annotations

import weakref
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from uuid import uuid4

import pyarrow as pa

from daft.logical.schema import Schema
from daft.table import Table

if TYPE_CHECKING:
    import pandas as pd

PartID = int


@dataclass(frozen=True)
class vPartitionReadOptions:
    """Options for reading a vPartition

    Args:
        num_rows: Number of rows to read, or None to read all rows
        column_names: Column names to include when reading, or None to read all columns
    """

    num_rows: int | None = None
    column_names: list[str] | None = None


@dataclass(frozen=True)
class vPartitionSchemaInferenceOptions:
    """Options for schema inference when reading a vPartition

    Args:
        schema: A schema to use when reading the vPartition. If provided, all schema inference should be skipped.
        inference_column_names: Column names to use when performing schema inference
    """

    schema: Schema | None = None
    inference_column_names: list[str] | None = None

    def full_schema_column_names(self) -> list[str] | None:
        """Returns all column names for the schema, or None if not provided."""
        if self.schema is not None:
            return self.schema.column_names()
        return self.inference_column_names


@dataclass(frozen=True)
class vPartitionParseCSVOptions:
    """Options for parsing CSVs

    Args:
        delimiter: The delimiter to use when parsing CSVs, defaults to ","
        has_headers: Whether the CSV has headers, defaults to True
        column_names: Column names to use in place of headers, defaults to None
        skip_rows_before_header: Number of rows to skip before the header, defaults to 0
        skip_rows_after_header: Number of rows to skip after the header, defaults to 0
    """

    delimiter: str = ","
    has_headers: bool = True
    skip_rows_before_header: int = 0
    skip_rows_after_header: int = 0


@dataclass(frozen=True)
class PartialPartitionMetadata:
    num_rows: None | int
    size_bytes: None | int


@dataclass(frozen=True)
class PartitionMetadata(PartialPartitionMetadata):
    num_rows: int
    size_bytes: int

    @classmethod
    def from_table(cls, table: Table) -> PartitionMetadata:
        return PartitionMetadata(
            num_rows=len(table),
            size_bytes=table.size_bytes(),
        )


PartitionT = TypeVar("PartitionT")


class PartitionSet(Generic[PartitionT]):
    def _get_merged_vpartition(self) -> Table:
        raise NotImplementedError()

    def to_pydict(self) -> dict[str, list[Any]]:
        """Retrieves all the data in a PartitionSet as a Python dictionary. Values are the raw data from each Block."""
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_pydict()

    def to_pandas(self, schema: Schema | None = None) -> pd.DataFrame:
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_pandas(schema=schema)

    def to_arrow(self) -> pa.Table:
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_arrow()

    def items(self) -> list[tuple[PartID, PartitionT]]:
        """
        Returns all (partition id, partition) in this PartitionSet,
        ordered by partition ID.
        """
        raise NotImplementedError()

    def values(self) -> list[PartitionT]:
        return [value for _, value in self.items()]

    @abstractmethod
    def get_partition(self, idx: PartID) -> PartitionT:
        raise NotImplementedError()

    @abstractmethod
    def set_partition(self, idx: PartID, part: PartitionT) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete_partition(self, idx: PartID) -> None:
        raise NotImplementedError()

    @abstractmethod
    def has_partition(self, idx: PartID) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    @abstractmethod
    def len_of_partitions(self) -> list[int]:
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

    def __repr__(self) -> str:
        return f"PartitionCacheEntry: {self.key}"

    def __getstate__(self):
        return self.key

    def __setstate__(self, key):
        self.key = key
        self.value = None


class PartitionSetCache:
    def __init__(self) -> None:
        self._uuid_to_partition_set: weakref.WeakValueDictionary[
            str, PartitionCacheEntry
        ] = weakref.WeakValueDictionary()

    def get_partition_set(self, pset_id: str) -> PartitionCacheEntry:
        assert pset_id in self._uuid_to_partition_set
        return self._uuid_to_partition_set[pset_id]

    def put_partition_set(self, pset: PartitionSet) -> PartitionCacheEntry:
        pset_id = uuid4().hex
        part_entry = PartitionCacheEntry(pset_id, pset)
        self._uuid_to_partition_set[pset_id] = part_entry
        return part_entry

    def rm(self, pset_id: str) -> None:
        if pset_id in self._uuid_to_partition_set:
            del self._uuid_to_partition_set[pset_id]

    def clear(self) -> None:
        del self._uuid_to_partition_set
        self._uuid_to_partition_set = weakref.WeakValueDictionary()
