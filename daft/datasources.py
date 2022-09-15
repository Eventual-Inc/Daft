import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol


class StorageType(Enum):
    CSV = "CSV"
    PARQUET = "PARQUET"
    IN_MEMORY = "IN_MEMORY"
    JSON = "JSON"


class SourceInfo(Protocol):
    """A class that provides information about a given Datasource"""

    def scan_type(self) -> StorageType:
        ...

    def get_num_partitions(self) -> int:
        ...


@dataclass(frozen=True)
class CSVSourceInfo(SourceInfo):

    filepaths: List[str]
    delimiter: str
    has_headers: bool

    def scan_type(self):
        return StorageType.CSV

    def get_num_partitions(self) -> int:
        return len(self.filepaths)


@dataclass(frozen=True)
class JSONSourceInfo(SourceInfo):

    filepaths: List[str]

    def scan_type(self):
        return StorageType.JSON

    def get_num_partitions(self) -> int:
        return len(self.filepaths)


@dataclass(frozen=True)
class InMemorySourceInfo(SourceInfo):

    data: Dict[str, List[Any]]
    num_partitions: int = 1

    def scan_type(self):
        return StorageType.IN_MEMORY

    def get_num_partitions(self) -> int:
        return self.num_partitions


@dataclass(frozen=True)
class ParquetSourceInfo(SourceInfo):

    filepaths: List[str]

    def scan_type(self):
        return StorageType.PARQUET

    def get_num_partitions(self) -> int:
        return len(self.filepaths)
