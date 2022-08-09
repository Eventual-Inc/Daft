from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Protocol


class ScanType(Enum):
    CSV = "CSV"
    PARQUET = "PARQUET"
    IN_MEMORY = "IN_MEMORY"


class SourceInfo(Protocol):
    """A class that provides information about a given Datasource"""

    def scan_type(self) -> ScanType:
        ...

    def get_num_partitions(self) -> int:
        ...


@dataclass(frozen=True)
class CSVSourceInfo(SourceInfo):

    filepaths: List[str]
    delimiter: str
    has_headers: bool

    def scan_type(self):
        return ScanType.CSV

    def get_num_partitions(self) -> int:
        return len(self.filepaths)


@dataclass(frozen=True)
class InMemorySourceInfo(SourceInfo):

    data: Dict[str, List[Any]]
    num_partitions: int = 1

    def scan_type(self):
        return ScanType.IN_MEMORY

    def get_num_partitions(self) -> int:
        return self.num_partitions
