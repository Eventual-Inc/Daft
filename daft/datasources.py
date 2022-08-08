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


@dataclass(frozen=True)
class CSVSourceInfo(SourceInfo):

    filepaths: List[str]
    delimiter: str
    has_headers: bool

    def scan_type(self):
        return ScanType.CSV


@dataclass(frozen=True)
class InMemorySourceInfo(SourceInfo):

    data: Dict[str, List[Any]]

    def scan_type(self):
        return ScanType.IN_MEMORY
