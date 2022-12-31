from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol


class StorageType(Enum):
    CSV = "CSV"
    PARQUET = "PARQUET"
    JSON = "JSON"


class SourceInfo(Protocol):
    """A class that provides information about a given Datasource"""

    def scan_type(self) -> StorageType:
        ...


@dataclass(frozen=True)
class CSVSourceInfo(SourceInfo):

    delimiter: str
    has_headers: bool

    def scan_type(self):
        return StorageType.CSV


@dataclass(frozen=True)
class JSONSourceInfo(SourceInfo):
    def scan_type(self):
        return StorageType.JSON


@dataclass(frozen=True)
class ParquetSourceInfo(SourceInfo):
    def scan_type(self):
        return StorageType.PARQUET
