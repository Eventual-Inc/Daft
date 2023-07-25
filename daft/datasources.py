from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.io import IOConfig

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

    use_native_downloader: bool
    io_config: IOConfig | None

    def scan_type(self):
        return StorageType.PARQUET
