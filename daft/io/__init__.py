from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import (
    AzureConfig,
    CosConfig,
    GCSConfig,
    GravitinoConfig,
    IOConfig,
    HTTPConfig,
    S3Config,
    S3Credentials,
    TosConfig,
    UnityConfig,
    HuggingFaceConfig,
)
from daft.lazy_import import LazyImport
from daft.io._csv import read_csv
from daft.io._text import read_text
from daft.io.delta_lake._deltalake import read_deltalake
from daft.io.hudi._hudi import read_hudi
from daft.io.iceberg._iceberg import read_iceberg
from daft.io._json import read_json
from daft.io._kafka import read_kafka
from daft.io._parquet import read_parquet
from daft.io._sql import read_sql
from daft.io._warc import read_warc
from daft.io.huggingface import read_huggingface
from daft.io.mcap._mcap import read_mcap
from daft.io._range import _range
from daft.io.file_path import from_glob_path
from daft.io.sink import DataSink
from daft.io.source import DataSource, DataSourceTask
from daft.io.av import read_video_frames

# Lance is lazy-loaded because lance_namespace pulls in ~450ms of pydantic models.
if TYPE_CHECKING:
    from daft.io.lance._lance import read_lance

_lance = LazyImport("daft.io.lance")


def __getattr__(name: str) -> object:
    if name == "read_lance":
        return getattr(_lance, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AzureConfig",
    "CosConfig",
    "DataSink",
    "DataSource",
    "DataSourceTask",
    "GCSConfig",
    "GravitinoConfig",
    "HTTPConfig",
    "HuggingFaceConfig",
    "IOConfig",
    "S3Config",
    "S3Credentials",
    "TosConfig",
    "UnityConfig",
    "_range",
    "from_glob_path",
    "read_csv",
    "read_deltalake",
    "read_hudi",
    "read_huggingface",
    "read_iceberg",
    "read_json",
    "read_kafka",
    "read_lance",
    "read_mcap",
    "read_parquet",
    "read_sql",
    "read_text",
    "read_video_frames",
    "read_warc",
]
