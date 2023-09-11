from __future__ import annotations

import sys

from daft.daft import (
    AzureConfig,
    GCSConfig,
    IOConfig,
    S3Config,
    set_io_pool_num_threads,
)
from daft.io._csv import read_csv
from daft.io._json import read_json
from daft.io._parquet import read_parquet
from daft.io.file_path import from_glob_path


def _set_linux_cert_paths():
    import os
    import ssl

    paths = ssl.get_default_verify_paths()
    if paths.cafile:
        os.environ[paths.openssl_cafile_env] = paths.openssl_cafile
    if paths.capath:
        os.environ[paths.openssl_capath_env] = paths.openssl_capath


if sys.platform == "linux":
    _set_linux_cert_paths()

__all__ = [
    "read_csv",
    "read_json",
    "from_glob_path",
    "read_parquet",
    "IOConfig",
    "S3Config",
    "AzureConfig",
    "GCSConfig",
    "set_io_pool_num_threads",
]
