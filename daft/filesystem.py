from __future__ import annotations

import dataclasses
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

from typing import Any

import pyarrow as pa
from fsspec import AbstractFileSystem, get_filesystem_class
from loguru import logger

from daft.datasources import ParquetSourceInfo, SourceInfo


@dataclasses.dataclass(frozen=True)
class ListingInfo:
    path: str
    size: int
    type: Literal["file"] | Literal["directory"]
    rows: int | None = None


def _get_s3fs_kwargs() -> dict[str, Any]:
    """Get keyword arguments to forward to s3fs during construction"""

    try:
        import botocore.session
    except ImportError:
        logger.error(
            "Error when importing botocore. Install getdaft[aws] for the required 3rd party dependencies to interact with AWS S3 (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
        )
        raise

    kwargs = {}

    # If accessing S3 without credentials, use anonymous access: https://github.com/Eventual-Inc/Daft/issues/503
    credentials_available = botocore.session.get_session().get_credentials() is not None
    if not credentials_available:
        logger.warning(
            "AWS credentials not found - using anonymous access to S3 which will fail if the bucket you are accessing is not a public bucket. See boto3 documentation for more details on configuring your AWS credentials: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        )
        kwargs["anon"] = True

    return kwargs


def get_filesystem(protocol: str, **kwargs) -> AbstractFileSystem:

    if protocol == "s3" or protocol == "s3a":
        kwargs = {**kwargs, **_get_s3fs_kwargs()}

    try:
        klass = get_filesystem_class(protocol)
    except ImportError:
        logger.error(
            f"Error when importing dependencies for accessing data with: {protocol}. Please ensure that getdaft was installed with the appropriate extra dependencies (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
        )
        raise

    fs = klass(**kwargs)
    return fs


def get_protocol_from_path(path: str, **kwargs) -> str:
    split = path.split(":")
    assert len(split) <= 2, f"too many colons found in {path}"
    protocol = split[0] if len(split) == 2 else "file"
    return protocol


def get_filesystem_from_path(path: str, **kwargs) -> AbstractFileSystem:
    protocol = get_protocol_from_path(path)
    fs = get_filesystem(protocol, **kwargs)
    return fs


###
# File globbing
###


def _ensure_path_protocol(protocol: str, returned_path: str) -> str:
    """This function adds the protocol that fsspec strips from returned results"""
    if protocol == "file":
        return returned_path
    parsed_scheme = urlparse(returned_path).scheme
    if parsed_scheme == "" or parsed_scheme is None:
        return f"{protocol}://{returned_path}"
    return returned_path


def _path_is_glob(path: str) -> bool:
    # fsspec glob supports *, ? and [..] patterns
    # See: : https://filesystem-spec.readthedocs.io/en/latest/api.html
    return any([char in path for char in ["*", "?", "["]])


def glob_path_with_stats(path: str, source_info: SourceInfo | None) -> list[ListingInfo]:
    """Glob a path, returning a list ListingInfo."""
    fs = get_filesystem_from_path(path)
    protocol = get_protocol_from_path(path)

    filepaths_to_infos: dict[str, dict[str, Any]] = defaultdict(dict)

    if _path_is_glob(path):
        globbed_data = fs.glob(path, detail=True)

        for path, details in globbed_data.items():
            filepaths_to_infos[path]["size"] = details["size"]
            filepaths_to_infos[path]["type"] = details["type"]

    elif fs.isfile(path):
        file_info = fs.info(path)

        filepaths_to_infos[path]["size"] = file_info["size"]
        filepaths_to_infos[path]["type"] = file_info["type"]

    elif fs.isdir(path):
        files_info = fs.ls(path, detail=True)

        for file_info in files_info:
            path = file_info["name"]
            filepaths_to_infos[path]["size"] = file_info["size"]
            filepaths_to_infos[path]["type"] = file_info["type"]

    else:
        raise FileNotFoundError(f"File or directory not found: {path}")

    # Set number of rows if available.
    if isinstance(source_info, ParquetSourceInfo):
        parquet_metadatas = ThreadPoolExecutor().map(_get_parquet_metadata_single, filepaths_to_infos.keys())
        for path, parquet_metadata in zip(filepaths_to_infos.keys(), parquet_metadatas):
            filepaths_to_infos[path]["rows"] = parquet_metadata.num_rows

    return [
        ListingInfo(path=_ensure_path_protocol(protocol, path), **infos) for path, infos in filepaths_to_infos.items()
    ]


def _get_parquet_metadata_single(path: str) -> pa.parquet.FileMetadata:
    """Get the Parquet metadata for a given Parquet file."""
    fs = get_filesystem_from_path(path)
    with fs.open(path) as f:
        return pa.parquet.ParquetFile(f).metadata
