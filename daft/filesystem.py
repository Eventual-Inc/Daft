from __future__ import annotations

import dataclasses
import pathlib
import sys
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

from typing import Any

import fsspec
import pyarrow as pa
from fsspec.registry import get_filesystem_class
from loguru import logger
from pyarrow.fs import (
    FileSystem,
    FSSpecHandler,
    PyFileSystem,
    _resolve_filesystem_and_path,
)

from daft.daft import FileFormat, FileInfos, NativeStorageConfig, StorageConfig
from daft.table import Table

_CACHED_FSES: dict[str, FileSystem] = {}


def _get_fs_from_cache(protocol: str) -> FileSystem | None:
    """
    Get an instantiated pyarrow filesystem from the cache based on the URI protocol.

    Returns None if no such cache entry exists.
    """
    global _CACHED_FSES

    return _CACHED_FSES.get(protocol)


def _put_fs_in_cache(protocol: str, fs: FileSystem) -> None:
    """Put pyarrow filesystem in cache under provided protocol."""
    global _CACHED_FSES

    _CACHED_FSES[protocol] = fs


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

    # kwargs["default_fill_cache"] = False

    return kwargs


def get_filesystem(protocol: str, **kwargs) -> fsspec.AbstractFileSystem:
    if protocol == "s3" or protocol == "s3a":
        kwargs = {**kwargs, **_get_s3fs_kwargs()}

    try:
        klass = fsspec.get_filesystem_class(protocol)
    except ImportError:
        logger.error(
            f"Error when importing dependencies for accessing data with: {protocol}. Please ensure that getdaft was installed with the appropriate extra dependencies (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
        )
        raise

    fs = klass(**kwargs)
    return fs


def get_protocol_from_path(path: str) -> str:
    parsed_scheme = urllib.parse.urlparse(path, allow_fragments=False).scheme
    parsed_scheme = parsed_scheme.lower()
    if parsed_scheme == "" or parsed_scheme is None:
        return "file"
    if sys.platform == "win32" and len(parsed_scheme) == 1 and ("a" <= parsed_scheme) and (parsed_scheme <= "z"):
        return "file"
    return parsed_scheme


_CANONICAL_PROTOCOLS = {
    "gcs": "gs",
    "https": "http",
    "s3a": "s3",
    "ssh": "sftp",
    "arrow_hdfs": "hdfs",
    "az": "abfs",
    "blockcache": "cached",
    "jlab": "jupyter",
}


def canonicalize_protocol(protocol: str) -> str:
    """
    Return the canonical protocol from the provided protocol, such that there's a 1:1
    mapping between protocols and pyarrow/fsspec filesystem implementations.
    """
    return _CANONICAL_PROTOCOLS.get(protocol, protocol)


def get_filesystem_from_path(path: str, **kwargs) -> fsspec.AbstractFileSystem:
    protocol = get_protocol_from_path(path)
    fs = get_filesystem(protocol, **kwargs)
    return fs


def _resolve_paths_and_filesystem(
    paths: str | pathlib.Path | list[str],
    filesystem: FileSystem | fsspec.AbstractFileSystem | None = None,
) -> tuple[list[str], FileSystem]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths, and ensures that all paths use the same filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    if isinstance(paths, pathlib.Path):
        paths = str(paths)
    if isinstance(paths, str):
        paths = [paths]
    assert isinstance(paths, list), paths
    assert all(isinstance(p, str) for p in paths), paths
    assert len(paths) > 0, paths

    # Ensure that protocols for all paths are consistent, i.e. that they would map to the
    # same filesystem.
    protocols = {get_protocol_from_path(path) for path in paths}
    canonicalized_protocols = {canonicalize_protocol(protocol) for protocol in protocols}
    if len(canonicalized_protocols) > 1:
        raise ValueError(
            "All paths must have the same canonical protocol to ensure that they are all "
            f"hitting the same storage backend, but got protocols {protocols} with canonical "
            f"protocols - {canonicalized_protocols} and full paths - {paths}"
        )

    # Canonical protocol shared by all paths.
    protocol = next(iter(canonicalized_protocols))

    if filesystem is None:
        # Try to get filesystem from protocol -> fs cache.
        filesystem = _get_fs_from_cache(protocol)
    elif isinstance(filesystem, fsspec.AbstractFileSystem):
        # Wrap fsspec filesystems so they are valid pyarrow filesystems.
        filesystem = PyFileSystem(FSSpecHandler(filesystem))

    # Resolve path and filesystem for the first path.
    # We use this first resolved filesystem for validation on all other paths.
    resolved_path, resolved_filesystem = _resolve_path_and_filesystem(paths[0], filesystem)

    if filesystem is None:
        filesystem = resolved_filesystem
        # Put resolved filesystem in cache under these paths' canonical protocol.
        _put_fs_in_cache(protocol, filesystem)

    # filesystem should be a non-None pyarrow FileSystem at this point, either
    # user-provided, taken from the cache, or inferred from the first path.
    assert filesystem is not None and isinstance(filesystem, FileSystem)

    # Resolve all other paths and validate with the user-provided/cached/inferred filesystem.
    resolved_paths = [resolved_path]
    for path in paths[1:]:
        resolved_path, _ = _resolve_path_and_filesystem(path, filesystem)
        resolved_paths.append(resolved_path)

    return resolved_paths, filesystem


def _resolve_path_and_filesystem(
    path: str,
    filesystem: FileSystem | fsspec.AbstractFileSystem | None,
) -> tuple[str, FileSystem]:
    """
    Resolves and normalizes the provided path, infers a filesystem from the
    path, and ensures that the inferred filesystem is compatible with the passed
    filesystem, if provided.

    Args:
        path: A single file/directory path.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against the
            filesystem inferred from the provided path to ensure compatibility.
    """
    # Use pyarrow utility to resolve filesystem and this particular path.
    # If a non-None filesystem is provided to this utility, it will ensure that
    # it is compatible with the provided path.
    # A non-None filesystem will be provided if:
    #  - a user-provided filesystem was passed to _resolve_paths_and_filesystem.
    #  - a filesystem for the paths' protocol exists in the protocol -> fs cache.
    #  - a filesystem was resolved for a previous path; i.e., filesystem is
    #    guaranteed to be non-None for all but the first path.
    try:
        resolved_filesystem, resolved_path = _resolve_filesystem_and_path(path, filesystem)
    except pa.lib.ArrowInvalid as e:
        if "Unrecognized filesystem type in URI" in str(e):
            # Fall back to fsspec.
            protocol = get_protocol_from_path(path)
            logger.debug(f"pyarrow doesn't support paths with protocol {protocol}, falling back to fsspec.")
            try:
                fsspec_fs_cls = get_filesystem_class(protocol)
            except ValueError:
                raise ValueError("pyarrow and fsspec don't recognize protocol {protocol} for path {path}.")
            fsspec_fs = fsspec_fs_cls()
            resolved_filesystem, resolved_path = _resolve_filesystem_and_path(path, fsspec_fs)
        else:
            raise

    # If filesystem is fsspec HTTPFileSystem, the protocol/scheme of paths
    # should not be unwrapped/removed, because HTTPFileSystem expects full file
    # paths including protocol/scheme. This is different behavior compared to
    # pyarrow filesystems.
    if not _is_http_fs(resolved_filesystem):
        resolved_path = _unwrap_protocol(resolved_path)

    resolved_path = resolved_filesystem.normalize_path(resolved_path)
    return resolved_path, resolved_filesystem


def _is_http_fs(fs: FileSystem) -> bool:
    """Returns whether the provided pyarrow filesystem is an HTTP filesystem."""
    from fsspec.implementations.http import HTTPFileSystem

    return (
        isinstance(fs, PyFileSystem)
        and isinstance(fs.handler, FSSpecHandler)
        and isinstance(fs.handler.fs, HTTPFileSystem)
    )


def _unwrap_protocol(path):
    """
    Slice off any protocol prefixes on path.
    """
    parsed = urllib.parse.urlparse(path, allow_fragments=False)  # support '#' in path
    query = "?" + parsed.query if parsed.query else ""  # support '?' in path
    return parsed.netloc + parsed.path + query


###
# File globbing
###


def _ensure_path_protocol(protocol: str, returned_path: str) -> str:
    """This function adds the protocol that fsspec strips from returned results"""
    if protocol == "file":
        return returned_path
    parsed_scheme = urllib.parse.urlparse(returned_path).scheme
    if parsed_scheme == "" or parsed_scheme is None:
        return f"{protocol}://{returned_path}"
    return returned_path


def _path_is_glob(path: str) -> bool:
    # fsspec glob supports *, ? and [..] patterns
    # See: : https://filesystem-spec.readthedocs.io/en/latest/api.html
    return any([char in path for char in ["*", "?", "["]])


def glob_path_with_stats(
    path: str,
    file_format: FileFormat | None,
    fs: fsspec.AbstractFileSystem,
    storage_config: StorageConfig | None,
) -> FileInfos:
    """Glob a path, returning a list ListingInfo."""
    protocol = get_protocol_from_path(path)

    filepaths_to_infos: dict[str, dict[str, Any]] = defaultdict(dict)

    if _path_is_glob(path):
        globbed_data = fs.glob(path, detail=True)

        for path, details in globbed_data.items():
            path = _ensure_path_protocol(protocol, path)
            filepaths_to_infos[path]["size"] = details["size"]

    elif fs.isfile(path):
        file_info = fs.info(path)

        filepaths_to_infos[path]["size"] = file_info["size"]

    elif fs.isdir(path):
        files_info = fs.ls(path, detail=True)

        for file_info in files_info:
            path = file_info["name"]
            path = _ensure_path_protocol(protocol, path)
            filepaths_to_infos[path]["size"] = file_info["size"]

    else:
        raise FileNotFoundError(f"File or directory not found: {path}")

    # Set number of rows if available.
    if file_format is not None and file_format == FileFormat.Parquet:
        config = storage_config.config if storage_config is not None else None
        if config is not None and isinstance(config, NativeStorageConfig):
            parquet_statistics = Table.read_parquet_statistics(
                list(filepaths_to_infos.keys()), config.io_config
            ).to_pydict()
            for path, num_rows in zip(parquet_statistics["uris"], parquet_statistics["row_count"]):
                filepaths_to_infos[path]["rows"] = num_rows
        else:
            parquet_metadatas = ThreadPoolExecutor().map(_get_parquet_metadata_single, filepaths_to_infos.keys())
            for path, parquet_metadata in zip(filepaths_to_infos.keys(), parquet_metadatas):
                filepaths_to_infos[path]["rows"] = parquet_metadata.num_rows

    file_paths = []
    file_sizes = []
    num_rows = []
    for path, infos in filepaths_to_infos.items():
        file_paths.append(_ensure_path_protocol(protocol, path))
        file_sizes.append(infos.get("size"))
        num_rows.append(infos.get("rows"))

    return FileInfos.from_infos(file_paths=file_paths, file_sizes=file_sizes, num_rows=num_rows)


def _get_parquet_metadata_single(path: str) -> pa.parquet.FileMetadata:
    """Get the Parquet metadata for a given Parquet file."""
    fs = get_filesystem_from_path(path)
    with fs.open(path) as f:
        return pa.parquet.ParquetFile(f).metadata
