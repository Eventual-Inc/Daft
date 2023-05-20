from __future__ import annotations

import dataclasses
import pathlib
import sys
import urllib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

from typing import Any

import fsspec
import pyarrow as pa
from fsspec.implementations.http import HTTPFileSystem
from loguru import logger
from pyarrow.fs import FileSystem

from daft.datasources import ParquetSourceInfo, SourceInfo

_LOCAL_SCHEME = "local"
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
    if parsed_scheme == "" or parsed_scheme is None:
        return "file"
    return parsed_scheme


def get_filesystem_from_path(path: str, **kwargs) -> fsspec.AbstractFileSystem:
    protocol = get_protocol_from_path(path)
    fs = get_filesystem(protocol, **kwargs)
    return fs


def _resolve_paths_and_filesystem(
    paths: str | pathlib.Path | list[str],
    filesystem: pa.fs.FileSystem | None = None,
) -> tuple[list[str], pa.fs.FileSystem]:
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
    import pyarrow as pa
    from pyarrow.fs import (
        FileSystem,
        FSSpecHandler,
        PyFileSystem,
        _resolve_filesystem_and_path,
    )

    if isinstance(paths, pathlib.Path):
        paths = str(paths)
    if isinstance(paths, str):
        paths = [paths]
    elif not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        raise ValueError(f"paths must be a path string or a list of path strings, got: {paths}")
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")

    if filesystem and not isinstance(filesystem, FileSystem):
        err_msg = (
            "The filesystem passed must be either pyarrow.fs.FileSystem or "
            f"fsspec.spec.AbstractFileSystem. The provided filesystem was: {filesystem}"
        )
        if not isinstance(filesystem, fsspec.spec.AbstractFileSystem):
            raise TypeError(err_msg) from None

        filesystem = PyFileSystem(FSSpecHandler(filesystem))

    resolved_paths = []
    for path in paths:
        path = _resolve_custom_scheme(path)
        protocol = get_protocol_from_path(path)
        cached_fs = _get_fs_from_cache(protocol)
        if cached_fs is not None:
            filesystem = cached_fs
        try:
            resolved_filesystem, resolved_path = _resolve_filesystem_and_path(path, filesystem)
        except pa.lib.ArrowInvalid as e:
            if "Cannot parse URI" in str(e):
                resolved_filesystem, resolved_path = _resolve_filesystem_and_path(_encode_url(path), filesystem)
                resolved_path = _decode_url(resolved_path)
            elif "Unrecognized filesystem type in URI" in str(e):
                # Fall back to fsspec.
                logger.debug(f"pyarrow doesn't support paths with protocol {protocol}, falling back to fsspec.")
                try:
                    from fsspec.registry import get_filesystem_class
                except ModuleNotFoundError:
                    raise ImportError(f"Please install fsspec to read files with protocol {protocol}.") from None
                try:
                    fsspec_fs_cls = get_filesystem_class(protocol)
                except ValueError:
                    raise ValueError("pyarrow and fsspec don't recognize protocol {protocol} for path {path}.")
                resolved_filesystem = PyFileSystem(FSSpecHandler(fsspec_fs_cls()))
                resolved_path = path
            else:
                raise
        _put_fs_in_cache(protocol, resolved_filesystem)
        if filesystem is None:
            filesystem = resolved_filesystem
        # If filesystem is fsspec HTTPFileSystem, the protocol/scheme of paths
        # should not be unwrapped/removed, because HTTPFileSystem expects full file
        # paths including protocol/scheme. This is different behavior compared to
        # filesystems implementated in pyarrow.
        elif not (
            isinstance(resolved_filesystem, PyFileSystem)
            and isinstance(resolved_filesystem.handler, FSSpecHandler)
            and isinstance(resolved_filesystem.handler.fs, HTTPFileSystem)
        ):
            resolved_path = _unwrap_protocol(resolved_path)
        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    return resolved_paths, filesystem


def _encode_url(path):
    return urllib.parse.quote(path, safe="/:")


def _decode_url(path):
    return urllib.parse.unquote(path)


def _unwrap_protocol(path):
    """
    Slice off any protocol prefixes on path.
    """
    if sys.platform == "win32" and _is_local_windows_path(path):
        # Represent as posix path such that downstream functions properly handle it.
        # This is executed when 'file://' is NOT included in the path.
        return pathlib.Path(path).as_posix()

    parsed = urllib.parse.urlparse(path, allow_fragments=False)  # support '#' in path
    query = "?" + parsed.query if parsed.query else ""  # support '?' in path
    netloc = parsed.netloc
    if parsed.scheme == "s3" and "@" in parsed.netloc:
        # If the path contains an @, it is assumed to be an anonymous
        # credentialed path, and we need to strip off the credentials.
        netloc = parsed.netloc.split("@")[-1]

    parsed_path = parsed.path
    # urlparse prepends the path with a '/'. This does not work on Windows
    # so if this is the case strip the leading slash.
    if (
        sys.platform == "win32"
        and not netloc
        and len(parsed_path) >= 3
        and parsed_path[0] == "/"  # The problematic leading slash
        and parsed_path[1].isalpha()  # Ensure it is a drive letter.
        and parsed_path[2:4] in (":", ":/")
    ):
        parsed_path = parsed_path[1:]

    return netloc + parsed_path + query


def _is_local_windows_path(path: str) -> bool:
    """Determines if path is a Windows file-system location."""
    if len(path) >= 1 and path[0] == "\\":
        return True
    if len(path) >= 3 and path[1] == ":" and (path[2] == "/" or path[2] == "\\") and path[0].isalpha():
        return True
    return False


def _resolve_custom_scheme(path: str) -> str:
    """Returns the resolved path if the given path follows a Ray-specific custom
    scheme. Othewise, returns the path unchanged.

    The supported custom schemes are: "local", "example".
    """
    import urllib.parse

    parsed_uri = urllib.parse.urlparse(path)
    if parsed_uri.scheme == _LOCAL_SCHEME:
        path = parsed_uri.netloc + parsed_uri.path
    return path


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
    source_info: SourceInfo | None,
    fs: fsspec.AbstractFileSystem,
) -> list[ListingInfo]:
    """Glob a path, returning a list ListingInfo."""
    protocol = get_protocol_from_path(path)

    filepaths_to_infos: dict[str, dict[str, Any]] = defaultdict(dict)

    if _path_is_glob(path):
        globbed_data = fs.glob(path, detail=True)

        for path, details in globbed_data.items():
            path = _ensure_path_protocol(protocol, path)
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
            path = _ensure_path_protocol(protocol, path)
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
