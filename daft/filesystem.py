from __future__ import annotations

import dataclasses
import pathlib
import sys
import urllib.parse

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal


import pyarrow as pa
from loguru import logger
from pyarrow.fs import FileSystem, _resolve_filesystem_and_path

from daft.daft import FileFormat, FileInfos, IOConfig, io_glob
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


def _resolve_paths_and_filesystem(
    paths: str | pathlib.Path | list[str],
    filesystem: FileSystem | None = None,
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
    filesystem: FileSystem | None,
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
            logger.error(f"Daft does not yet support reading natively from url: {path} - please make an issue!")
            raise
        else:
            raise

    resolved_path = resolved_filesystem.normalize_path(resolved_path)
    return resolved_path, resolved_filesystem


###
# File globbing
###


def glob_path_with_stats(
    path: str,
    file_format: FileFormat | None,
    io_config: IOConfig | None,
) -> FileInfos:
    """Glob a path, returning a list ListingInfo."""

    files = io_glob(path, io_config=io_config)
    filepaths_to_infos = {f["path"]: {"size": f["size"], "type": f["type"]} for f in files}

    # Set number of rows if available.
    if file_format is not None and file_format == FileFormat.Parquet:
        parquet_statistics = Table.read_parquet_statistics(
            list(filepaths_to_infos.keys()),
            io_config=io_config,
        ).to_pydict()
        for path, num_rows in zip(parquet_statistics["uris"], parquet_statistics["row_count"]):
            filepaths_to_infos[path]["rows"] = num_rows

    file_paths = []
    file_sizes = []
    num_rows = []
    for path, infos in filepaths_to_infos.items():
        file_paths.append(path)
        file_sizes.append(infos.get("size"))
        num_rows.append(infos.get("rows"))

    return FileInfos.from_infos(file_paths=file_paths, file_sizes=file_sizes, num_rows=num_rows)
