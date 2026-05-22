from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import sys
import urllib.parse
from datetime import datetime, timezone
from typing import Any

from daft.daft import FileFormat, FileInfos, IOConfig, io_glob
from daft.dependencies import fsspec, pafs
from daft.recordbatch import MicroPartition

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class PyArrowFSWithExpiry:
    fs: pafs.FileSystem
    expiry: datetime | None


_CACHED_FSES: dict[tuple[str, IOConfig | None], PyArrowFSWithExpiry] = {}


def _get_fs_from_cache(protocol: str, io_config: IOConfig | None) -> pafs.FileSystem | None:
    """Get an instantiated pyarrow filesystem from the cache based on the URI protocol.

    Returns None if no such cache entry exists.
    """
    global _CACHED_FSES

    if (protocol, io_config) in _CACHED_FSES:
        fs = _CACHED_FSES[(protocol, io_config)]

        if fs.expiry is None or fs.expiry > datetime.now(timezone.utc):
            return fs.fs

    return None


def _put_fs_in_cache(protocol: str, fs: pafs.FileSystem, io_config: IOConfig | None, expiry: datetime | None) -> None:
    """Put pyarrow filesystem in cache under provided protocol."""
    global _CACHED_FSES

    _CACHED_FSES[(protocol, io_config)] = PyArrowFSWithExpiry(fs, expiry)


def get_filesystem(protocol: str, **kwargs: Any) -> fsspec.AbstractFileSystem:
    if protocol == "s3" or protocol == "s3a":
        try:
            import botocore.session
        except ImportError:
            logger.error(
                "Error when importing botocore. install daft[aws] for the required 3rd party dependencies to interact with AWS S3 (https://docs.daft.ai/en/latest/install)"
            )
            raise

        s3fs_kwargs = {}

        # If accessing S3 without credentials, use anonymous access: https://github.com/Eventual-Inc/Daft/issues/503
        credentials_available = botocore.session.get_session().get_credentials() is not None
        if not credentials_available:
            logger.warning(
                "AWS credentials not found - using anonymous access to S3 which will fail if the bucket you are accessing is not a public bucket. See boto3 documentation for more details on configuring your AWS credentials: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
            )
            s3fs_kwargs["anon"] = True

        kwargs = {**kwargs, **s3fs_kwargs}

    try:
        klass = fsspec.get_filesystem_class(protocol)
    except ImportError:
        logger.error(
            "Error when importing dependencies for accessing data with: %s. Please ensure that daft was installed with the appropriate extra dependencies (https://docs.daft.ai/en/latest/install)",
            protocol,
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
    """Return the canonical protocol from the provided protocol, such that there's a 1:1 mapping between protocols and pyarrow/fsspec filesystem implementations."""
    return _CANONICAL_PROTOCOLS.get(protocol, protocol)


def _apply_protocol_alias(path: str, aliases: dict[str, str]) -> str:
    """Rewrite the URI scheme of *path* if it matches an entry in *aliases*."""
    sep = path.find("://")
    if sep == -1:
        return path
    target = aliases.get(path[:sep].lower())
    if target is None:
        return path
    return target + path[sep:]


def _resolve_paths_and_filesystem(
    paths: str | pathlib.Path | list[str],
    io_config: IOConfig | None = None,
) -> tuple[list[str], pafs.FileSystem]:
    """Resolve and normalize paths and return them with their shared PyArrow filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        io_config: A Daft IOConfig that should be best-effort applied onto the returned
            FileSystem.
    """
    if isinstance(paths, pathlib.Path):
        paths = [str(paths)]
    if isinstance(paths, str):
        paths = [paths]
    assert isinstance(paths, list), paths
    assert all(isinstance(p, str) for p in paths), paths
    assert len(paths) > 0, paths

    # Apply protocol aliases from io_config (e.g. {"foo": "s3"}).
    # This mirrors resolve_url_alias() in Rust so that custom URI schemes are
    # handled by the appropriate PyArrow filesystem. The original scheme is
    # preserved by callers (e.g. Iceberg DataFile file_path entries).
    if io_config is not None and io_config.protocol_aliases:
        paths = [_apply_protocol_alias(p, io_config.protocol_aliases) for p in paths]

    # Sanitize s3a/s3n protocols, which Hadoop-based systems use to pick an s3
    # filesystem client. PyArrow doesn't recognize these.
    paths = [f"s3://{p[6:]}" if p.startswith("s3a://") or p.startswith("s3n://") else p for p in paths]

    # All paths must map to the same canonical protocol (hence the same fs).
    canonical_protocols = {canonicalize_protocol(get_protocol_from_path(p)) for p in paths}
    if len(canonical_protocols) > 1:
        raise ValueError(f"All paths must share a canonical protocol, got {canonical_protocols} for paths {paths}")
    protocol = next(iter(canonical_protocols))

    fs = _get_fs_from_cache(protocol, io_config)
    if fs is None:
        fs, expiry = _build_filesystem(protocol, io_config)
        _put_fs_in_cache(protocol, fs, io_config, expiry)

    return [_resolve_path(p, fs) for p in paths], fs


def _build_filesystem(
    protocol: str,
    io_config: IOConfig | None,
) -> tuple[pafs.FileSystem, datetime | None]:
    """Build a PyArrow filesystem for the given canonical protocol."""

    def _set_if_not_none(kwargs: dict[str, Any], key: str, val: Any | None) -> None:
        if val is not None:
            kwargs[key] = val

    if protocol == "s3":
        translated_kwargs: dict[str, Any] = {}
        expiry: datetime | None = None
        if io_config is not None and io_config.s3 is not None:
            s3_config = io_config.s3
            _set_if_not_none(translated_kwargs, "endpoint_override", s3_config.endpoint_url)
            _set_if_not_none(translated_kwargs, "access_key", s3_config.key_id)
            _set_if_not_none(translated_kwargs, "secret_key", s3_config.access_key)
            _set_if_not_none(translated_kwargs, "session_token", s3_config.session_token)
            _set_if_not_none(translated_kwargs, "region", s3_config.region_name)
            _set_if_not_none(translated_kwargs, "anonymous", s3_config.anonymous)
            _set_if_not_none(translated_kwargs, "force_virtual_addressing", s3_config.force_virtual_addressing)
            if s3_config.num_tries is not None:
                from pyarrow.fs import AwsStandardS3RetryStrategy

                translated_kwargs["retry_strategy"] = AwsStandardS3RetryStrategy(max_attempts=s3_config.num_tries)

            if (s3_creds := s3_config.provide_cached_credentials()) is not None:
                _set_if_not_none(translated_kwargs, "access_key", s3_creds.key_id)
                _set_if_not_none(translated_kwargs, "secret_key", s3_creds.access_key)
                _set_if_not_none(translated_kwargs, "session_token", s3_creds.session_token)
                expiry = s3_creds.expiry
        return pafs.S3FileSystem(**translated_kwargs), expiry

    if protocol == "file":
        return pafs.LocalFileSystem(), None

    if protocol == "gs":
        from pyarrow.fs import GcsFileSystem

        gcs_kwargs: dict[str, Any] = {}
        if io_config is not None and io_config.gcs is not None:
            _set_if_not_none(gcs_kwargs, "anonymous", io_config.gcs.anonymous)
            _set_if_not_none(gcs_kwargs, "project_id", io_config.gcs.project_id)
        return GcsFileSystem(**gcs_kwargs), None

    if protocol == "http":
        # Both http and https canonicalize to "http"; build a matching fsspec fs.
        fsspec_fs = fsspec.get_filesystem_class("http")()
        return pafs.PyFileSystem(fsspec_fs), None

    if protocol == "abfs":
        fsspec_fs_cls = fsspec.get_filesystem_class("abfs")
        if io_config is not None:
            # TODO: look into support for other AzureConfig parameters
            fsspec_fs = fsspec_fs_cls(
                account_name=io_config.azure.storage_account,
                account_key=io_config.azure.access_key,
                sas_token=io_config.azure.sas_token,
                tenant_id=io_config.azure.tenant_id,
                client_id=io_config.azure.client_id,
                client_secret=io_config.azure.client_secret,
                anon=io_config.azure.anonymous,
            )
        else:
            fsspec_fs = fsspec_fs_cls()
        return pafs.PyFileSystem(pafs.FSSpecHandler(fsspec_fs)), None

    if protocol == "gvfs":
        # Gravitino's filesystem delegates to Daft's Rust layer; reads go through
        # Rust directly, writes use this custom PyArrow filesystem.
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        return GravitinoFileSystem(io_config=io_config), None

    raise NotImplementedError(f"Cannot infer PyArrow filesystem for protocol {protocol}: please file an issue!")


def _resolve_path(path: str, fs: pafs.FileSystem) -> str:
    """Strip the protocol prefix, expanduser, normalize, and drop trailing slashes."""
    # Strip trailing slashes so downstream "{dir}/{file}" joins don't double them.
    path = path.rstrip("/") or path
    protocol = get_protocol_from_path(path)
    # Gravitino keeps the full gvfs:// URI — its Rust-backed handler expects it.
    if protocol == "gvfs":
        return path
    if protocol == "file":
        path = os.path.expanduser(path)
    return fs.normalize_path(_unwrap_protocol(path))


def _unwrap_protocol(path: str) -> str:
    """Slice off any protocol prefixes on path."""
    parsed = urllib.parse.urlparse(path, allow_fragments=False)  # support '#' in path
    query = "?" + parsed.query if parsed.query else ""  # support '?' in path
    return parsed.netloc + parsed.path + query


###
# File globbing
###


def glob_path_with_stats(
    path: str,
    file_format: FileFormat | None,
    io_config: IOConfig | None,
) -> FileInfos:
    """Glob a path, returning a FileInfos."""
    files = io_glob(path, io_config=io_config)
    filepaths_to_infos = {f["path"]: {"size": f["size"], "type": f["type"]} for f in files}

    # Set number of rows if available.
    if file_format is not None and file_format == FileFormat.Parquet:
        parquet_statistics = MicroPartition.read_parquet_statistics(
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


###
# Path joining
###


def join_path(fs: pafs.FileSystem, base_path: str, *sub_paths: str) -> str:
    """Join a base path with sub-paths using the appropriate path separator for the given filesystem."""
    if isinstance(fs, pafs.LocalFileSystem):
        return os.path.join(base_path, *sub_paths)
    else:
        return f"{base_path.rstrip('/')}/{'/'.join(sub_paths)}"
