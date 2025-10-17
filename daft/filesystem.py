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
from daft.expressions.expressions import ExpressionsProjection, col
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


def _resolve_paths_and_filesystem(
    paths: str | pathlib.Path | list[str],
    io_config: IOConfig | None = None,
) -> tuple[list[str], pafs.FileSystem]:
    """Resolves and normalizes the provided path and infers its filesystem.

    Also ensures that the inferred filesystem is compatible with the passed filesystem, if provided.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        io_config: A Daft IOConfig that should be best-effort applied onto the returned
            FileSystem
    """
    if isinstance(paths, pathlib.Path):
        paths = str(paths)
    if isinstance(paths, str):
        paths = [paths]
    assert isinstance(paths, list), paths
    assert all(isinstance(p, str) for p in paths), paths
    assert len(paths) > 0, paths

    # Sanitize s3a/s3n protocols, which are produced by Hadoop-based systems as a way of denoting which s3
    # filesystem client to use. However this doesn't matter for Daft, and PyArrow cannot recognize these protocols.
    paths = [f"s3://{p[6:]}" if p.startswith("s3a://") or p.startswith("s3n://") else p for p in paths]

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

    # Try to get filesystem from protocol -> fs cache.
    resolved_filesystem = _get_fs_from_cache(protocol, io_config)
    if resolved_filesystem is None:
        # Resolve path and filesystem for the first path.
        # We use this first resolved filesystem for validation on all other paths.
        resolved_path, resolved_filesystem, expiry = _infer_filesystem(paths[0], io_config)

        # Put resolved filesystem in cache under these paths' canonical protocol.
        _put_fs_in_cache(protocol, resolved_filesystem, io_config, expiry)
    else:
        resolved_path = _validate_filesystem(paths[0], resolved_filesystem, io_config)

    # filesystem should be a non-None pyarrow FileSystem at this point, either
    # user-provided, taken from the cache, or inferred from the first path.
    assert resolved_filesystem is not None and isinstance(resolved_filesystem, pafs.FileSystem)

    # Resolve all other paths and validate with the user-provided/cached/inferred filesystem.
    resolved_paths = [resolved_path]
    for path in paths[1:]:
        resolved_path = _validate_filesystem(path, resolved_filesystem, io_config)
        resolved_paths.append(resolved_path)

    return resolved_paths, resolved_filesystem


def _validate_filesystem(path: str, fs: pafs.FileSystem, io_config: IOConfig | None) -> str:
    resolved_path, inferred_fs, _ = _infer_filesystem(path, io_config)
    if not isinstance(fs, type(inferred_fs)):
        raise RuntimeError(
            f"Cannot read multiple paths with different inferred PyArrow filesystems. Expected: {fs} but received: {inferred_fs}"
        )
    return resolved_path


def _infer_filesystem(
    path: str,
    io_config: IOConfig | None,
) -> tuple[str, pafs.FileSystem, datetime | None]:
    """Resolves and normalizes the provided path and infers its filesystem and expiry.

    Also ensures that the inferred filesystem is compatible with the passedfilesystem, if provided.

    Args:
        path: A single file/directory path.
        io_config: A Daft IOConfig that should be best-effort applied onto the returned
            FileSystem
    """
    protocol = get_protocol_from_path(path)
    translated_kwargs: dict[str, Any]
    resolved_filesystem: pafs.FileSystem
    expiry: datetime | None = None

    def _set_if_not_none(kwargs: dict[str, Any], key: str, val: Any | None) -> None:
        """Helper method used when setting kwargs for pyarrow."""
        if val is not None:
            kwargs[key] = val

    ###
    # S3
    ###
    if protocol == "s3":
        translated_kwargs = {}
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
                try:
                    from pyarrow.fs import AwsStandardS3RetryStrategy

                    translated_kwargs["retry_strategy"] = AwsStandardS3RetryStrategy(max_attempts=s3_config.num_tries)
                except ImportError:
                    pass  # Config does not exist in pyarrow 7.0.0

            if (s3_creds := s3_config.provide_cached_credentials()) is not None:
                _set_if_not_none(translated_kwargs, "access_key", s3_creds.key_id)
                _set_if_not_none(translated_kwargs, "secret_key", s3_creds.access_key)
                _set_if_not_none(translated_kwargs, "session_token", s3_creds.session_token)

                expiry = s3_creds.expiry

        resolved_filesystem = pafs.S3FileSystem(**translated_kwargs)
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem, expiry

    ###
    # Local
    ###
    elif protocol == "file":
        resolved_filesystem = pafs.LocalFileSystem()
        path = os.path.expanduser(path)
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem, None

    ###
    # GCS
    ###
    elif protocol in {"gs", "gcs"}:
        try:
            from pyarrow.fs import GcsFileSystem
        except ImportError:
            raise ImportError(
                "Unable to import GcsFileSystem from pyarrow - please ensure you have pyarrow >= 9.0 or consider "
                "using Daft's native GCS IO code instead"
            )

        translated_kwargs = {}
        if io_config is not None and io_config.gcs is not None:
            gcs_config = io_config.gcs
            _set_if_not_none(translated_kwargs, "anonymous", gcs_config.anonymous)
            _set_if_not_none(translated_kwargs, "project_id", gcs_config.project_id)

        resolved_filesystem = GcsFileSystem(**translated_kwargs)
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem, None

    ###
    # HTTP: Use FSSpec as a fallback
    ###
    elif protocol in {"http", "https"}:
        fsspec_fs_cls = fsspec.get_filesystem_class(protocol)
        fsspec_fs = fsspec_fs_cls()
        resolved_filesystem = pafs.PyFileSystem(fsspec_fs)
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem, None

    ###
    # Azure: Use FSSpec as a fallback
    ###
    elif protocol in {"az", "abfs", "abfss"}:
        fsspec_fs_cls = fsspec.get_filesystem_class(protocol)

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
        resolved_filesystem = pafs.PyFileSystem(pafs.FSSpecHandler(fsspec_fs))
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem, None

    else:
        raise NotImplementedError(f"Cannot infer PyArrow filesystem for protocol {protocol}: please file an issue!")


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


def overwrite_files(
    written_file_paths: list[str],
    root_dir: str | pathlib.Path,
    io_config: IOConfig | None,
    overwrite_partitions: bool,
) -> None:
    [resolved_path], fs = _resolve_paths_and_filesystem(root_dir, io_config=io_config)

    all_file_paths = []
    if overwrite_partitions:
        # Get all files in ONLY the directories that were written to.

        written_dirs = set(str(pathlib.Path(path).parent) for path in written_file_paths)
        for dir in written_dirs:
            file_selector = pafs.FileSelector(dir, recursive=True)
            try:
                all_file_paths.extend(
                    [info.path for info in fs.get_file_info(file_selector) if info.type == pafs.FileType.File]
                )
            except FileNotFoundError:
                continue
    else:
        # Get all files in the root directory.

        file_selector = pafs.FileSelector(resolved_path, recursive=True)
        try:
            all_file_paths.extend(
                [info.path for info in fs.get_file_info(file_selector) if info.type == pafs.FileType.File]
            )
        except FileNotFoundError:
            # The root directory does not exist, so there are no files to delete.
            return

    all_file_paths_df = MicroPartition.from_pydict({"path": all_file_paths})

    # Find the files that were not written to in this run and delete them.
    to_delete = all_file_paths_df.filter(ExpressionsProjection([~(col("path").is_in(written_file_paths))]))

    # TODO: Look into parallelizing this
    for entry in to_delete.get_column_by_name("path"):
        fs.delete_file(entry)
