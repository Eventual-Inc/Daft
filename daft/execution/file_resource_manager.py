"""Resource manager for downloading and caching task resources.

Before task execution,
the ResourceManager downloads added resources to the worker's local filesystem.

Archive resources support the ``name#path`` format to specify an extraction
directory.  For example:

- ``archive.zip`` or ``archive.zip#`` — extract to the current working directory.
- ``archive.zip#/tmp/mydir`` — extract to ``/tmp/mydir``.
- ``s3://bucket/data.tar.gz#/opt/data`` — download and extract to ``/opt/data``.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tarfile
import tempfile
import time
import zipfile
from typing import Literal

from daft.file import File

logger = logging.getLogger(__name__)

# File extensions
_ARCHIVE_EXTENSIONS = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".zip", ".whl")

_REMOTE_SCHEMES = ("s3://", "gs://", "gcs://", "http://", "https://", "az://", "abfs://")

# Retry configuration for remote downloads
_MAX_DOWNLOAD_RETRIES = 3
_RETRY_BACKOFF_SECS = 1.0

# Cache size limit (default 10 GiB)
_CACHE_MAX_SIZE_BYTES = 10 * 1024 * 1024 * 1024


def _get_extension(name: str) -> str:
    """Extract the file extension, handling ``#path`` suffix and compound extensions."""
    # Strip #path suffix for archive extraction path
    actual_name = name.split("#")[0] if "#" in name else name
    lower = actual_name.lower()
    for ext in (".tar.gz", ".tar.bz2"):
        if lower.endswith(ext):
            return ext
    _, ext = os.path.splitext(lower)
    return ext


def _is_archive(name: str) -> bool:
    """Check whether the resource is an archive that needs extraction."""
    return _get_extension(name) in _ARCHIVE_EXTENSIONS


def _parse_resource_name(name: str) -> tuple[str, str | None]:
    """Parse a resource name that may contain a ``#path`` extraction directory.

    The ``#path`` suffix is only meaningful for archive types.  For non-archive
    files (``.py``, ``.egg``, etc.) the ``#`` is treated as part of the name.

    Args:
        name: Resource name, possibly containing ``#path``.

    Returns:
        A tuple of ``(actual_name, extract_path)``.  *extract_path* is ``None``
        when no explicit extraction directory is specified.

    Examples:
        >>> _parse_resource_name("archive.zip")
        ('archive.zip', None)
        >>> _parse_resource_name("archive.zip#")
        ('archive.zip', None)
        >>> _parse_resource_name("archive.zip#/tmp/dir")
        ('archive.zip', '/tmp/dir')
        >>> _parse_resource_name("s3://bucket/data.tar.gz#/opt/data")
        ('s3://bucket/data.tar.gz', '/opt/data')
        >>> _parse_resource_name("model.py#something")
        ('model.py#something', None)
    """
    if "#" not in name:
        return name, None

    # Split on the last '#'
    idx = name.rfind("#")
    candidate = name[:idx]

    # Only split if the part before '#' looks like an archive
    candidate_ext = _get_extension(candidate)
    if candidate_ext not in _ARCHIVE_EXTENSIONS:
        # Not an archive — treat '#' as part of the name
        return name, None

    path_part = name[idx + 1 :]
    return candidate, path_part if path_part else None


class FileResourceManager:
    """Resource manager that downloads file resources to the worker's local filesystem.

    file types:
    - Python files: .py, .egg — downloaded to the current working directory.
    - Archives: .tar, .tar.gz, .tgz, .tar.bz2, .zip, .whl — downloaded and extracted
      to the current working directory (or to a custom directory via ``name#path``).

    Archive resources may use the ``name#path`` format to specify an extraction
    directory.  See :func:`_parse_resource_name` for details.

    Resources are tracked by name to avoid duplicate downloads within the same
    worker lifecycle.
    """

    def __init__(self, cache_max_size_bytes: int = _CACHE_MAX_SIZE_BYTES) -> None:
        self._cache_dir = os.path.join(tempfile.gettempdir(), "daft_resources")
        self._resolved: dict[str, tuple[str, int]] = {}  # resource name -> (local path, timestamp)
        self._cache_max_size_bytes = cache_max_size_bytes
        os.makedirs(self._cache_dir, exist_ok=True)

    @property
    def cache_dir(self) -> str:
        """Return the temporary download cache directory."""
        return self._cache_dir

    def _evict_cache_if_needed(self) -> None:
        """Evict oldest cached files (by access time) until total size is under the limit."""
        try:
            entries = []
            total_size = 0
            for entry in os.scandir(self._cache_dir):
                if entry.is_file(follow_symlinks=False):
                    stat = entry.stat()
                    entries.append((entry.path, stat.st_atime, stat.st_size))
                    total_size += stat.st_size

            if total_size <= self._cache_max_size_bytes:
                return

            # Sort by access time ascending (oldest first) for LRU eviction
            entries.sort(key=lambda item: item[1])

            for path, _, size in entries:
                if total_size <= self._cache_max_size_bytes:
                    break
                try:
                    os.remove(path)
                    total_size -= size
                    logger.debug("Evicted cached file '%s' (%d bytes)", path, size)
                except OSError:
                    pass

            logger.info(
                "Cache eviction complete, current size: %d bytes (limit: %d bytes)",
                total_size,
                self._cache_max_size_bytes,
            )
        except OSError as e:
            logger.warning("Failed to scan cache directory for eviction: %s", e)

    def resolve(self, added_resources: dict[str, int]) -> None:
        """Resolve added resources by downloading them to the worker.

        For .py / .egg files, the file is placed in the current working directory.
        For archive files, the archive is extracted into the current working directory
        unless a ``#path`` suffix specifies an alternative extraction directory.

        Args:
            added_resources: Mapping of resource name/URI to Unix millisecond timestamp.
                Archive names may use ``name#path`` format.
        """
        if not added_resources:
            return

        for name, timestamp in added_resources.items():
            if name in self._resolved:
                _, resolved_timestamp = self._resolved[name]
                if resolved_timestamp == timestamp:
                    logger.debug("Resource '%s' already resolved with same timestamp, skipping", name)
                    continue
                logger.info(
                    "Resource '%s' timestamp changed (%d -> %d), re-downloading", name, resolved_timestamp, timestamp
                )

            actual_name, extract_path = _parse_resource_name(name)

            local_path = self._fetch_resource(actual_name, timestamp, extract_path)
            if local_path is not None:
                self._resolved[name] = (local_path, timestamp)
                logger.info("Resolved resource '%s' -> %s", name, local_path)
            else:
                logger.warning("Failed to resolve resource '%s'", name)

    def get_resource_path(self, name: str) -> str | None:
        """Get the local path for a resolved resource.

        For archives, returns the directory they were extracted to.
        For .py / .egg files, returns the path in the working directory.

        Args:
            name: The resource name/URI.

        Returns:
            Local filesystem path, or None if not resolved.
        """
        entry = self._resolved.get(name)
        return entry[0] if entry is not None else None

    def _fetch_resource(self, name: str, timestamp: int, extract_path: str | None = None) -> str:
        """Fetch a resource: download to cache, then place or extract.

        Args:
            name: Resource name (without ``#path``), local path, or remote URI.
            timestamp: Unix millisecond timestamp for cache invalidation.
            extract_path: Optional extraction directory for archives.  When
                ``None``, archives are extracted to the current working directory.

        Returns:
            Final local path (file or extraction directory).
        """
        # Step 1: download to cache
        cached_path = self._download_to_cache(name, timestamp)

        # Step 2: place or extract
        if _is_archive(name):
            dest_dir = extract_path if extract_path is not None else os.getcwd()
            if extract_path is not None:
                os.makedirs(extract_path, exist_ok=True)
            return self._extract_archive(cached_path, name, dest_dir)
        else:
            # copy to cwd
            cwd = os.getcwd()
            dest = os.path.join(cwd, os.path.basename(name))
            shutil.copy2(cached_path, dest)
            return dest

    def _download_to_cache(self, name: str, timestamp: int) -> str:
        """Download a resource to the local cache directory.

        Supports local files and remote URIs (S3, GCS, HTTP, Azure, etc.)
        via Daft's native IO layer.

        Args:
            name: Resource name, local path, or remote URI.
            timestamp: Unix millisecond timestamp for cache invalidation.

        Returns:
            Path to the cached file.

        Raises:
            FileNotFoundError: If the resource is a local path that does not exist.
            ValueError: If the resource scheme is not supported.
            RuntimeError: If a remote download fails after all retries.
        """
        self._evict_cache_if_needed()

        cache_key = hashlib.sha256(f"{name}:{timestamp}".encode()).hexdigest()[:16]
        basename = os.path.basename(name) if "/" in name or "\\" in name else name
        cached_path = os.path.join(self._cache_dir, f"{cache_key}_{basename}")

        # Already in cache — touch to update access time for LRU eviction
        if os.path.exists(cached_path):
            logger.debug("Resource '%s' found in cache at %s", name, cached_path)
            os.utime(cached_path)
            return cached_path

        # Local file — copy to cache
        if os.path.isfile(name):
            shutil.copy2(name, cached_path)
            return cached_path

        # Non-local, non-remote — cannot resolve
        if not any(name.startswith(s) for s in _REMOTE_SCHEMES):
            if not os.path.exists(name):
                raise FileNotFoundError(f"Resource not found: '{name}'")
            raise ValueError(f"Unsupported resource type: '{name}'")

        # Remote URI — download via Daft IO with retry
        last_error: Exception | None = None
        for attempt in range(1, _MAX_DOWNLOAD_RETRIES + 1):
            try:
                remote_file = File(name)
                with remote_file.open() as f:
                    data = f.read()
                with open(cached_path, "wb") as out:
                    out.write(data)
                return cached_path
            except Exception as e:
                last_error = e
                if attempt < _MAX_DOWNLOAD_RETRIES:
                    wait = _RETRY_BACKOFF_SECS * (2 ** (attempt - 1))
                    logger.warning(
                        "Download attempt %d/%d failed for '%s': %s. Retrying in %.1fs...",
                        attempt,
                        _MAX_DOWNLOAD_RETRIES,
                        name,
                        e,
                        wait,
                    )
                    time.sleep(wait)

        raise RuntimeError(
            f"Failed to download resource '{name}' after {_MAX_DOWNLOAD_RETRIES} attempts: {last_error}"
        ) from last_error

    @staticmethod
    def _validate_extraction_path(member_name: str, dest_dir: str) -> None:
        """Validate that an archive member does not escape the destination directory.

        Prevents Zip Slip / path traversal attacks where malicious archives
        contain entries like ``../../../etc/passwd``.

        Args:
            member_name: The name/path of the archive member.
            dest_dir: The intended extraction directory.

        Raises:
            ValueError: If the member path would escape *dest_dir*.
        """
        target = os.path.realpath(os.path.join(dest_dir, member_name))
        dest_real = os.path.realpath(dest_dir)
        if not target.startswith(dest_real + os.sep) and target != dest_real:
            raise ValueError(f"Archive member '{member_name}' would escape destination directory '{dest_dir}'")

    def _extract_archive(self, cached_path: str, name: str, dest_dir: str) -> str:
        """Extract an archive file into the destination directory.

        All archive members are validated against path traversal attacks
        before extraction.

        Args:
            cached_path: Local path to the downloaded archive.
            name: Original resource name (used for extension detection).
            dest_dir: Directory to extract into.

        Returns:
            The destination directory path.

        Raises:
            ValueError: If the archive format is unknown or path traversal is detected.
            RuntimeError: If extraction fails due to a corrupt archive or I/O error.
        """
        ext = _get_extension(name)
        if ext in (".zip", ".whl"):
            with zipfile.ZipFile(cached_path, "r") as zf:
                for member_name in zf.namelist():
                    self._validate_extraction_path(member_name, dest_dir)
                zf.extractall(dest_dir)
        elif ext in (".tar", ".tar.gz", ".tgz", ".tar.bz2"):
            mode: Literal["r:", "r:gz", "r:bz2"] = (
                "r:gz" if ext in (".tar.gz", ".tgz") else "r:bz2" if ext == ".tar.bz2" else "r:"
            )
            with tarfile.open(cached_path, mode) as tf:
                for member in tf.getmembers():
                    self._validate_extraction_path(member.name, dest_dir)
                tf.extractall(dest_dir, filter="fully_trusted")
        else:
            raise ValueError(f"Unknown archive format for '{name}'")
        return dest_dir


file_resource_manager = FileResourceManager()
