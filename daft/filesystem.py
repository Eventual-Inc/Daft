from __future__ import annotations

from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.implementations.cached import SimpleCacheFileSystem

from daft.context import get_context


def get_filesystem(protocol: str, **kwargs) -> AbstractFileSystem:
    klass = get_filesystem_class(protocol)
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

    # If a cache location is set, use as a simple disk-based cache
    cache_location = get_context().cache_location
    if cache_location is not None:
        fs = SimpleCacheFileSystem(fs=fs, cache_storage=str(cache_location / "simple-cache-file-system"))

    return fs
