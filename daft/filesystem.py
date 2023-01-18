from __future__ import annotations

from fsspec import AbstractFileSystem, get_filesystem_class
from loguru import logger


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
    try:
        fs = get_filesystem(protocol, **kwargs)
    except ImportError:
        logger.error(
            f"Error when importing dependencies for accessing data with: {protocol}. Please ensure that getdaft was installed with the appropriate extra dependencies (https://getdaft.io/docs/learn/install.html)"
        )
        raise
    return fs


def glob_path(path: str) -> list[str]:
    fs = get_filesystem_from_path(path)
    protocol = get_protocol_from_path(path)
    if fs.isdir(path):
        return [f"{protocol}://{path}" if protocol != "file" else path for path in fs.ls(path)]
    elif fs.isfile(path):
        return [path]
    try:
        expanded = fs.expand_path(path, recursive=True)
    except FileNotFoundError:
        expanded = []
    return [f"{protocol}://{path}" if protocol != "file" else path for path in expanded]
