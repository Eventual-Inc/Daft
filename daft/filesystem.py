from __future__ import annotations

import dataclasses

from fsspec import AbstractFileSystem, get_filesystem_class
from loguru import logger


@dataclasses.dataclass(frozen=True)
class FileInfo:
    path: str
    size: int


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


def _fix_returned_path(protocol: str, returned_path: str) -> str:
    """This function adds the protocol that fsspec strips from returned results"""
    return returned_path if protocol == "file" else f"{protocol}://{returned_path}"


def _path_is_glob(path: str) -> bool:
    # fsspec glob supports *, ? and [..] patterns
    # See: : https://filesystem-spec.readthedocs.io/en/latest/api.html
    return any([char in path for char in ["*", "?", "["]])


def glob_path(path: str) -> list[str]:
    fs = get_filesystem_from_path(path)
    protocol = get_protocol_from_path(path)

    if _path_is_glob(path):
        globbed_data = fs.glob(path, detail=False)
        return [_fix_returned_path(protocol, path) for path in globbed_data]

    if fs.isfile(path):
        return [path]
    elif fs.isdir(path):
        files_info = fs.ls(path, detail=False)
        return [_fix_returned_path(protocol, path) for path in files_info]
    raise FileNotFoundError(f"File or directory not found: {path}")


def glob_path_with_stats(path: str) -> list[FileInfo]:
    fs = get_filesystem_from_path(path)
    protocol = get_protocol_from_path(path)

    if _path_is_glob(path):
        globbed_data = fs.glob(path, detail=True)
        return [
            FileInfo(path=_fix_returned_path(protocol, path), size=details["size"])
            for path, details in globbed_data.items()
        ]

    if fs.isfile(path):
        file_info = fs.info(path)
        return [FileInfo(path=_fix_returned_path(protocol, file_info["name"]), size=file_info["size"])]
    elif fs.isdir(path):
        files_info = fs.ls(path, detail=True)
        return [
            FileInfo(path=_fix_returned_path(protocol, file_info["name"]), size=file_info["size"])
            for file_info in files_info
        ]
    raise FileNotFoundError(f"File or directory not found: {path}")
