from __future__ import annotations

import dataclasses
import sys

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

from fsspec import AbstractFileSystem, get_filesystem_class
from loguru import logger


@dataclasses.dataclass(frozen=True)
class ListingInfo:
    path: str
    size: int
    type: Literal["file"] | Literal["directory"]


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

    # If accessing S3 without credentials, use anonymous access: https://github.com/Eventual-Inc/Daft/issues/503
    if protocol == "s3" or protocol == "s3a":
        try:
            import boto3
        except ImportError:
            logger.error(
                "Error when importing boto3. Install getdaft[aws] for the required 3rd party dependencies to interact with AWS S3 (https://getdaft.io/docs/learn/install.html)"
            )
            raise

        credentials_available = boto3.session.Session().get_credentials() is not None
        if not credentials_available:
            logger.warning(
                "AWS credentials not found - using anonymous access to S3 which will fail if the bucket you are accessing is not a public bucket. See boto3 documentation for more details on configuring your AWS credentials: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
            )
            kwargs["anon"] = True

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


def glob_path_with_stats(path: str) -> list[ListingInfo]:
    fs = get_filesystem_from_path(path)
    protocol = get_protocol_from_path(path)

    if _path_is_glob(path):
        globbed_data = fs.glob(path, detail=True)
        return [
            ListingInfo(path=_fix_returned_path(protocol, path), size=details["size"], type=details["type"])
            for path, details in globbed_data.items()
        ]

    if fs.isfile(path):
        file_info = fs.info(path)
        return [
            ListingInfo(
                path=_fix_returned_path(protocol, file_info["name"]), size=file_info["size"], type=file_info["type"]
            )
        ]
    elif fs.isdir(path):
        files_info = fs.ls(path, detail=True)
        return [
            ListingInfo(
                path=_fix_returned_path(protocol, file_info["name"]), size=file_info["size"], type=file_info["type"]
            )
            for file_info in files_info
        ]
    raise FileNotFoundError(f"File or directory not found: {path}")
