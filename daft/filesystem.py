from enum import Enum
from typing import List
from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.implementations.dirfs import DirFileSystem


def get_filesystem(protocol: str, **kwargs) -> AbstractFileSystem:
    klass = get_filesystem_class(protocol)
    fs = klass(**kwargs)
    return fs

