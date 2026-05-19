from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from daft.lazy_import import LazyImport

if TYPE_CHECKING:
    import os

_daft_lance = LazyImport("daft_lance._lance")

__all__ = [
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "read_lance",
]


def read_lance(
    uri: str | os.PathLike[str],
    io_config: Any = None,
    version: Any = None,
    asof: Any = None,
    block_size: Any = None,
    commit_lock: Any = None,
    index_cache_size: Any = None,
    default_scan_options: Any = None,
    metadata_cache_size_bytes: Any = None,
    fragment_group_size: Any = None,
    include_fragment_id: Any = None,
    checkpoint: Any = None,
) -> Any:
    warnings.warn(
        "daft.read_lance is deprecated and will be removed in a future release. "
        "Please use daft_lance.read_lance from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return _daft_lance.read_lance(
        uri,
        io_config=io_config,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
        fragment_group_size=fragment_group_size,
        include_fragment_id=include_fragment_id,
        checkpoint=checkpoint,
    )


def merge_columns(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.merge_columns is deprecated and will be removed in a future release. "
        "Please use daft_lance.merge_columns from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return _daft_lance.merge_columns(*args, **kwargs)


def merge_columns_df(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.merge_columns_df is deprecated and will be removed in a future release. "
        "Please use daft_lance.merge_columns_df from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return _daft_lance.merge_columns_df(*args, **kwargs)


def create_scalar_index(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.create_scalar_index is deprecated and will be removed in a future release. "
        "Please use daft_lance.create_scalar_index from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return _daft_lance.create_scalar_index(*args, **kwargs)


def compact_files(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.compact_files is deprecated and will be removed in a future release. "
        "Please use daft_lance.compact_files from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return _daft_lance.compact_files(*args, **kwargs)
