from __future__ import annotations

import warnings
from typing import Any

from daft.lazy_import import LazyImport

_daft_lance = LazyImport("daft_lance._lance")

__all__ = [
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "read_lance",
]


def read_lance(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.read_lance is deprecated and will be removed in a future release. "
        "Please use daft_lance.read_lance from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return _daft_lance.read_lance(*args, **kwargs)


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
