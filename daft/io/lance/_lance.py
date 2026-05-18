from __future__ import annotations

import warnings
from typing import Any

from daft_lance._lance import read_lance

__all__ = [
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "read_lance",
]


def merge_columns(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.merge_columns is deprecated and will be removed in a future release. "
        "Please use daft_lance.merge_columns from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    from daft_lance._lance import merge_columns as _impl

    return _impl(*args, **kwargs)


def merge_columns_df(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.merge_columns_df is deprecated and will be removed in a future release. "
        "Please use daft_lance.merge_columns_df from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    from daft_lance._lance import merge_columns_df as _impl

    return _impl(*args, **kwargs)


def create_scalar_index(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.create_scalar_index is deprecated and will be removed in a future release. "
        "Please use daft_lance.create_scalar_index from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    from daft_lance._lance import create_scalar_index as _impl

    return _impl(*args, **kwargs)


def compact_files(*args: Any, **kwargs: Any) -> Any:
    warnings.warn(
        "daft.io.lance.compact_files is deprecated and will be removed in a future release. "
        "Please use daft_lance.compact_files from the daft-lance package instead: pip install daft-lance",
        category=DeprecationWarning,
        stacklevel=2,
    )
    from daft_lance._lance import compact_files as _impl

    return _impl(*args, **kwargs)
