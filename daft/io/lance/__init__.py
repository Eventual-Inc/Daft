try:
    import daft_lance
except ImportError as e:
    raise ImportError("daft-lance is required for daft.io.lance. Install with: pip install 'daft[lance]'") from e

from daft.io.lance._lance import (
    compact_files,
    create_scalar_index,
    merge_columns,
    merge_columns_df,
    read_lance,
)

__all__ = [
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "read_lance",
]
