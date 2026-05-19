from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from daft.api_annotations import PublicAPI
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


@PublicAPI
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
    """Create a DataFrame from a LanceDB table.

    Args:
        uri: The URI of the Lance table to read from. Accepts a local path or an
            object-store URI like "s3://bucket/path".
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        version : optional, int | str
            If specified, load a specific version of the Lance dataset. Else, loads the
            latest version. A version number (`int`) or a tag (`str`) can be provided.
        asof : optional, datetime or str
            If specified, find the latest version created on or earlier than the given
            argument value. If a version is already specified, this arg is ignored.
        block_size : optional, int
            Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock : optional, lance.commit.CommitLock
            A custom commit lock.  Only needed if your object store does not support
            atomic commits.  See the user guide for more details.
        index_cache_size : optional, int
            Index cache size. Index cache is a LRU cache with TTL. This number specifies the
            number of index pages, for example, IVF partitions, to be cached in
            the host memory. Default value is ``256``.

            Roughly, for an ``IVF_PQ`` partition with ``n`` rows, the size of each index
            page equals the combination of the pq code (``np.array([n,pq], dtype=uint8))``
            Approximately, ``n = Total Rows / number of IVF partitions``.
            ``pq = number of PQ sub-vectors``.
        default_scan_options : optional, dict
            Default scan options that are used when scanning the dataset.  This accepts
            the same arguments described in :py:meth:`lance.LanceDataset.scanner`.  The
            arguments will be applied to any scan operation.

            This can be useful to supply defaults for common parameters such as
            ``batch_size``.

            It can also be used to create a view of the dataset that includes meta
            fields such as ``_rowid`` or ``_rowaddr``.  If ``default_scan_options`` is
            provided then the schema returned by :py:meth:`lance.LanceDataset.schema` will
            include these fields if the appropriate scan options are set.
            like this:
            default_scan_options = {"with_row_address": True, "with_row_id" : True,  "batch_size": 1024}
            more see: https://lance-format.github.io/lance-python-doc/dataset.html
        metadata_cache_size_bytes : optional, int
            Size of the metadata cache in bytes. This cache is used to store metadata
            information about the dataset, such as schema and statistics. If not specified,
            a default size will be used.
        fragment_group_size : optional, int
            Number of fragments to group together in a single scan task. If None or <= 1,
            each fragment will be processed individually (default behavior).
        include_fragment_id : Optional, bool
            Whether to display fragment_id.
            if you have the behavior of 'merge_columns_df' or 'write_lance(mode = 'merge')', the `include_fragment_id` must be set to True
        checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
            checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
            already exists in the store are skipped on re-run. Requires the Ray runner.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified LanceDB table

        This function requires the use of [LanceDB](https://lancedb.github.io/lancedb/), which is the Python library for the LanceDB project.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Read a local LanceDB table:
        >>> df = daft.read_lance("/path/to/lance/data/")
        >>> df.show()

        Read a LanceDB table and specify a version:
        >>> df = daft.read_lance("/path/to/lance/data/", version=1)
        >>> df.show()

        Read a LanceDB table with fragment grouping:
        >>> df = daft.read_lance("/path/to/lance/data/", fragment_group_size=5)
        >>> df.show()

        Read a LanceDB table from a public S3 bucket:
        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_lance("s3://daft-oss-public-data/lance/words-test-dataset", io_config=io_config)
        >>> df.show()
    """
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
