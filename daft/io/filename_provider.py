from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class FilenameProvider(ABC):
    """Strategy interface for generating filenames during writes.

    Implement this interface to customize how Daft names output files when writing
    tabular data or uploading via :func:`daft.functions.upload`.

    Implement :meth:`get_filename_for_block` when writing block-oriented outputs
    (for example, :meth:`daft.DataFrame.write_parquet` and
    :meth:`daft.DataFrame.write_csv`). Implement :meth:`get_filename_for_row`
    when generating per-row filenames (for example,
    :func:`daft.functions.upload`).

    Filenames **must** be deterministic and unique for a given combination of
    ``write_uuid`` and index parameters.
    """

    @abstractmethod
    def get_filename_for_block(
        self,
        write_uuid: str,
        task_index: int,
        block_index: int,
        file_idx: int,
        ext: str,
    ) -> str:
        """Generate a filename for a block-oriented output file.

        Args:
            write_uuid: Identifier for the logical write operation. A single call
                to :meth:`daft.DataFrame.write_parquet`,
                :meth:`daft.DataFrame.write_csv`, or
                :func:`daft.functions.upload` shares the same ``write_uuid``.
            task_index: Index of the write task within the job.
            block_index: Index of the block *within* the task.
            file_idx: Monotonically increasing index for files within the task.
            ext: Suggested file extension (for example, ``"parquet"`` or
                ``"csv"``).

        Returns:
            The basename of the output file, **without** any directory
            components.
        """

    @abstractmethod
    def get_filename_for_row(
        self,
        row: dict[str, Any],
        write_uuid: str,
        task_index: int,
        block_index: int,
        row_index: int,
        ext: str,
    ) -> str:
        """Generate a filename for a single-row output.

        Args:
            row: A mapping representing the row being written. Implementations
                should treat this as read-only.
            write_uuid: Identifier for the logical write operation.
            task_index: Index of the write task within the job.
            block_index: Index of the block *within* the task.
            row_index: Index of the row *within* the block.
            ext: Suggested file extension. For URL uploads this may be an empty
                string when Daft does not know the desired extension.

        Returns:
            The basename of the output file, **without** any directory
            components.
        """


class _DefaultFilenameProvider(FilenameProvider):
    """Default filename provider used by Daft when none is supplied.

    The pattern is::

        <write_uuid>_<task_index>_<block_index>_<row_or_file_index>.<ext>

    where indices are zero-padded to six digits. This mirrors Ray Data's
    default behaviour and greatly reduces the chance of collisions when
    writing concurrently.
    """

    def get_filename_for_block(
        self,
        write_uuid: str,
        task_index: int,
        block_index: int,
        file_idx: int,
        ext: str,
    ) -> str:  # pragma: no cover - exercised via higher-level IO tests
        base = f"{write_uuid}_{task_index:06d}_{block_index:06d}_{file_idx:06d}"
        return f"{base}.{ext}" if ext else base

    def get_filename_for_row(
        self,
        row: dict[str, Any],
        write_uuid: str,
        task_index: int,
        block_index: int,
        row_index: int,
        ext: str,
    ) -> str:  # pragma: no cover - exercised via higher-level IO tests
        base = f"{write_uuid}_{task_index:06d}_{block_index:06d}_{row_index:06d}"
        return f"{base}.{ext}" if ext else base
