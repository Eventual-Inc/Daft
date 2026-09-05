"""Carryover removal for Iceberg COW changelog (CDC) reads."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions.window import row_number
from daft.window import Window

if TYPE_CHECKING:
    from daft.dataframe import DataFrame

_METADATA_COLUMNS = ("_change_type", "_change_ordinal", "_commit_snapshot_id")


def remove_carryovers(df: DataFrame) -> DataFrame:
    """Cancel out same-commit DELETE+INSERT pairs of otherwise-identical rows.

    Copy-on-write file rewrites carry along every unchanged row in the rewritten file as a
    (DELETE old-file-row, INSERT new-file-row) pair, even when that specific row never
    actually changed. Rows are partitioned by every non-metadata column plus
    `_change_ordinal` and `_commit_snapshot_id`. Within each partition, DELETE and INSERT
    occurrences are numbered independently and cancelled one-for-one: for a group with 3
    INSERTs and 2 DELETEs, the first 2 INSERTs (by occurrence index) are cancelled against
    the 2 DELETEs, leaving 1 INSERT and 0 DELETEs -- not "keep all 3 INSERTs because
    INSERT count > DELETE count". A group-level-only decision would incorrectly retain all
    three INSERT rows.

    Including both `_change_ordinal` and `_commit_snapshot_id` in the grouping key is
    load-bearing: without them, distinct events from different commits that happen to share
    identical content could be wrongly cancelled against each other.
    """
    data_columns = [c for c in df.column_names if c not in _METADATA_COLUMNS]
    group_key = [*data_columns, "_change_ordinal", "_commit_snapshot_id"]

    # Rows within one (group_key, _change_type) partition are, by construction,
    # byte-identical on every column that matters (all data columns plus the metadata
    # columns used for grouping) -- they're indistinguishable, so the tie-break used to
    # number them doesn't affect correctness; it only needs to be a valid, deterministic
    # ordering expression within a single execution.
    occurrence_window = Window().partition_by(*group_key, "_change_type").order_by(lit(1))
    occurrence_index = row_number().over(occurrence_window)

    count_window = Window().partition_by(*group_key)
    n_insert = (col("_change_type") == lit("INSERT")).cast(DataType.int64()).sum().over(count_window)
    n_delete = (col("_change_type") == lit("DELETE")).cast(DataType.int64()).sum().over(count_window)

    marked = df.with_column("_carryover_occurrence", occurrence_index).with_columns(
        {"_carryover_n_insert": n_insert, "_carryover_n_delete": n_delete}
    )

    keep = ((col("_change_type") == lit("INSERT")) & (col("_carryover_occurrence") > col("_carryover_n_delete"))) | (
        (col("_change_type") == lit("DELETE")) & (col("_carryover_occurrence") > col("_carryover_n_insert"))
    )

    return marked.where(keep).exclude("_carryover_occurrence", "_carryover_n_insert", "_carryover_n_delete")
