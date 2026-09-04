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

_CARRYOVER_INTERNAL_BASENAMES = ("occurrence", "n_insert", "n_delete")


def _fresh_internal_names(existing: set[str], *, prefix: str, basenames: tuple[str, ...]) -> dict[str, str]:
    """Generate internal column names guaranteed not to collide with `existing`.

    A user's Iceberg table is under no obligation to avoid names like `occurrence` or
    `n_insert` -- only the three public `_METADATA_COLUMNS` are reserved. Using plain
    literal names for a post-processing function's internal working columns would silently
    overwrite a same-named user column and then `.exclude()` it away, permanently losing
    its original values. Deterministic (not UUID-based) so tests can assert on the exact
    generated names; the while loop only iterates in the (extremely unlikely) case where an
    entire candidate round collides. `prefix` distinguishes independent callers (e.g.
    `remove_carryovers` vs. a future `net_changes`) sharing this one generic helper, so two
    callers never generate the same candidate name for different purposes.
    """
    suffix = ""
    attempt = 0
    while True:
        candidates = {base: f"__daft_{prefix}_{base}{suffix}" for base in basenames}
        if not (set(candidates.values()) & existing):
            return candidates
        attempt += 1
        suffix = f"_{attempt}"


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
    names = _fresh_internal_names(set(df.column_names), prefix="carryover", basenames=_CARRYOVER_INTERNAL_BASENAMES)

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

    marked = df.with_column(names["occurrence"], occurrence_index).with_columns(
        {names["n_insert"]: n_insert, names["n_delete"]: n_delete}
    )

    keep = ((col("_change_type") == lit("INSERT")) & (col(names["occurrence"]) > col(names["n_delete"]))) | (
        (col("_change_type") == lit("DELETE")) & (col(names["occurrence"]) > col(names["n_insert"]))
    )

    return marked.where(keep).exclude(names["occurrence"], names["n_insert"], names["n_delete"])
