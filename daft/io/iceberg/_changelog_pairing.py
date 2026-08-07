"""`compute_updates` support for Iceberg COW changelog (CDC) reads."""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.expressions import col, lit
from daft.functions.misc import when
from daft.functions.window import lag, lead
from daft.window import Window

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema

    from daft.dataframe import DataFrame

_RESERVED_COLUMN_NAMES = ("_change_type", "_change_ordinal", "_commit_snapshot_id")


def resolve_identifier_columns(
    identifier_columns: list[str] | None,
    baseline_iceberg_schema: IcebergSchema,
) -> list[str]:
    """Resolve and validate `identifier_columns` against `baseline_iceberg_schema`.

    `None` means "use the schema's declared identifier fields"; an explicit `[]` is a
    distinct, invalid input (compute_updates cannot pair rows without any identifier) --
    these two must not be conflated via Python truthiness (`identifier_columns or default`
    would incorrectly treat an explicit `[]` the same as `None`).
    """
    if identifier_columns is None:
        resolved = sorted(baseline_iceberg_schema.identifier_field_names())
        if not resolved:
            raise ValueError(
                "compute_updates=True requires identifier_columns, or the table must "
                "declare identifier fields in its schema."
            )
    elif identifier_columns == []:
        raise ValueError("compute_updates=True: identifier_columns may not be an empty list.")
    else:
        resolved = identifier_columns

    data_field_names = {f.name for f in baseline_iceberg_schema.fields}

    seen: set[str] = set()
    for name in resolved:
        if "." in name:
            raise ValueError(
                f"compute_updates=True: identifier_columns entry {name!r} looks like a nested "
                "field path; nested identifier fields are not yet supported, only top-level "
                "column names."
            )
        if name in _RESERVED_COLUMN_NAMES:
            raise ValueError(
                f"compute_updates=True: identifier_columns entry {name!r} collides with a "
                f"reserved changelog metadata column name {_RESERVED_COLUMN_NAMES}."
            )
        if name not in data_field_names:
            raise ValueError(
                f"compute_updates=True: identifier_columns entry {name!r} does not exist in this table's schema."
            )
        if name in seen:
            raise ValueError(f"compute_updates=True: identifier_columns contains duplicate entry {name!r}.")
        seen.add(name)

    return resolved


@daft.func(on_error="raise")
def _resolve_change_type(
    change_type: str,
    next_type: str | None,
    prev_type: str | None,
    group_count: int,
) -> str:
    """Fold cardinality/consistency validation and update-pairing into one output column.

    The returned value *is* the final `_change_type` -- validation and the value that
    downstream code actually consumes are the same computation, so there is no
    side-effect-only column that Daft's projection pruning could discard before it runs.
    """
    if change_type not in ("INSERT", "DELETE"):
        raise ValueError(f"unexpected _change_type value: {change_type!r}")
    if group_count > 1:
        raise ValueError(
            "pair_updates requires at most one DELETE and one INSERT per "
            "(identifier_columns, _change_ordinal); identifier_columns may not uniquely "
            "identify a logical row, or remove_carryovers left unresolved duplicate-content "
            "rows."
        )
    if change_type == "DELETE" and next_type == "DELETE":
        raise ValueError(
            "found two consecutive DELETE events for the same identifier with no "
            "intervening INSERT; identifier_columns may not be unique."
        )
    if change_type == "INSERT" and prev_type == "INSERT":
        raise ValueError(
            "found two consecutive INSERT events for the same identifier with no "
            "intervening DELETE; identifier_columns may not be unique."
        )
    if change_type == "DELETE" and next_type == "INSERT":
        return "UPDATE_BEFORE"
    if change_type == "INSERT" and prev_type == "DELETE":
        return "UPDATE_AFTER"
    return change_type


def pair_updates(df: DataFrame, identifier_columns: list[str]) -> DataFrame:
    """Re-mark matched DELETE+INSERT pairs (by identifier) as UPDATE_BEFORE/UPDATE_AFTER.

    Must run *after* `remove_carryovers` -- unresolved same-commit carryover noise sharing
    an identifier would otherwise be misread as a genuine update. Pairing is
    deliberately cross-ordinal: an identifier DELETEd at one commit and re-INSERTed at a
    later one (with no intervening event for that identifier) is paired as a single update,
    matching Iceberg's `ComputeUpdateIterator` semantics.

    `identifier_columns` must be non-empty: an empty list would partition the whole
    DataFrame into a single window, potentially pairing unrelated logical rows together.
    The public API (`resolve_identifier_columns`) already rejects this before it gets
    here, but this function is itself a reusable, module-level entry point -- it must not
    rely solely on an upstream caller to uphold this precondition.
    """
    if not identifier_columns:
        raise ValueError("pair_updates() requires a non-empty identifier_columns.")

    # Same-ordinal DELETE sorts before INSERT, eliminating ties. Illegal `_change_type`
    # values get an arbitrary-but-deterministic rank
    # here; they're rejected by `_resolve_change_type` regardless of where they sort.
    type_rank = (
        when(col("_change_type") == lit("DELETE"), lit(0))
        .when(col("_change_type") == lit("INSERT"), lit(1))
        .otherwise(lit(2))
    )

    order_window = Window().partition_by(*identifier_columns).order_by("_change_ordinal", type_rank)
    next_type = lead(col("_change_type"), 1).over(order_window)
    prev_type = lag(col("_change_type"), 1).over(order_window)

    # Cardinality scope: at most one event of a given type per (identifier, ordinal).
    cardinality_window = Window().partition_by(*identifier_columns, "_change_ordinal", "_change_type")
    group_count = col("_change_type").count().over(cardinality_window)

    new_change_type = _resolve_change_type(col("_change_type"), next_type, prev_type, group_count)
    return df.with_column("_change_type", new_change_type)
