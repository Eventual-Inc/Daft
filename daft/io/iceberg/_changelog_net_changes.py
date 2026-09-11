"""`net_changes` support for Iceberg COW changelog (CDC) reads."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import daft
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions.window import first_value, last_value, row_number
from daft.io.iceberg._changelog_postprocess import _METADATA_COLUMNS, _fresh_internal_names
from daft.window import Window

if TYPE_CHECKING:
    from daft.dataframe import DataFrame

_NET_CHANGES_INTERNAL_BASENAMES = (
    "ordering",
    "signed",
    "type_rank",
    "running",
    "is_reset",
    "resets_including_self",
    "run_id",
    "seg_first_ordinal",
    "seg_first_change_type",
    "seg_first_commit_snapshot_id",
    "seg_last_running",
    "rn",
    "net_resolution",
    "replay_count",
    "marker",
)

_ORDER_KEY_DTYPE = DataType.struct({"signed": DataType.int64(), "type_rank": DataType.int64()})


@daft.func(return_dtype=_ORDER_KEY_DTYPE, on_error="raise")
def _resolve_change_type_ordering(change_type: str) -> dict[str, Any]:
    """Produce `signed`/`type_rank` from the same validated expression, never separately.

    `net_changes` only accepts `_change_type in {"INSERT", "DELETE"}` -- NULL,
    UPDATE_BEFORE/UPDATE_AFTER (compute_updates' output values; the two modes are mutually
    exclusive, but this must not assume that guarantee always holds upstream), or any other
    unknown string must fail here rather than being silently misclassified. `signed` and
    `type_rank` deriving from one function call rules out the two ever drifting out of sync.
    """
    if change_type == "INSERT":
        return {"signed": 1, "type_rank": 1}
    if change_type == "DELETE":
        return {"signed": -1, "type_rank": 0}
    raise ValueError(
        f"net_changes: unexpected _change_type value: {change_type!r}; only 'INSERT' and "
        "'DELETE' are supported (NULL, UPDATE_BEFORE/UPDATE_AFTER, or any other value "
        "indicates upstream data that net_changes cannot process)."
    )


_NET_RESOLUTION_DTYPE = DataType.struct({"change_type": DataType.string(), "replay_count": DataType.int64()})


@daft.func(return_dtype=_NET_RESOLUTION_DTYPE, on_error="raise")
def _resolve_net_change_type(net_count: int, seg_first_change_type: str) -> dict[str, Any]:
    """Return `{change_type, replay_count}`, validating sign/type consistency.

    `replay_count` is always `abs(net_count)` -- a plain `range(net_count)` on a negative
    count (a surviving all-DELETE segment) would silently produce zero replayed rows
    instead of the correct DELETE row(s).
    """
    if net_count > 0:
        if seg_first_change_type != "INSERT":
            raise ValueError(
                f"net_changes: net_count={net_count} is positive but segment's first row "
                f"type is {seg_first_change_type!r}; expected INSERT. This indicates an "
                "algorithm bug (signed-sum sign should always match the segment's "
                "surviving type), not a legitimate data scenario."
            )
        return {"change_type": "INSERT", "replay_count": net_count}
    if net_count < 0:
        if seg_first_change_type != "DELETE":
            raise ValueError(
                f"net_changes: net_count={net_count} is negative but segment's first row "
                f"type is {seg_first_change_type!r}; expected DELETE. This indicates an "
                "algorithm bug, not a legitimate data scenario."
            )
        return {"change_type": "DELETE", "replay_count": abs(net_count)}
    raise ValueError("net_changes: net_count == 0 should have been filtered before this point.")


@daft.func(return_dtype=DataType.list(DataType.int64()))
def _repeat_marker(n: int) -> list[int]:
    return list(range(n))  # n is already abs()'d by this point; never negative here.


def net_changes(df: DataFrame) -> DataFrame:
    """Net-cancel changes across the whole scanned range, replacing `remove_carryovers`.

    Mirrors Iceberg's `RemoveNetCarryoverIterator`: rows are grouped by every non-metadata
    column (not `_change_ordinal`/`_commit_snapshot_id` -- cancellation must span the whole
    range, not just one commit). Within each group, ordered by `(_change_ordinal,
    type_rank)` (DELETE before INSERT within the same ordinal, matching Spark's own
    `sortSpec`), a signed running total (INSERT=+1, DELETE=-1) is tracked; whenever it hits
    zero the segment so far is cancelled and a new one starts. Any segment whose running
    total is still nonzero at the end survives, replayed `abs(net_count)` times using the
    *first* row's metadata. This is a genuinely path-dependent algorithm,
    not a order-independent aggregate, which is why the exact order key matters.

    Called *instead of* `remove_carryovers`, never after it -- same-commit carryover noise
    is naturally absorbed by the running-total reset, no separate handling needed.
    """
    data_columns = [c for c in df.column_names if c not in _METADATA_COLUMNS]
    names = _fresh_internal_names(set(df.column_names), prefix="net_changes", basenames=_NET_CHANGES_INTERNAL_BASENAMES)

    df = df.with_column(names["ordering"], _resolve_change_type_ordering(col("_change_type")))
    df = df.with_column(names["signed"], col(names["ordering"])["signed"])
    df = df.with_column(names["type_rank"], col(names["ordering"])["type_rank"])

    # Step 1: signed cumulative sum.
    running_window = (
        Window()
        .partition_by(*data_columns)
        .order_by("_change_ordinal", names["type_rank"])
        .rows_between(Window.unbounded_preceding, Window.current_row)
    )
    df = df.with_column(names["running"], col(names["signed"]).sum().over(running_window))

    # Step 2: segment (run) boundary detection. Each `.with_column()` here materializes the
    # previous step's window result into a real column before it's used inside another
    # `.over()` call -- Daft doesn't allow inlining one window expression's result directly
    # into another window expression.
    df = df.with_column(names["is_reset"], (col(names["running"]) == lit(0)).cast(DataType.int64()))
    df = df.with_column(names["resets_including_self"], col(names["is_reset"]).sum().over(running_window))
    df = df.with_column(names["run_id"], col(names["resets_including_self"]) - col(names["is_reset"]))

    # Step 3: pick each surviving segment's first-row metadata and last (net) running value,
    # via explicit-frame first_value/last_value + row_number, not groupby().agg().first()/
    # .last() (whose ordering guarantees aren't part of Daft's documented contract).
    seg_window = (
        Window()
        .partition_by(*data_columns, names["run_id"])
        .order_by("_change_ordinal", names["type_rank"])
        .rows_between(Window.unbounded_preceding, Window.unbounded_following)
    )
    df = df.with_column(names["seg_first_ordinal"], first_value(col("_change_ordinal")).over(seg_window))
    df = df.with_column(names["seg_first_change_type"], first_value(col("_change_type")).over(seg_window))
    df = df.with_column(names["seg_first_commit_snapshot_id"], first_value(col("_commit_snapshot_id")).over(seg_window))
    df = df.with_column(names["seg_last_running"], last_value(col(names["running"])).over(seg_window))

    rn_window = Window().partition_by(*data_columns, names["run_id"]).order_by("_change_ordinal", names["type_rank"])
    df = df.with_column(names["rn"], row_number().over(rn_window))

    survivors = df.where((col(names["rn"]) == lit(1)) & (col(names["seg_last_running"]) != lit(0)))

    # Step 4: replay each surviving segment abs(net_count) times with its first row's
    # metadata.
    result = (
        survivors.with_column(
            names["net_resolution"],
            _resolve_net_change_type(col(names["seg_last_running"]), col(names["seg_first_change_type"])),
        )
        .with_column("_change_type", col(names["net_resolution"])["change_type"])
        .with_column(names["replay_count"], col(names["net_resolution"])["replay_count"])
        .with_column("_change_ordinal", col(names["seg_first_ordinal"]))
        .with_column("_commit_snapshot_id", col(names["seg_first_commit_snapshot_id"]))
        .with_column(names["marker"], _repeat_marker(col(names["replay_count"])))
        .explode(names["marker"])
    )

    return result.exclude(*names.values())
