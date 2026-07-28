"""MCAP file metadata expressions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft import Expression


def _mcap_summary_projection(file_expr: Expression) -> Expression:
    # Keep the inexpensive helpers as projections of one identical expression.
    # Daft factors that common subexpression when several helpers share a select.
    from daft.expressions import Expression

    return Expression._call_builtin_scalar_fn("_mcap_summary_projection", file_expr)


def mcap_topics(file_expr: Expression) -> Expression:
    """Return topics declared by the channels in an MCAP summary."""
    return _mcap_summary_projection(file_expr).get("topics")


def mcap_message_count(file_expr: Expression) -> Expression:
    """Return the MCAP summary message count, or null without statistics."""
    return _mcap_summary_projection(file_expr).get("message_count")


def mcap_time_range(file_expr: Expression) -> Expression:
    """Return inclusive MCAP log-time bounds, or null bounds for no messages."""
    return _mcap_summary_projection(file_expr).get("time_range")
