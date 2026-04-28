from __future__ import annotations

import os
import sys


def _parse_show_query_id_env(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


def emit_query_id(query_id: str) -> None:
    """Emit the query ID to stderr so users can correlate runs with diagnostics."""
    show_env = os.environ.get("DAFT_SHOW_QUERY_ID")
    if show_env is None:
        show = sys.stderr.isatty()
    else:
        parsed_show = _parse_show_query_id_env(show_env)
        # If explicitly configured but unrecognized (e.g. empty string), default to disabled.
        show = parsed_show if parsed_show is not None else False

    if not show:
        return
    print(f"Daft Query ID: {query_id}", file=sys.stderr)
