from __future__ import annotations

import os
import sys


def emit_query_id(query_id: str) -> None:
    """Emit the query ID to stderr so users can correlate runs with diagnostics."""
    if os.environ.get("DAFT_SHOW_QUERY_ID", "1") == "0":
        return
    print(f"Daft Query ID: {query_id}", file=sys.stderr)
