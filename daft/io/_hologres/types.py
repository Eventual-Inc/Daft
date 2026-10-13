"""Shared enums for Hologres COPY operations.

Adapted from holo-client-py (hologres/types.py, hologres/copy.py).
"""

from __future__ import annotations

from enum import Enum


class OnConflictAction(Enum):
    """Conflict resolution strategy for tables with primary keys."""

    NONE = "none"
    INSERT_OR_IGNORE = "ignore"
    INSERT_OR_UPDATE = "update"
    INSERT_OR_REPLACE = "replace"


class CopyFormat(Enum):
    """Data format for COPY operations."""

    TEXT = "text"
    BINARY = "binary"


class CopyMode(Enum):
    """COPY write mode.

    - STREAM: Hologres streaming (fixed plan) mode with row-level locks;
      supports on_conflict; works on all versions; safe for concurrent writers.
    - BULK_LOAD: standard PostgreSQL COPY path with a table-level lock;
      highest throughput; on_conflict requires Hologres >= 3.1.
    """

    STREAM = "stream"
    BULK_LOAD = "bulk_load"
