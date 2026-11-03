from __future__ import annotations

from typing import TypedDict


class ParallelProviderOptions(TypedDict, total=False):
    """Options for the Parallel Decisions client."""

    api_key: str | None
    base_url: str | None
    timeout: float
