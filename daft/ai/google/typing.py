from __future__ import annotations

from typing import Any, TypedDict


class GoogleProviderOptions(TypedDict, total=False):
    """These are Google GenAI client constructor parameters."""

    api_key: str | None
    vertexai: bool | None
    project: str | None
    location: str | None
    http_options: dict[str, Any] | None
