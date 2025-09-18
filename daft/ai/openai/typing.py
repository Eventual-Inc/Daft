from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from collections.abc import Mapping


class OpenAIProviderOptions(TypedDict, total=False):
    """These are OpenAI client constructor parameters."""

    api_key: str | None
    organization: str | None
    project: str | None
    webhook_secret: str | None
    base_url: str | None
    websocket_base_url: str | None
    timeout: float | None
    max_retries: int
    default_headers: Mapping[str, str] | None
    default_query: Mapping[str, object] | None
