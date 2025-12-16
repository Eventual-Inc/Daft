from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from google.auth.credentials import Credentials
    from google.genai.client import DebugConfig
    from google.genai.types import HttpOptionsOrDict


class GoogleProviderOptions(TypedDict, total=False):
    """These are Google GenAI client constructor parameters."""

    vertexai: bool | None
    api_key: str | None
    credentials: Credentials | None
    project: str | None
    location: str | None
    debug_config: DebugConfig | None
    http_options: HttpOptionsOrDict | None
