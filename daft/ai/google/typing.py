from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias, TypedDict

if TYPE_CHECKING:
    from google.auth.credentials import Credentials
    from google.genai.types import (
        EmbedContentConfigDict,
        EmbedContentResponseDict,
        GenerateContentConfigDict,
        HttpOptionsOrDict,
    )
else:
    # Keep this module importable without google-genai installed. Providers will validate
    # dependencies before use; these are only for typing / external call signatures.
    Credentials: TypeAlias = Any
    HttpOptionsOrDict: TypeAlias = Any
    EmbedContentConfigDict: TypeAlias = dict[str, Any]
    GenerateContentConfigDict: TypeAlias = dict[str, Any]
    EmbedContentResponseDict: TypeAlias = dict[str, Any]


class GoogleProviderOptions(TypedDict, total=False):
    """These are Google GenAI client constructor parameters."""

    vertexai: bool | None
    api_key: str | None
    credentials: Credentials | None
    project: str | None
    location: str | None
    http_options: HttpOptionsOrDict | None


# Use Google's types for our own.
GoogleEmbedContentConfig = EmbedContentConfigDict
GoogleEmbedContentResponse = EmbedContentResponseDict
GoogleGenerationContentConfig = GenerateContentConfigDict
