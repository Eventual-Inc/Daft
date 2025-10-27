from __future__ import annotations

from typing import TYPE_CHECKING

from daft.ai.openai.typing import DEFAULT_OPENAI_BASE_URL

if TYPE_CHECKING:
    from openai import OpenAI


def normalize_model_name(model_name: str, base_url: str = DEFAULT_OPENAI_BASE_URL) -> str:
    """Normalize model name fallback for OpenAI."""
    # Strip "openai/" prefix only for default OpenAI base_url
    if base_url == DEFAULT_OPENAI_BASE_URL and model_name.startswith("openai/"):
        return model_name[7:]  # Remove "openai/" (7 characters)

    return model_name


def validate_model_availability(client: OpenAI, model_name: str) -> bool:
    """Validate model is listed in the client's model list."""
    try:
        available_models = client.models.list()
        return model_name in [model.id for model in available_models]
    except Exception as e:
        raise ValueError(f"Error listing models for OpenAI client at {client.base_url}: {e}")
