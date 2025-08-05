import os


from daft.ai.model import Model
from daft.ai.typing import Embedding, Provider
from daft.ai.protocols import TextEmbedderLike
from daft.session import current_provider, current_model

__all__ = [
    "Embedding",
    "Model",
    "Provider",
]


def get_default_provider() -> str:
    """Returns the default provider identifier."""
    if provider := current_provider():
        return provider
    return os.environ.get("DAFT_DEFAULT_PROVIDER", "vllm")


def get_default_model() -> str:
    """Returns the default model identifier."""
    if provider := current_model():
        return provider
    return os.environ.get("DAFT_DEFAULT_MODEL", "")
