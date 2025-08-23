from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Unpack

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import TextEmbedderDescriptor


class ProviderImportError(ImportError):
    def __init__(self, dependencies: list[str]):
        deps = ", ".join(f"'{d}'" for d in dependencies)
        super().__init__(f"Missing required dependencies: {deps}. " f"Please install {deps} to use this provider.")


def load_openai(name: str | None = None, **options: Unpack[OpenAIProviderOptions]) -> Provider:
    try:
        from daft.ai.openai import OpenAIProvider

        return OpenAIProvider(name, **options)
    except ImportError as e:
        raise ProviderImportError(["openai"]) from e


def load_sentence_transformers(name: str | None = None, **options: Any) -> Provider:
    try:
        from daft.ai.sentence_transformers import SentenceTransformersProvider

        return SentenceTransformersProvider(name, **options)
    except ImportError as e:
        raise ProviderImportError(["sentence_transformers", "torch"]) from e


PROVIDERS: dict[str, Callable[..., Provider]] = {
    "openai": load_openai,
    "sentence_transformers": load_sentence_transformers,
}


def load_provider(provider: str, name: str | None = None, **options: Any) -> Provider:
    if provider not in PROVIDERS:
        raise ValueError(f"Provider '{provider}' is not yet supported.")
    return PROVIDERS[provider](name, **options)


class Provider(ABC):
    """Provider is the base class for resolving implementations for the various AI/ML protocols.

    Note:
        We will need to move instantiation from the TextEmbedderDesriptor to the Provider or other.
        It is not set at the moment, and instantiation directly from the descriptor is the easiest.
        We could opt to include a factory method location (descriptor's init) in the serialization.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the provider's name."""
        ...

    @abstractmethod
    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        """Returns a TextEmbedderDescriptor for this provider."""
        ...
