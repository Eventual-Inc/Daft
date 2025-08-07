from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedderDescriptor
    from daft.ai.typing import Options


class ProviderImportError(ImportError):
    def __init__(self, dependencies: list[str]):
        deps = ", ".join(f"'{d}'" for d in dependencies)
        super().__init__(f"Missing required dependencies: {deps}. " f"Please install {deps} to use this provider.")


def load_sentence_transformers(options: Options) -> Provider:
    try:
        from daft.ai.sentence_transformers import SentenceTransformersProvider

        return SentenceTransformersProvider(**options)
    except ImportError as e:
        raise ProviderImportError(["sentence_transformers", "torch"]) from e


PROVIDERS: dict[str, Callable[[Options], Provider]] = {
    "sentence_transformers": load_sentence_transformers,
}


def load_provider(provider: str, **options: str) -> Provider:
    if provider not in PROVIDERS:
        raise ValueError(f"Provider '{provider}' is not yet supported.")
    return PROVIDERS[provider](options)


class Provider(ABC):
    """Provider is the base class for resolving implementations for the various AI/ML protocols.

    Note:
        We will need to move instantiation from the TextEmbedderDesriptor to the Provider or other.
        It is not set at the moment, and instantiation directly from the descriptor is the easiest.
        We could opt to include a factory method location (descriptor's init) in the serialization.
    """

    @abstractmethod
    def get_text_embedder(self, model: str | None = None, **options: str) -> TextEmbedderDescriptor:
        """Returns a TextEmbedderDescriptor for this provider."""
        ...
