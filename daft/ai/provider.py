from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Literal

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

if TYPE_CHECKING:
    from daft.ai.google.typing import GoogleProviderOptions
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import (
        ImageClassifierDescriptor,
        ImageEmbedderDescriptor,
        PrompterDescriptor,
        TextClassifierDescriptor,
        TextEmbedderDescriptor,
    )


class ProviderImportError(ImportError):
    def __init__(self, tag: str, dependencies: list[str]):
        deps = ", ".join(f"'{d}'" for d in dependencies)
        super().__init__(
            f"Missing required dependencies: {deps}. Please `pip install 'daft[{tag}]'` to use this provider."
        )


def load_google(name: str | None = None, **options: Unpack[GoogleProviderOptions]) -> Provider:
    try:
        from daft.ai.google.provider import GoogleProvider
    except ImportError as e:
        raise ProviderImportError("google", ["google-genai", "numpy", "Pillow"]) from e

    return GoogleProvider(name, **options)


def load_lm_studio(name: str | None = None, **options: Any) -> Provider:
    try:
        from daft.ai.lm_studio.provider import LMStudioProvider
    except ImportError as e:
        raise ProviderImportError("openai", ["openai", "numpy", "Pillow"]) from e

    return LMStudioProvider(name, **options)


def load_openai(name: str | None = None, **options: Unpack[OpenAIProviderOptions]) -> Provider:
    try:
        from daft.ai.openai.provider import OpenAIProvider
    except ImportError as e:
        raise ProviderImportError("openai", ["openai", "numpy", "Pillow"]) from e

    return OpenAIProvider(name, **options)


def load_transformers(name: str | None = None, **options: Any) -> Provider:
    try:
        from daft.ai.transformers.provider import TransformersProvider
    except ImportError as e:
        raise ProviderImportError(
            "transformers", ["torch", "torchvision", "transformers", "sentence-transformers", "Pillow"]
        ) from e

    return TransformersProvider(name, **options)


def load_vllm_prefix_caching(name: str | None = None, **options: Any) -> Provider:
    try:
        from daft.ai.vllm.provider import VLLMPrefixCachingProvider
    except ImportError as e:
        raise ProviderImportError("vllm", ["vllm"]) from e

    return VLLMPrefixCachingProvider(name, **options)


ProviderType = Literal["google", "lm_studio", "openai", "transformers", "vllm-prefix-caching"]
PROVIDERS: dict[ProviderType, Callable[..., Provider]] = {
    "google": load_google,
    "lm_studio": load_lm_studio,
    "openai": load_openai,
    "transformers": load_transformers,
    "vllm-prefix-caching": load_vllm_prefix_caching,
}


def load_provider(provider: str, name: str | None = None, **options: Any) -> Provider:
    if provider not in PROVIDERS:
        raise ValueError(f"Provider '{provider}' is not yet supported.")
    return PROVIDERS[provider](name, **options)  # type: ignore


def not_implemented_err(provider: Provider, method: str) -> NotImplementedError:
    return NotImplementedError(f"{method} is not currently implemented for the '{provider.name}' provider")


class Provider(ABC):
    """Provider is the base class for resolving implementations for the various AI/ML protocols.

    Handles integration with model providers such as OpenAI, LM Studio,
    Hugging Face Transformers, etc. Provides a unified interface for model access
    and execution regardless of the underlying implementation.

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

    def get_text_embedder(
        self, model: str | None = None, dimensions: int | None = None, **options: Any
    ) -> TextEmbedderDescriptor:
        """Returns a TextEmbedderDescriptor for this provider."""
        raise not_implemented_err(self, method="embed_text")

    def get_image_embedder(self, model: str | None = None, **options: Any) -> ImageEmbedderDescriptor:
        """Returns an ImageEmbedderDescriptor for this provider."""
        raise not_implemented_err(self, method="embed_image")

    def get_image_classifier(self, model: str | None = None, **options: Any) -> ImageClassifierDescriptor:
        """Returns an ImageClassifierDescriptor for this provider."""
        raise not_implemented_err(self, method="classify_image")

    def get_text_classifier(self, model: str | None = None, **options: Any) -> TextClassifierDescriptor:
        """Returns a TextClassifierDescriptor for this provider."""
        raise not_implemented_err(self, method="classify_text")

    def get_prompter(self, model: str | None = None, **options: Any) -> PrompterDescriptor:
        """Returns a PrompterDescriptor for this provider."""
        raise not_implemented_err(self, method="prompt")
