from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

if TYPE_CHECKING:
    from collections.abc import Callable

    from daft.ai.google.typing import GoogleProviderOptions
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import (
        ImageClassifierDescriptor,
        ImageEmbedderDescriptor,
        PrompterDescriptor,
        TextClassifierDescriptor,
        TextEmbedder,
        TextEmbedderDescriptor,
    )
    from daft.ai.typing import (
        ClassifyImageOptions,
        ClassifyTextOptions,
        EmbedImageOptions,
        EmbedTextOptions,
        PromptOptions,
        UDFOptions,
    )
    from daft.expressions import Expression


class ProviderImportError(ImportError):
    def __init__(self, extra: str, *, function: str | None = None):
        function_msg = f" to use the {function} function" if function is not None else ""
        super().__init__(f"Please `pip install 'daft[{extra}]'`{function_msg} with this provider.")


def load_google(name: str | None = None, **options: Unpack[GoogleProviderOptions]) -> Provider:
    try:
        from daft.ai.google.provider import GoogleProvider

        return GoogleProvider(name, **options)
    except ImportError as e:
        raise ProviderImportError("google") from e


def load_lm_studio(name: str | None = None, **options: Unpack[OpenAIProviderOptions]) -> Provider:
    try:
        from daft.ai.lm_studio.provider import LMStudioProvider

        return LMStudioProvider(name, **options)
    except ImportError as e:
        raise ProviderImportError("openai") from e


def load_openai(name: str | None = None, **options: Unpack[OpenAIProviderOptions]) -> Provider:
    try:
        from daft.ai.openai.provider import OpenAIProvider

        return OpenAIProvider(name, **options)
    except ImportError as e:
        raise ProviderImportError("openai") from e


def load_transformers(name: str | None = None, **options: Any) -> Provider:
    try:
        from daft.ai.transformers.provider import TransformersProvider

        return TransformersProvider(name, **options)
    except ImportError as e:
        raise ProviderImportError("transformers") from e


def load_vllm_prefix_caching(name: str | None = None, **options: Any) -> Provider:
    try:
        from daft.ai.vllm.provider import VLLMPrefixCachingProvider

        return VLLMPrefixCachingProvider(name, **options)
    except ImportError as e:
        raise ProviderImportError("vllm") from e


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

    Each function (e.g. embed_text) follows a three-method pattern:

    1. ``get_<fn>_descriptor`` — validates options, returns a serializable descriptor.
    2. ``get_<fn>`` — returns an implementation that adheres to the protocol.
    3. ``<fn>`` — builds and returns a Daft Expression (default uses 1 + 2).

    Providers must implement (1). They may override (2) and (3) for custom behavior.
    The default (3) builds a sync class-based UDF using the protocol implementation.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the provider's name."""
        ...

    def _get_udf_options(self, descriptor: TextEmbedderDescriptor) -> UDFOptions:
        """Extracts UDF configuration from a descriptor. Override for custom behavior."""
        from daft.ai.typing import UDFOptions as UDFOptionsCls
        from daft.utils import from_dict

        return from_dict(UDFOptionsCls, dict(descriptor.get("embed_options", {})))

    # ------------------------------------------------------------------
    # embed_text
    # ------------------------------------------------------------------
    # 1. We must be able to introspect and serialize our resolved function info (descriptor).
    # 2. We must be able to instantiate an expression from this serializable info (descriptor).
    # 3. Implementations should adhere to an interface, but are not required to (protocol).
    # 4. We must give providers control on how their expression is instantiated (custom expressions).
    # ------------------------------------------------------------------

    def get_text_embedder_descriptor(
        self, model: str | None = None, dimensions: int | None = None, **options: Unpack[EmbedTextOptions]
    ) -> TextEmbedderDescriptor:
        """Validates options and returns a serializable TextEmbedderDescriptor."""
        raise not_implemented_err(self, method="embed_text")

    def get_text_embedder(self, descriptor: TextEmbedderDescriptor) -> TextEmbedder:
        """Returns a TextEmbedder implementation from the descriptor."""
        raise not_implemented_err(self, method="embed_text")

    def embed_text(
        self,
        text: Expression,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> Expression:
        """Returns an Expression that embeds text using this provider.

        The default builds a synchronous class-based UDF from the descriptor
        and protocol implementation. Providers should override this for async
        implementations or custom expression behavior.
        """
        from daft.ai._expressions import _TextEmbedderExpression
        from daft.udf import cls as daft_cls
        from daft.udf import method

        descriptor = self.get_text_embedder_descriptor(model, dimensions, **options)
        udf_options = self._get_udf_options(descriptor)

        _TextEmbedderExpression.__call__ = method.batch(  # type: ignore[method-assign]
            method=_TextEmbedderExpression._call_sync,
            return_dtype=descriptor["dimensions"].as_dtype(),
            batch_size=udf_options.batch_size,
        )
        wrapped_cls = daft_cls(
            _TextEmbedderExpression,
            max_concurrency=udf_options.concurrency,
            gpus=udf_options.num_gpus or 0,
            max_retries=udf_options.max_retries,
            on_error=udf_options.on_error,
            name_override="embed_text",
        )

        return wrapped_cls(descriptor)(text)

    # ------------------------------------------------------------------
    # embed_image
    # ------------------------------------------------------------------

    def get_image_embedder(
        self, model: str | None = None, **options: Unpack[EmbedImageOptions]
    ) -> ImageEmbedderDescriptor:
        """Returns an ImageEmbedderDescriptor for this provider."""
        raise not_implemented_err(self, method="embed_image")

    # ------------------------------------------------------------------
    # classify_image
    # ------------------------------------------------------------------

    def get_image_classifier(
        self, model: str | None = None, **options: Unpack[ClassifyImageOptions]
    ) -> ImageClassifierDescriptor:
        """Returns an ImageClassifierDescriptor for this provider."""
        raise not_implemented_err(self, method="classify_image")

    # ------------------------------------------------------------------
    # classify_text
    # ------------------------------------------------------------------

    def get_text_classifier(
        self, model: str | None = None, **options: Unpack[ClassifyTextOptions]
    ) -> TextClassifierDescriptor:
        """Returns a TextClassifierDescriptor for this provider."""
        raise not_implemented_err(self, method="classify_text")

    # ------------------------------------------------------------------
    # prompt
    # ------------------------------------------------------------------

    def get_prompter(
        self,
        model: str | None = None,
        return_format: Any | None = None,
        system_message: str | None = None,
        **options: Unpack[PromptOptions],
    ) -> PrompterDescriptor:
        """Returns a PrompterDescriptor for this provider."""
        raise not_implemented_err(self, method="prompt")
