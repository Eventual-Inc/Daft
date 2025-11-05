from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.protocols import PrompterDescriptor
    from daft.ai.typing import Options


class VLLMPrefixCachingProvider(Provider):
    """vLLM provider for optimized LLM inference with prefix caching.

    Note:
        This provider is highly experimental and may not work. In addition, the API will likely change (or be removed) in the near future.
    """

    _name: str
    _options: Options

    def __init__(self, name: str | None = None, **options: Any):
        self._name = name if name else "vllm-prefix-caching"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_prompter(self, model: str | None = None, **options: Any) -> PrompterDescriptor:
        from daft.ai.vllm.protocols.prompter import VLLMPrefixCachingPrompterDescriptor

        descriptor_kwargs = {
            "provider_name": self._name,
            **self._options,
            **options,
        }

        if model is not None:
            descriptor_kwargs["model_name"] = model

        return VLLMPrefixCachingPrompterDescriptor(**descriptor_kwargs)
