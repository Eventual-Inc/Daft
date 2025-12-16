from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from daft.ai.vllm.protocols.prompter import VLLMPromptOptions

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.protocols import PrompterDescriptor
    from daft.ai.typing import Options, PromptOptions


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

    def get_prompter(
        self,
        model: str | None = None,
        return_format: Any | None = None,
        system_message: str | None = None,
        **options: Unpack[PromptOptions],
    ) -> PrompterDescriptor:
        from daft.ai.vllm.protocols.prompter import VLLMPrefixCachingPrompterDescriptor

        # Collect all options
        all_options = {**self._options, **options}
        vllm_options: dict[str, Any] = {
            k: v for k, v in all_options.items() if k in VLLMPromptOptions.__annotations__.keys()
        }

        descriptor_kwargs: dict[str, Any] = {
            "provider_name": self._name,
            "return_format": return_format,
            "system_message": system_message,
            "options": vllm_options,
        }

        if model is not None:
            descriptor_kwargs["model_name"] = model

        return VLLMPrefixCachingPrompterDescriptor(**descriptor_kwargs)
