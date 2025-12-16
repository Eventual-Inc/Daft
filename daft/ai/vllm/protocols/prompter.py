from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

from daft.ai.protocols import Prompter, PrompterDescriptor

if TYPE_CHECKING:
    from daft.ai.typing import Options


class VLLMPromptOptions(TypedDict, total=False):
    concurrency: int
    gpus_per_actor: int
    do_prefix_routing: bool
    max_buffer_size: int
    min_bucket_size: int
    prefix_match_threshold: float
    load_balance_threshold: int
    batch_size: int | None
    engine_args: dict[str, Any]
    generate_args: dict[str, Any]
    num_gpus: int


@dataclass
class VLLMPrefixCachingPrompterDescriptor(PrompterDescriptor):
    """Descriptor for vLLM prompter.

    Note: This descriptor is not actually used to instantiate a Prompter.
    Instead, the prompt() function detects this descriptor type and calls
    PyExpr.vllm() directly for optimized prefix-cached inference.
    """

    provider_name: str
    options: VLLMPromptOptions
    return_format: Any | None = None
    system_message: str | None = None
    model_name: str = "facebook/opt-125m"

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        defaults: VLLMPromptOptions = {
            "concurrency": 1,
            "gpus_per_actor": 1,
            "do_prefix_routing": True,
            "max_buffer_size": 5000,
            "min_bucket_size": 16,
            "prefix_match_threshold": 0.33,
            "load_balance_threshold": 256,
            "batch_size": None,
            "engine_args": {},
            "generate_args": {},
            "num_gpus": 1,
        }
        return {**defaults, **self.options}

    def instantiate(self) -> Prompter:
        """This method should not be called for vLLM provider.

        The prompt() function bypasses the UDF execution path for vLLM
        and calls PyExpr.vllm() directly instead.
        """
        raise NotImplementedError(
            "VLLMPrompterDescriptor.instantiate() should not be called. "
            "The prompt() function uses PyExpr.vllm() directly for vLLM provider."
        )
