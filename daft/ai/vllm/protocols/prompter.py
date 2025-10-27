from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from daft.ai.protocols import Prompter, PrompterDescriptor

if TYPE_CHECKING:
    from daft.ai.typing import Options, UDFOptions


@dataclass
class VLLMPrefixCachedPrompterDescriptor(PrompterDescriptor):
    """Descriptor for vLLM prompter.

    Note: This descriptor is not actually used to instantiate a Prompter.
    Instead, the prompt() function detects this descriptor type and calls
    PyExpr.vllm() directly for optimized prefix-cached inference.
    """

    provider_name: str
    model_name: str = "facebook/opt-125m"
    concurrency: int = 1
    max_buffer_size: int = 1024
    max_running_tasks: int = 512
    batch_size: int | None = None
    engine_args: dict[str, Any] = field(default_factory=dict)
    generate_args: dict[str, Any] = field(default_factory=dict)
    num_gpus: int = 1

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return {
            "concurrency": self.concurrency,
            "max_buffer_size": self.max_buffer_size,
            "max_running_tasks": self.max_running_tasks,
            "batch_size": self.batch_size,
            "engine_args": self.engine_args,
            "generate_args": self.generate_args,
            "num_gpus": self.num_gpus,
        }

    def get_udf_options(self) -> UDFOptions:
        from daft.ai.typing import UDFOptions

        return UDFOptions(concurrency=self.concurrency, num_gpus=self.num_gpus)

    def instantiate(self) -> Prompter:
        """This method should not be called for vLLM provider.

        The prompt() function bypasses the UDF execution path for vLLM
        and calls PyExpr.vllm() directly instead.
        """
        raise NotImplementedError(
            "VLLMPrompterDescriptor.instantiate() should not be called. "
            "The prompt() function uses PyExpr.vllm() directly for vLLM provider."
        )
