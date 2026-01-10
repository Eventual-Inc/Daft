from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import torch

from daft.ai.metrics import record_token_metrics
from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.transformers.protocols.prompter.messages import TransformersPrompterMessageProcessor
from daft.ai.transformers.protocols.prompter.model_loader import TransformersPrompterModelLoader
from daft.ai.transformers.typing import (
    AutoProcessorOptions,
    AutoTokenizerOptions,
    ChatTemplateOptions,
    GenerationConfigOptions,
    ModelLoadingOptions,
)
from daft.ai.utils import extract_options

if TYPE_CHECKING:
    from pydantic import BaseModel


@dataclass
class TransformersPrompterDescriptor(PrompterDescriptor):
    """Descriptor for Transformers prompter.

    Responsibilities:
    - Stores raw options from user
    - Resolves/partitions config in __post_init__ (delegates to ModelLoader)
    - Instantiates Prompter
    """

    provider_name: str
    model_name: str
    system_message: str | None = None
    return_format: type[BaseModel] | None = None
    prompt_options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.model_loading_options = extract_options(self.prompt_options, ModelLoadingOptions)
        self.processor_options = extract_options(self.prompt_options, AutoProcessorOptions)
        self.tokenizer_options = extract_options(self.prompt_options, AutoTokenizerOptions)
        self.chat_template_options = extract_options(self.prompt_options, ChatTemplateOptions)
        self.generation_config_options = extract_options(self.prompt_options, GenerationConfigOptions)
        self.model_class: str = self.prompt_options.pop("model_class", None)

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_system_message(self) -> str | None:
        return self.system_message

    def get_return_format(self) -> type[BaseModel] | None:
        if self.return_format is not None:
            raise ValueError("return_format is not supported for Transformers provider yet.")
        return None

    def get_options(self) -> dict[str, Any]:
        return self.prompt_options

    def get_model_loading_options(self) -> dict[str, Any]:
        return self.model_loading_options

    def get_processor_options(self) -> dict[str, Any]:
        return self.processor_options

    def get_tokenizer_options(self) -> dict[str, Any]:
        return self.tokenizer_options

    def get_chat_template_options(self) -> dict[str, Any]:
        return self.chat_template_options

    def get_generation_config_options(self) -> dict[str, Any]:
        return self.generation_config_options

    def instantiate(self) -> Prompter:
        """Instantiate the prompter.

        Note: This method exists to satisfy the Descriptor protocol.
        The protocol will be refactored in a future PR.
        """
        return TransformersPrompter(self)


class TransformersPrompter(Prompter):
    """Transformers prompter implementation.

    Follows the standard HuggingFace inference pattern:
    1. Build chat messages (system + user content)
    2. Apply chat template + tokenize
    3. Generate
    4. Decode

    Note: All heavy loading happens in __init__ because this class is
    instantiated on distributed workers after the descriptor is serialized.
    """

    def __init__(self, descriptor: TransformersPrompterDescriptor) -> None:
        # Store metadata from descriptor
        self.provider_name = descriptor.get_provider()
        self.model_name = descriptor.get_model()
        self.system_message = descriptor.get_system_message()
        self.return_format = descriptor.get_return_format()
        self.chat_template_options = descriptor.get_chat_template_options()

        # Load model and processor
        self.model_loader = TransformersPrompterModelLoader(descriptor)
        self.model = self.model_loader.create_model()
        self.processor = self.model_loader.create_processor()
        self.generation_config = self.model_loader.create_generation_config()

        # Message processor (follows OpenAI pattern)
        self.message_processor = TransformersPrompterMessageProcessor()

    def _record_usage_metrics(
        self,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
    ) -> None:
        record_token_metrics(
            protocol="prompt",
            model=self.model_name,
            provider=self.provider_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
        )

    def _parse_return_format(self, generated_text: str) -> Any:
        """Parse structured output if return_format is specified."""
        if self.return_format is None:
            return generated_text

        import json

        try:
            parsed = json.loads(generated_text)
            return self.return_format.model_validate(parsed)
        except Exception:
            return generated_text

    # =========================================================================
    # Main Prompt Method (follows OpenAI prompter pattern)
    # =========================================================================

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        """Generate responses for prompt messages.

        Follows the same pattern as OpenAI prompter:
        1. Build messages list with system message + user content
        2. Process through chat template
        3. Generate
        4. Decode and return
        """
        # 1. Build chat messages (Normalization)
        chat_messages = self.message_processor.process_messages(messages, self.system_message)

        # 2. Apply chat template + tokenize (Preprocess)
        # Chat template options with sensible defaults for generation
        template_kwargs: dict[str, Any] = {
            "tokenize": True,
            "add_generation_prompt": True,
            "return_tensors": "pt",
            "return_dict": True,
            **self.chat_template_options,
        }
        inputs = self.processor.apply_chat_template(chat_messages, **template_kwargs)

        # Move inputs to model device
        inputs = {k: v.to(self.model.device) for k, v in inputs.items()}
        input_length = inputs["input_ids"].shape[1]

        # 3. Generate (Inference)
        with torch.inference_mode():
            outputs = self.model.generate(**inputs, generation_config=self.generation_config)

        # 4. Decode (Postprocess)
        generated_ids = outputs[0, input_length:]
        generated_text = self.processor.decode(generated_ids, skip_special_tokens=True)

        # Record metrics
        output_tokens = len(generated_ids)
        self._record_usage_metrics(
            input_tokens=input_length,
            output_tokens=output_tokens,
            total_tokens=input_length + output_tokens,
        )

        return self._parse_return_format(generated_text)
