from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import torch

from daft.ai.metrics import record_token_metrics
from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.transformers.protocols.prompter.messages import ChatTemplateFormatter
from daft.ai.transformers.protocols.prompter.model_loader import (
    PartitionedConfig,
    TransformersPrompterModelLoader,
)
from daft.ai.transformers.typing import UDF_KEYS
from daft.ai.typing import UDFOptions
from daft.ai.utils import get_gpu_udf_options, get_torch_device

if TYPE_CHECKING:
    from pydantic import BaseModel

    from daft.ai.typing import Options


# =============================================================================
# Descriptor (Config Resolution)
# =============================================================================


@dataclass
class TransformersPrompterDescriptor(PrompterDescriptor):
    """Descriptor for Transformers prompter.

    Responsibilities:
    - Stores raw options from user
    - Resolves/partitions config in __post_init__ (delegates to ModelLoader)
    - Instantiates Prompter
    """

    # === Raw inputs (from Provider.get_prompter) ===
    provider_name: str
    model_name: str
    prompt_options: dict[str, Any] = field(default_factory=dict)
    system_message: str | None = None
    return_format: type[BaseModel] | None = None

    # === Resolved config (set in __post_init__, serializable) ===
    _partitioned_config: PartitionedConfig | None = field(default=None, init=False, repr=False)
    _udf_options: UDFOptions = field(default_factory=UDFOptions, init=False, repr=False)

    def __post_init__(self) -> None:
        """Resolve all configuration. No object instantiation yet."""
        self._resolve_config()

    def _resolve_config(self) -> None:
        """Partition raw options into HF-specific and UDF config.

        Delegates HF-specific partitioning to ModelLoader (domain layer).
        """
        opts = dict(self.prompt_options)

        # Extract UDF options first (Daft-specific)
        udf_dict: dict[str, Any] = {"max_retries": 0, "on_error": "raise"}  # Local models don't retry
        for key in list(opts.keys()):
            if key in UDF_KEYS:
                udf_dict[key] = opts.pop(key)

        # Delegate HF-specific partitioning to ModelLoader
        self._partitioned_config = TransformersPrompterModelLoader.partition_options(opts)

        # Store UDF options
        self._udf_options = UDFOptions(
            concurrency=udf_dict.get("concurrency"),
            num_gpus=udf_dict.get("num_gpus"),
            max_retries=udf_dict.get("max_retries", 0),
            on_error=udf_dict.get("on_error", "raise"),
            batch_size=udf_dict.get("batch_size"),
        )

    # =========================================================================
    # Descriptor Protocol Implementation
    # =========================================================================

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        """Return merged options for logging/debugging."""
        if self._partitioned_config is None:
            return {}
        return {
            **self._partitioned_config.model_kwargs,
            **self._partitioned_config.generation_kwargs,
        }

    def get_udf_options(self) -> UDFOptions:
        """Return resolved UDF options.

        For transformers, we also apply GPU-based concurrency settings.
        """
        gpu_options = get_gpu_udf_options()

        return UDFOptions(
            concurrency=self._udf_options.concurrency or gpu_options.concurrency,
            num_gpus=self._udf_options.num_gpus or gpu_options.num_gpus,
            max_retries=self._udf_options.max_retries,
            on_error=self._udf_options.on_error,
            batch_size=self._udf_options.batch_size,
        )

    def instantiate(self) -> Prompter:
        """Create a Prompter instance.

        Note: Heavy loading (model, processor) happens in Prompter.__init__
        because this runs on distributed workers after serialization.
        """
        return TransformersPrompter(self)


# =============================================================================
# Prompter (Orchestrator)
# =============================================================================


class TransformersPrompter(Prompter):
    """Transformers prompter implementation.

    Follows the standard HuggingFace inference pattern:
    1. Format messages (via ChatTemplateFormatter)
    2. Apply chat template + tokenize
    3. Generate
    4. Decode

    Note: All heavy loading happens in __init__ because this class is
    instantiated on distributed workers after the descriptor is serialized.
    """

    def __init__(self, descriptor: TransformersPrompterDescriptor) -> None:
        # Store metadata from descriptor
        self.provider_name = descriptor.provider_name
        self.model_name = descriptor.model_name
        self.system_message = descriptor.system_message
        self.return_format = descriptor.return_format

        if descriptor._partitioned_config is None:
            raise ValueError("Descriptor config not resolved - call _resolve_config() first")

        # === Load model and processor (runs on worker) ===
        loader = TransformersPrompterModelLoader(
            model_name=descriptor.model_name,
            config=descriptor._partitioned_config,
        )

        self.device = get_torch_device()
        self.supported_modalities = loader.detect_supported_modalities()
        self.model = loader.create_model(self.device, self.supported_modalities)
        self.processor = loader.create_processor(self.supported_modalities)
        self.generation_config = loader.create_generation_config()
        self.chat_template_kwargs = loader.chat_template_kwargs
        self.formatter = ChatTemplateFormatter()

        # Get tokenizer from processor (for multimodal, it's an attribute)
        self.tokenizer = getattr(self.processor, "tokenizer", self.processor)

        # Ensure padding token is set
        self._setup_padding_token()

    def _setup_padding_token(self) -> None:
        """Ensure the tokenizer has a padding token configured."""
        if hasattr(self.tokenizer, "pad_token") and self.tokenizer.pad_token is None:
            if getattr(self.tokenizer, "eos_token", None) is None:
                raise ValueError("Tokenizer has no pad_token and no eos_token. Cannot safely infer padding behavior.")
            self.tokenizer.pad_token = self.tokenizer.eos_token

            if getattr(self.tokenizer, "pad_token_id", None) is None:
                eos_id = getattr(self.tokenizer, "eos_token_id", None)
                if eos_id is not None:
                    try:
                        self.tokenizer.pad_token_id = eos_id
                    except Exception:
                        pass

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
    # Main Prompt Method (follows standard HF inference pattern)
    # =========================================================================

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        """Generate responses for prompt messages.

        Follows the standard HuggingFace inference pattern seen on model cards:
        1. Format messages
        2. processor.apply_chat_template(messages, **chat_template_kwargs)
        3. model.generate(**inputs)
        4. tokenizer.decode()
        """
        # 1. Format messages (all message handling in one place)
        chat_messages, images = self.formatter.format(messages, self.system_message)

        # 2. Apply chat template + tokenize
        #
        # For multimodal models, images typically must be passed separately to the processor
        # while the chat template includes {"type": "image"} placeholders.
        if images and "image" in self.supported_modalities:
            # Render prompt text only (tokenize=False) then run processor with images + text.
            chat_template_kwargs = dict(self.chat_template_kwargs)
            chat_template_kwargs["tokenize"] = False
            prompt_text = self.processor.apply_chat_template(chat_messages, **chat_template_kwargs)

            # Build processor kwargs from chat template kwargs (tokenization-focused subset)
            processor_kwargs: dict[str, Any] = {}
            for key in ("return_tensors", "return_dict", "padding", "truncation", "max_length"):
                if key in self.chat_template_kwargs:
                    processor_kwargs[key] = self.chat_template_kwargs[key]

            inputs = self.processor(
                text=prompt_text,
                images=images,
                **processor_kwargs,
            )
        else:
            # Text-only path (or model doesn't support images): just use apply_chat_template.
            # User can configure via options: tokenize, add_generation_prompt, etc.
            inputs = self.processor.apply_chat_template(chat_messages, **self.chat_template_kwargs)

        # Move to device
        model_device = getattr(self.model, "device", self.device)
        inputs = {k: v.to(model_device) if hasattr(v, "to") else v for k, v in inputs.items()}

        input_length = inputs["input_ids"].shape[1]

        # Ensure pad_token_id is set
        if self.generation_config.pad_token_id is None:
            self.generation_config.pad_token_id = self.tokenizer.pad_token_id

        with torch.inference_mode():
            # 3. Generate
            outputs = self.model.generate(**inputs, generation_config=self.generation_config)

            # 4. Decode
            generated_ids = outputs[0, input_length:]
            generated_text = self.tokenizer.decode(generated_ids, skip_special_tokens=True)

        # Record metrics
        output_tokens = len(generated_ids)
        self._record_usage_metrics(
            input_tokens=input_length,
            output_tokens=output_tokens,
            total_tokens=input_length + output_tokens,
        )

        return self._parse_return_format(generated_text)
