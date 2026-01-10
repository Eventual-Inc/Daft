from __future__ import annotations

from typing import TYPE_CHECKING

from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoModelForImageTextToText,
    AutoProcessor,
    AutoTokenizer,
    GenerationConfig,
    PreTrainedModel,
)

from daft.ai.utils import get_torch_device

if TYPE_CHECKING:
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor


class TransformersPrompterModelLoader:
    """Helper to load and configure Transformers models and processors."""

    def __init__(self, descriptor: TransformersPrompterDescriptor) -> None:
        self.descriptor = descriptor
        self.model_name = descriptor.model_name
        self.auto_config = AutoConfig.from_pretrained(self.model_name)
        self.device = get_torch_device()

    def create_model(self) -> PreTrainedModel:
        """Create and configure the model."""
        options = self.descriptor.get_model_loading_options()

        if hasattr(self.auto_config, "vision_config") and self.auto_config.vision_config is not None:
            model = AutoModelForImageTextToText.from_pretrained(self.model_name, **options)
        else:
            model = AutoModelForCausalLM.from_pretrained(self.model_name, **options)

        if options.get("device_map") is None:
            model = model.to(self.device)

        return model

    def create_processor(self) -> AutoProcessor | AutoTokenizer:
        """Create the processor/tokenizer for the model.

        Uses AutoProcessor for multimodal models (vision_config present),
        AutoTokenizer for text-only models.
        """
        if hasattr(self.auto_config, "vision_config") and self.auto_config.vision_config is not None:
            return AutoProcessor.from_pretrained(self.model_name, **self.descriptor.get_processor_options())
        return AutoTokenizer.from_pretrained(self.model_name, **self.descriptor.get_tokenizer_options())

    def create_generation_config(self) -> GenerationConfig:
        """Create a GenerationConfig from descriptor options."""
        return GenerationConfig(**self.descriptor.get_generation_config_options())
