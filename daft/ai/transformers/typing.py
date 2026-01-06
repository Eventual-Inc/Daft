"""TypedDict definitions and key sets for Transformers provider configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from typing import Literal


# =============================================================================
# Key Sets (source of truth for config partitioning)
# =============================================================================
# Keys that go to model.from_pretrained() (not GenerationConfig)
MODEL_LOADING_KEYS: frozenset[str] = frozenset(
    {
        "model_class",
        "trust_remote_code",
        "torch_dtype",
        "device_map",
        "quantization_config",
        "attn_implementation",
        "low_cpu_mem_usage",
        "use_safetensors",
    }
)
# Keys that go to processor.apply_chat_template()
CHAT_TEMPLATE_KEYS: frozenset[str] = frozenset(
    {
        "tokenize",
        "add_generation_prompt",
        "return_tensors",
        "return_dict",
        "continue_final_message",
        "padding",
        "truncation",
    }
)

# Keys that go to UDF options
UDF_KEYS: frozenset[str] = frozenset(
    {
        "concurrency",
        "num_gpus",
        "max_retries",
        "on_error",
        "batch_size",
    }
)

# Note: Generation config keys are derived at runtime from transformers.GenerationConfig
# because they change across versions and there are 50+ of them.


# =============================================================================
# TypedDicts (IDE hints, grouped by category)
# =============================================================================


class ModelLoadingOptions(TypedDict, total=False):
    """Options passed to model.from_pretrained().

    Attributes:
        model_class: Auto class name (e.g., "AutoModelForCausalLM", "AutoModelForImageTextToText").
                     Default: "AutoModelForCausalLM" for text, "AutoModelForImageTextToText" for vision.
        trust_remote_code: Whether to trust remote code when loading models. Default: True
        torch_dtype: The torch dtype to use (e.g., "float16", "bfloat16", "float32").
        device_map: Device placement strategy (e.g., "auto", "cuda", "cpu").
        attn_implementation: Attention implementation (e.g., "flash_attention_2", "sdpa").
        low_cpu_mem_usage: Whether to use low CPU memory usage mode.
        use_safetensors: Whether to use safetensors format.
    """

    model_class: str
    trust_remote_code: bool
    torch_dtype: str
    device_map: str | dict[str, Any]
    attn_implementation: str
    low_cpu_mem_usage: bool
    use_safetensors: bool


class QuantizationOptions(TypedDict, total=False):
    """Options for BitsAndBytesConfig.

    Example for 4-bit: {"load_in_4bit": True, "bnb_4bit_compute_dtype": "float16"}
    Example for 8-bit: {"load_in_8bit": True}
    """

    load_in_4bit: bool
    load_in_8bit: bool
    bnb_4bit_compute_dtype: str
    bnb_4bit_quant_type: str
    bnb_4bit_use_double_quant: bool


class GenerationOptions(TypedDict, total=False):
    """Common generation options (subset - full list derived from GenerationConfig).

    Note:
        Any additional GenerationConfig parameters not listed here will be
        passed through and validated by transformers.GenerationConfig.
    """

    max_new_tokens: int
    temperature: float
    top_p: float
    top_k: int
    do_sample: bool
    num_beams: int
    repetition_penalty: float
    pad_token_id: int
    eos_token_id: int | list[int]
    max_length: int
    min_length: int


class ChatTemplateOptions(TypedDict, total=False):
    """Options for processor.apply_chat_template().

    See: https://huggingface.co/docs/transformers/chat_templating

    Attributes:
        tokenize: Whether to tokenize the output. Default: True
        add_generation_prompt: Add tokens for assistant response. Default: True
        return_tensors: Tensor format ("pt", "tf", "np"). Default: "pt"
        return_dict: Return dict with input_ids, attention_mask, etc. Default: True
        continue_final_message: Continue last message instead of new one.
        padding: Padding strategy for tokenization.
        truncation: Truncation strategy for tokenization.
    """

    tokenize: bool
    add_generation_prompt: bool
    return_tensors: str
    return_dict: bool
    continue_final_message: bool
    padding: bool | str
    truncation: bool | str


class TransformersUDFOptions(TypedDict, total=False):
    """Daft UDF execution options.

    Attributes:
        concurrency: Max concurrent UDF workers.
        num_gpus: Number of GPUs per worker.
        max_retries: Maximum retry attempts. Default: 0 (local models don't retry).
        on_error: Error handling behavior.
        batch_size: Batch size for processing.
    """

    concurrency: int
    num_gpus: int
    max_retries: int
    on_error: Literal["raise", "log", "ignore"]
    batch_size: int


class TransformersPromptOptions(
    ModelLoadingOptions,
    GenerationOptions,
    ChatTemplateOptions,
    TransformersUDFOptions,
    total=False,
):
    """All options for Transformers prompter.

    Combines model loading, generation, chat template, and UDF options.
    See individual TypedDicts for detailed documentation.

    Example:
        >>> options: TransformersPromptOptions = {
        ...     "trust_remote_code": True,
        ...     "torch_dtype": "float16",
        ...     "max_new_tokens": 100,
        ...     "temperature": 0.7,
        ...     "add_generation_prompt": True,
        ... }
    """

    quantization_config: QuantizationOptions | dict[str, Any]


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "CHAT_TEMPLATE_KEYS",
    "MODEL_LOADING_KEYS",
    "UDF_KEYS",
    "ChatTemplateOptions",
    "GenerationOptions",
    "ModelLoadingOptions",
    "QuantizationOptions",
    "TransformersPromptOptions",
    "TransformersUDFOptions",
]
