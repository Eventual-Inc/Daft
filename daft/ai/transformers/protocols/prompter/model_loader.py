from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

import torch

from daft.ai.provider import ProviderImportError
from daft.ai.transformers.typing import CHAT_TEMPLATE_KEYS, MODEL_LOADING_KEYS

if TYPE_CHECKING:
    from transformers import AutoProcessor, AutoTokenizer, GenerationConfig, PreTrainedModel


# =============================================================================
# Config Partitioning
# =============================================================================


def _get_class_init_params(cls: type) -> frozenset[str]:
    """Extract parameter names from a class's __init__ signature."""
    try:
        sig = inspect.signature(cls)
        return frozenset(sig.parameters.keys())
    except (ValueError, TypeError):
        return frozenset()


@dataclass
class PartitionedConfig:
    """Partitioned configuration for TransformersPrompterModelLoader.

    Separates raw user options into HF-specific categories.
    Extensible for future output format options (logprobs, reasoning traces, etc.)
    """

    model_kwargs: dict[str, Any]
    processor_kwargs: dict[str, Any]
    generation_kwargs: dict[str, Any]
    chat_template_kwargs: dict[str, Any]
    quantization_kwargs: dict[str, Any] | None


class TransformersPrompterModelLoader:
    """Helper to load and configure Transformers models and processors."""

    # Cache for generation config keys (derived from transformers at runtime)
    _generation_config_keys: ClassVar[frozenset[str] | None] = None

    @classmethod
    def get_generation_config_keys(cls) -> frozenset[str]:
        """Get valid GenerationConfig parameter names from transformers."""
        if cls._generation_config_keys is None:
            from transformers import GenerationConfig

            cls._generation_config_keys = _get_class_init_params(GenerationConfig)
        return cls._generation_config_keys

    @classmethod
    def partition_options(cls, raw_options: dict[str, Any]) -> PartitionedConfig:
        """Partition raw user options into HF-specific categories.

        Args:
            raw_options: Raw options dict (will not be mutated).

        Returns:
            PartitionedConfig with separated kwargs.
        """
        opts = dict(raw_options)  # Don't mutate caller's dict
        generation_keys = cls.get_generation_config_keys()

        # Extract quantization config (dict or BitsAndBytesConfig -> dict)
        quantization_config = opts.pop("quantization_config", None)
        quantization_kwargs: dict[str, Any] | None = None
        if quantization_config is not None:
            if hasattr(quantization_config, "to_dict"):
                quantization_kwargs = quantization_config.to_dict()
            elif isinstance(quantization_config, dict):
                quantization_kwargs = quantization_config
            else:
                raise TypeError("quantization_config must be dict or BitsAndBytesConfig")

        # Extract model loading options
        model_kwargs: dict[str, Any] = {"trust_remote_code": True}  # Default
        for key in list(opts.keys()):
            if key in MODEL_LOADING_KEYS:
                model_kwargs[key] = opts.pop(key)

        # Extract chat template options (with inference defaults)
        chat_template_kwargs: dict[str, Any] = {
            "tokenize": True,
            "add_generation_prompt": True,
            "return_dict": True,
            "return_tensors": "pt",
        }
        for key in list(opts.keys()):
            if key in CHAT_TEMPLATE_KEYS:
                chat_template_kwargs[key] = opts.pop(key)

        # Remaining options go to generation config
        generation_kwargs: dict[str, Any] = {}
        for key, value in opts.items():
            if key in generation_keys or key not in MODEL_LOADING_KEYS:
                generation_kwargs[key] = value

        # Processor kwargs derived from model kwargs
        processor_kwargs = {"trust_remote_code": model_kwargs.get("trust_remote_code", True)}

        return PartitionedConfig(
            model_kwargs=model_kwargs,
            processor_kwargs=processor_kwargs,
            generation_kwargs=generation_kwargs,
            chat_template_kwargs=chat_template_kwargs,
            quantization_kwargs=quantization_kwargs,
        )

    def __init__(
        self,
        model_name: str,
        config: PartitionedConfig,
    ) -> None:
        self.model_name = model_name
        self.model_kwargs = config.model_kwargs
        self.processor_kwargs = config.processor_kwargs
        self.generation_kwargs = config.generation_kwargs
        self.chat_template_kwargs = config.chat_template_kwargs
        self.quantization_kwargs = config.quantization_kwargs

    def create_generation_config(self) -> GenerationConfig:
        """Create a validated GenerationConfig from resolved options."""
        import transformers

        return transformers.GenerationConfig(**self.generation_kwargs)

    def create_quantization_config(self) -> Any | None:
        """Create BitsAndBytesConfig if quantization was requested."""
        if self.quantization_kwargs is None:
            return None

        try:
            from transformers import BitsAndBytesConfig
        except ImportError:
            raise ProviderImportError("transformers", function="quantization")

        return BitsAndBytesConfig(**self.quantization_kwargs)

    def _get_model_class(self, supported_modalities: frozenset[str]) -> Any:
        """Get the model Auto class to use.

        User can specify via model_class option, otherwise picks sensible default.
        """
        import transformers

        model_class_name = self.model_kwargs.get("model_class")

        if model_class_name:
            # User-specified class
            model_cls = getattr(transformers, model_class_name, None)
            if model_cls is None:
                raise ValueError(
                    f"Unknown model class: {model_class_name}. "
                    f"Available: AutoModelForCausalLM, AutoModelForImageTextToText, etc."
                )
            return model_cls

        # Default based on modalities
        if "image" in supported_modalities or "video" in supported_modalities:
            # Prefer newer API, fall back to older if not available
            return getattr(
                transformers,
                "AutoModelForImageTextToText",
                getattr(transformers, "AutoModelForVision2Seq", transformers.AutoModelForCausalLM),
            )

        return transformers.AutoModelForCausalLM

    def create_model(self, device: torch.device, supported_modalities: frozenset[str]) -> PreTrainedModel:
        """Create and configure the model.

        Args:
            device: Target torch device.
            supported_modalities: Pre-detected modalities (call detect_supported_modalities once).

        Uses model_class option if specified, otherwise picks default based on modalities.
        Fails fast if loading fails - no fallback chain.
        """
        # Resolve torch dtype
        torch_dtype = self._resolve_torch_dtype(device)

        # Build model kwargs (exclude model_class - it's not a from_pretrained arg)
        from_pretrained_kwargs: dict[str, Any] = {
            "torch_dtype": torch_dtype,
            "trust_remote_code": self.model_kwargs.get("trust_remote_code", True),
        }

        # Add device_map if specified
        device_map = self.model_kwargs.get("device_map")
        if device_map is not None:
            # device_map="auto" requires accelerate
            if device_map == "auto":
                from daft.dependencies import accelerate

                if not accelerate.module_available():
                    raise ProviderImportError("transformers", function="prompt")
            from_pretrained_kwargs["device_map"] = device_map

        # Add attn_implementation if specified
        attn_impl = self.model_kwargs.get("attn_implementation")
        if attn_impl is not None:
            from_pretrained_kwargs["attn_implementation"] = attn_impl

        # Forward other common model loading kwargs if specified
        low_cpu_mem_usage = self.model_kwargs.get("low_cpu_mem_usage")
        if low_cpu_mem_usage is not None:
            from_pretrained_kwargs["low_cpu_mem_usage"] = low_cpu_mem_usage

        use_safetensors = self.model_kwargs.get("use_safetensors")
        if use_safetensors is not None:
            from_pretrained_kwargs["use_safetensors"] = use_safetensors

        # Add quantization config if present
        quant_config = self.create_quantization_config()
        if quant_config is not None:
            # Quantization requires bitsandbytes (and sometimes accelerate for dispatching)
            from daft.dependencies import bitsandbytes

            if not bitsandbytes.module_available():
                raise ProviderImportError("transformers", function="quantization")
            from_pretrained_kwargs["quantization_config"] = quant_config

        # Get model class and load (fail fast)
        model_cls = self._get_model_class(supported_modalities)
        model = model_cls.from_pretrained(self.model_name, **from_pretrained_kwargs)

        # Manual device placement if not using device_map
        if device_map is None:
            model = model.to(device)

        model.eval()
        return model

    def create_processor(self, supported_modalities: frozenset[str]) -> AutoProcessor | AutoTokenizer:
        """Create the processor/tokenizer for the model.

        Args:
            supported_modalities: Pre-detected modalities (call detect_supported_modalities once).

        Uses AutoProcessor for multimodal, AutoTokenizer for text-only.
        Fails fast if loading fails.
        """
        import transformers

        trust_remote_code = self.processor_kwargs.get("trust_remote_code", True)

        if any(m in supported_modalities for m in ("image", "audio", "video")):
            return transformers.AutoProcessor.from_pretrained(self.model_name, trust_remote_code=trust_remote_code)

        return transformers.AutoTokenizer.from_pretrained(self.model_name, trust_remote_code=trust_remote_code)

    def _resolve_torch_dtype(self, device: torch.device) -> torch.dtype:
        """Resolve torch dtype from string or device defaults."""
        torch_dtype_str = self.model_kwargs.get("torch_dtype")

        if torch_dtype_str:
            torch_dtype = getattr(torch, torch_dtype_str, torch.float32)
            if not isinstance(torch_dtype, torch.dtype):
                torch_dtype = torch.float32
        elif device.type in ("cuda", "mps"):
            torch_dtype = torch.float16
        else:
            torch_dtype = torch.float32

        return torch_dtype

    def detect_supported_modalities(self) -> frozenset[str]:
        """Detect model modalities from config.json (cheap operation)."""
        import transformers

        modalities: set[str] = {"text"}
        try:
            cfg = transformers.AutoConfig.from_pretrained(
                self.model_name,
                trust_remote_code=self.model_kwargs.get("trust_remote_code", True),
            )
            cfg_dict = cfg.to_dict() if hasattr(cfg, "to_dict") else dict(cfg)
        except Exception:
            return frozenset(modalities)

        # Vision
        if cfg_dict.get("vision_config") is not None:
            modalities.add("image")

        # Token-level hints for vision
        vision_hints = ("image_token_id", "vision_start_token_id", "vision_end_token_id", "vision_token_id")
        if any(k in cfg_dict for k in vision_hints):
            modalities.add("image")

        # Video
        if cfg_dict.get("video_token_id") is not None:
            modalities.add("video")

        # Audio
        if cfg_dict.get("audio_config") is not None:
            modalities.add("audio")

        return frozenset(modalities)
