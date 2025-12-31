from __future__ import annotations

from dataclasses import dataclass, field
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any

import torch
from transformers import AutoModelForCausalLM, AutoProcessor, AutoTokenizer

from daft.ai.metrics import record_token_metrics
from daft.ai.protocols import Prompter, PrompterDescriptor
from daft.ai.provider import ProviderImportError
from daft.ai.typing import Options, PromptOptions, UDFOptions
from daft.ai.utils import get_gpu_udf_options, get_torch_device
from daft.dependencies import np
from daft.file import File

if TYPE_CHECKING:
    from pydantic import BaseModel


class TransformersPromptOptions(PromptOptions, total=False):
    """Options for Transformers prompter.

    Attributes:
        max_new_tokens (int): Maximum number of new tokens to generate.
        temperature (float): Sampling temperature for generation.
        top_p (float): Top-p (nucleus) sampling parameter.
        top_k (int): Top-k sampling parameter.
        do_sample (bool): Whether to use sampling for generation.
        num_logprobs (int): Number of top log probabilities to return per token (only used when output_mode="logprobs").
        trust_remote_code (bool): Whether to trust remote code when loading models.
        torch_dtype (str): The torch dtype to use for the model (e.g., "float16", "bfloat16", "float32").
        device_map (str): Device map for model loading (e.g., "auto", "cuda", "cpu").
        quantization_config (dict): Quantization configuration for bitsandbytes or other quantization methods.
            Example for 4-bit: {"load_in_4bit": True, "bnb_4bit_compute_dtype": "float16"}
            Example for 8-bit: {"load_in_8bit": True}

    Note:
        Set output_mode="logprobs" in PromptOptions to get ground truth logprobs for RL training.
        Any additional arguments defined here will be forwarded directly to
        the model's generate() method when making prompt calls.
    """

    max_new_tokens: int
    temperature: float
    top_p: float
    top_k: int
    do_sample: bool
    num_logprobs: int
    trust_remote_code: bool
    torch_dtype: str
    device_map: str
    quantization_config: dict[str, Any]


@dataclass
class TransformersPrompterDescriptor(PrompterDescriptor):
    """Descriptor for Transformers prompter."""

    provider_name: str
    model_name: str
    prompt_options: TransformersPromptOptions = field(
        default_factory=lambda: TransformersPromptOptions(
            max_new_tokens=256,
            do_sample=False,
            num_logprobs=5,
            trust_remote_code=True,
            max_retries=0,
            on_error="raise",
            output_mode="text",
        )
    )
    system_message: str | None = None
    return_format: BaseModel | None = None

    def get_output_mode(self) -> str:
        """Returns the output mode for this prompter."""
        return self.prompt_options.get("output_mode", "text")

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.prompt_options)

    def get_udf_options(self) -> UDFOptions:
        udf_options = get_gpu_udf_options()
        for key, value in self.prompt_options.items():
            if key in udf_options.__annotations__:
                setattr(udf_options, key, value)
        # Local models don't retry the same way API-based providers do
        udf_options.max_retries = 0
        return udf_options

    def instantiate(self) -> Prompter:
        return TransformersPrompter(
            provider_name=self.provider_name,
            model=self.model_name,
            system_message=self.system_message,
            return_format=self.return_format,
            prompt_options=self.prompt_options,
        )


class TransformersPrompter(Prompter):
    """Transformers prompter implementation using Hugging Face Transformers for local inference."""

    def __init__(
        self,
        provider_name: str,
        model: str,
        system_message: str | None = None,
        return_format: BaseModel | None = None,
        prompt_options: TransformersPromptOptions | None = None,
    ) -> None:
        self.provider_name = provider_name
        self.model_name = model
        self.return_format = return_format
        self.system_message = system_message

        # Parse options
        opts = dict(prompt_options) if prompt_options else {}

        # Extract output mode and generation-specific options
        self.output_mode: str = str(opts.pop("output_mode", "text"))
        num_logprobs_val = opts.pop("num_logprobs", 5)
        self.num_logprobs: int = num_logprobs_val if isinstance(num_logprobs_val, int) else 5
        trust_remote_code: bool = bool(opts.pop("trust_remote_code", True))
        torch_dtype_str: str | None = opts.pop("torch_dtype", None)  # type: ignore[assignment]
        device_map: str | None = opts.pop("device_map", None)  # type: ignore[assignment]
        quantization_config: dict[str, Any] | None = opts.pop("quantization_config", None)  # type: ignore[assignment]

        # Remove non-generation options
        opts.pop("max_retries", None)
        opts.pop("on_error", None)

        # Get the target device first
        self.device = get_torch_device()

        # Determine torch dtype based on device capabilities
        if torch_dtype_str:
            torch_dtype = getattr(torch, torch_dtype_str, torch.float32)
            if not isinstance(torch_dtype, torch.dtype):
                torch_dtype = torch.float32
        else:
            # Choose appropriate dtype based on device
            if self.device.type == "cuda":
                torch_dtype = torch.float16
            elif self.device.type == "mps":
                # MPS works well with float16 for most models
                torch_dtype = torch.float16
            else:
                # CPU fallback to float32 for better compatibility
                torch_dtype = torch.float32

        # Check if accelerate is available for device_map usage
        accelerate_available = False
        try:
            import accelerate  # noqa: F401

            accelerate_available = True
            if device_map is None:
                device_map = "auto"
        except ImportError:
            # accelerate not available, use manual device placement
            device_map = None

        # Store remaining options for generation
        self.generation_config = {
            k: v
            for k, v in opts.items()
            if k not in TransformersPromptOptions.__annotations__
            or k in ("max_new_tokens", "temperature", "top_p", "top_k", "do_sample")
        }

        # Try to load as a multimodal model first, fall back to text-only
        try:
            self.processor = AutoProcessor.from_pretrained(
                model,
                trust_remote_code=trust_remote_code,
            )
            self.is_multimodal = True
        except Exception:
            self.processor = AutoTokenizer.from_pretrained(
                model,
                trust_remote_code=trust_remote_code,
            )
            self.is_multimodal = False

        # For multimodal processors, the tokenizer is accessed via .tokenizer attribute
        self.tokenizer = getattr(self.processor, "tokenizer", self.processor)

        # Ensure padding token is set
        if hasattr(self.tokenizer, "pad_token") and self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        model_kwargs: dict[str, Any] = {
            "torch_dtype": torch_dtype,
            "trust_remote_code": trust_remote_code,
        }

        # Handle quantization config
        if quantization_config is not None:
            from transformers import BitsAndBytesConfig

            # Convert compute dtype string to torch dtype if present
            if "bnb_4bit_compute_dtype" in quantization_config:
                dtype_str = quantization_config["bnb_4bit_compute_dtype"]
                quantization_config["bnb_4bit_compute_dtype"] = getattr(torch, dtype_str, torch.float16)

            model_kwargs["quantization_config"] = BitsAndBytesConfig(**quantization_config)
            # Quantization requires device_map
            if device_map is None:
                device_map = "auto"

        if accelerate_available and device_map is not None:
            model_kwargs["device_map"] = device_map

        # Try loading the model with different Auto classes for compatibility
        # VLM models (like Qwen2-VL) need AutoModelForVision2Seq, not AutoModelForCausalLM
        model_loaded = False
        model_classes = [AutoModelForCausalLM]

        if self.is_multimodal:
            # Try vision-to-sequence models first for VLMs
            from transformers import AutoModel, AutoModelForVision2Seq

            model_classes = [AutoModelForVision2Seq, AutoModelForCausalLM, AutoModel]

        for model_cls in model_classes:
            try:
                self.model = model_cls.from_pretrained(model, **model_kwargs)
                model_loaded = True
                break
            except (ValueError, KeyError):
                # This model class doesn't support this architecture, try the next one
                continue

        if not model_loaded:
            raise ValueError(f"Could not load model {model} with any supported Auto class")

        if not accelerate_available or device_map is None:
            # Manual device placement
            self.model = self.model.to(self.device)

        self.model.eval()

    @singledispatchmethod
    def _process_message(self, msg: Any) -> dict[str, Any]:
        """Fallback for unsupported message content types."""
        raise ValueError(f"Unsupported content type in prompt: {type(msg)}")

    @_process_message.register
    def _process_str_message(self, msg: str) -> dict[str, Any]:
        """Handle string messages as plain text."""
        return {"type": "text", "text": msg}

    @_process_message.register
    def _process_bytes_message(self, msg: bytes) -> dict[str, Any]:
        """Handle bytes messages by detecting MIME type."""
        from daft.daft import guess_mimetype_from_content

        maybe_mime_type = guess_mimetype_from_content(msg)
        mime_type = maybe_mime_type if maybe_mime_type else "application/octet-stream"

        if mime_type.startswith("image/"):
            return self._build_image_from_bytes(msg)
        return self._build_text_from_bytes(msg)

    @_process_message.register
    def _process_file_message(self, msg: File) -> dict[str, Any]:
        """Handle File objects."""
        mime_type = msg.mime_type()

        if self._is_text_mime_type(mime_type):
            filetag = f"file_{mime_type.replace('/', '_')}"
            text_content = f"<{filetag}>{self._read_text_content(msg)}</{filetag}>"
            return {"type": "text", "text": text_content}

        with msg.open() as f:
            file_bytes = f.read()

        if mime_type.startswith("image/"):
            return self._build_image_from_bytes(file_bytes)
        return self._build_text_from_bytes(file_bytes)

    if np.module_available():  # type: ignore[attr-defined]

        @_process_message.register(np.ndarray)
        def _process_image_message(self, msg: np.typing.NDArray[Any]) -> dict[str, Any]:
            """Handle numpy array messages (images)."""
            from daft.dependencies import pil_image

            if not pil_image.module_available():
                raise ProviderImportError("transformers", function="prompt")

            image = pil_image.fromarray(msg)
            return {"type": "image", "image": image}

    def _build_image_from_bytes(self, data: bytes) -> dict[str, Any]:
        """Build an image message from bytes."""
        import io

        from daft.dependencies import pil_image

        if not pil_image.module_available():
            raise ProviderImportError("transformers", function="prompt")

        image = pil_image.open(io.BytesIO(data))
        return {"type": "image", "image": image}

    def _build_text_from_bytes(self, data: bytes) -> dict[str, Any]:
        """Build a text message from bytes."""
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("utf-8", errors="replace")
        return {"type": "text", "text": text}

    def _is_text_mime_type(self, mime_type: str) -> bool:
        normalized = mime_type.split(";")[0].strip().lower()
        return normalized.startswith("text/")

    def _read_text_content(self, file_obj: File) -> str:
        with file_obj.open() as f:
            file_bytes = f.read()

        if isinstance(file_bytes, str):
            return file_bytes

        if isinstance(file_bytes, bytes):
            try:
                return file_bytes.decode("utf-8")
            except UnicodeDecodeError:
                return file_bytes.decode("utf-8", errors="replace")

        raise TypeError("File contents must be bytes or string")

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

    def _build_chat_messages(self, processed_messages: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[Any]]:
        """Build chat messages in the format expected by transformers chat templates."""
        messages: list[dict[str, Any]] = []

        if self.system_message:
            messages.append({"role": "system", "content": self.system_message})

        # Build user message content
        content: list[dict[str, Any]] = []
        images: list[Any] = []

        for msg in processed_messages:
            if msg["type"] == "text":
                content.append({"type": "text", "text": msg["text"]})
            elif msg["type"] == "image":
                content.append({"type": "image"})
                images.append(msg["image"])

        messages.append({"role": "user", "content": content})

        return messages, images

    def _extract_logprobs(
        self,
        output_ids: torch.Tensor,
        scores: tuple[torch.Tensor, ...],
        input_length: int,
    ) -> dict[str, Any]:
        """Extract log probabilities from generation output.

        Returns a dict containing:
            - token_ids: list of generated token IDs
            - tokens: list of decoded token strings
            - logprobs: list of log probabilities for each generated token (ground truth from generation)
            - top_logprobs: optional list of top-k alternatives per token (if num_logprobs > 0)

        The logprobs are the EXACT values computed during generation, not from a separate forward pass.
        This is critical for RL training where policy gradients require the original sampling distribution.
        """
        token_ids: list[int] = []
        tokens: list[str] = []
        logprobs: list[float] = []
        top_logprobs_list: list[list[dict[str, Any]]] = []

        for i, score in enumerate(scores):
            # Get log probabilities - these are from the actual generation forward pass
            log_probs = torch.nn.functional.log_softmax(score, dim=-1)

            # Get the generated token and its log probability
            generated_token_id = output_ids[0, input_length + i].item()
            generated_log_prob = log_probs[0, generated_token_id].item()

            token_ids.append(generated_token_id)
            tokens.append(self.tokenizer.decode([generated_token_id]))
            logprobs.append(generated_log_prob)

            # Optionally get top-k alternatives
            if self.num_logprobs > 0:
                top_log_probs, top_indices = torch.topk(log_probs[0], k=min(self.num_logprobs, log_probs.shape[-1]))
                top_tokens = [
                    {
                        "token": self.tokenizer.decode([tid]),
                        "token_id": tid,
                        "logprob": lp,
                    }
                    for lp, tid in zip(top_log_probs.tolist(), top_indices.tolist())
                ]
                top_logprobs_list.append(top_tokens)

        result = {
            "token_ids": token_ids,
            "tokens": tokens,
            "logprobs": logprobs,  # Ground truth logprobs from generation
        }

        if self.num_logprobs > 0:
            result["top_logprobs"] = top_logprobs_list

        return result

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        """Generate responses for a batch of message strings."""
        # Process messages
        processed_messages = [self._process_message(msg) for msg in messages]
        chat_messages, images = self._build_chat_messages(processed_messages)

        # Determine the device to use for inputs
        model_device = getattr(self.model, "device", self.device)

        # Prepare inputs based on model type
        with torch.inference_mode():
            if self.is_multimodal and images:
                # For multimodal models, use the processor with images
                text = self.processor.apply_chat_template(
                    chat_messages,
                    tokenize=False,
                    add_generation_prompt=True,
                )
                inputs = self.processor(
                    text=text,
                    images=images if images else None,
                    return_tensors="pt",
                    padding=True,
                ).to(model_device)
            else:
                # For text-only models, use standard tokenization
                # Flatten content to text for models without chat template support
                try:
                    text = self.processor.apply_chat_template(
                        chat_messages,
                        tokenize=False,
                        add_generation_prompt=True,
                    )
                except Exception:
                    # Fallback for models without chat template
                    text_parts = []
                    if self.system_message:
                        text_parts.append(f"System: {self.system_message}")
                    for msg in processed_messages:
                        if msg["type"] == "text":
                            text_parts.append(msg["text"])
                    text = "\n".join(text_parts)

                inputs = self.processor(
                    text,
                    return_tensors="pt",
                    padding=True,
                ).to(model_device)

            input_length = inputs["input_ids"].shape[1]

            # Set up generation kwargs
            gen_kwargs = {
                **self.generation_config,
                "pad_token_id": self.tokenizer.pad_token_id,
            }

            # Enable score output if logprobs mode requested
            if self.output_mode == "logprobs":
                gen_kwargs["output_scores"] = True
                gen_kwargs["return_dict_in_generate"] = True

            # Generate
            outputs = self.model.generate(**inputs, **gen_kwargs)

            # Handle output format based on output mode
            if self.output_mode == "logprobs":
                output_ids = outputs.sequences
                scores = outputs.scores
                logprobs = self._extract_logprobs(output_ids, scores, input_length)
            else:
                output_ids = outputs
                logprobs = None

            # Decode the generated text (excluding input tokens)
            generated_ids = output_ids[0, input_length:]
            generated_text = self.tokenizer.decode(generated_ids, skip_special_tokens=True)

            # Record metrics
            output_tokens = len(generated_ids)
            self._record_usage_metrics(
                input_tokens=input_length,
                output_tokens=output_tokens,
                total_tokens=input_length + output_tokens,
            )

        # Parse structured output if return_format is specified
        if self.return_format is not None:
            import json

            try:
                # Try to parse as JSON and validate with Pydantic
                parsed = json.loads(generated_text)
                result = self.return_format.model_validate(parsed)
            except Exception:
                # If parsing fails, return the raw text
                result = generated_text
        else:
            result = generated_text

        # Return based on output mode
        if self.output_mode == "logprobs" and logprobs:
            # Return full trajectory info for RL training
            # Logprobs are the ground truth values from generation - critical for policy gradients
            return {
                "text": result,
                "token_ids": logprobs["token_ids"],
                "tokens": logprobs["tokens"],
                "logprobs": logprobs["logprobs"],  # Ground truth for policy gradients
                "top_logprobs": logprobs.get("top_logprobs"),
            }

        return result
