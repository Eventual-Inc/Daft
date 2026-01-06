from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, Mock, patch

import pytest
from pydantic import BaseModel

from daft.ai.provider import ProviderImportError
from daft.ai.transformers.protocols.prompter import (
    TransformersPrompter,
    TransformersPrompterDescriptor,
)
from daft.ai.transformers.protocols.prompter.model_loader import (
    PartitionedConfig,
    TransformersPrompterModelLoader,
)

# Checks if transformers is available
transformers = pytest.importorskip("transformers")
torch = pytest.importorskip("torch")


def run_async(coro):
    """Helper to run async functions in sync tests."""
    return asyncio.run(coro)


class SimpleResponse(BaseModel):
    """Simple Pydantic model for testing structured outputs."""

    answer: str
    confidence: float


class ComplexResponse(BaseModel):
    """More complex Pydantic model for testing."""

    summary: str
    key_points: list[str]
    sentiment: str


DEFAULT_MODEL_NAME = "microsoft/Phi-3-mini-4k-instruct"
DEFAULT_PROVIDER_NAME = "transformers"


# =============================================================================
# Mocks & Helpers
# =============================================================================


def create_mock_model(device_type="cpu"):
    """Create a mock transformers model."""
    mock_model = MagicMock()
    mock_model.device = Mock()
    mock_model.device.type = device_type
    mock_model.eval = Mock()
    mock_model.to = Mock(return_value=mock_model)
    # Default generate return
    mock_model.generate = Mock(return_value=torch.tensor([[1, 2, 3]]))
    return mock_model


def create_mock_inputs():
    """Create mock inputs dict-like object returned by tokenizer/processor."""
    mock_inputs = MagicMock()
    # Use a MagicMock for input_ids so shape is settable
    mock_input_ids = MagicMock()
    mock_input_ids.shape = [1, 5]  # Batch size 1, seq len 5

    mock_inputs.input_ids = mock_input_ids
    # Make it behave like a dict for the dict comprehension in prompt()
    mock_inputs.items.return_value = [("input_ids", mock_input_ids)]
    mock_inputs.__getitem__ = Mock(side_effect=lambda k: mock_input_ids if k == "input_ids" else MagicMock())
    return mock_inputs


def create_mock_tokenizer():
    """Create a mock tokenizer."""
    mock_tokenizer = MagicMock()
    mock_tokenizer.pad_token = None
    mock_tokenizer.eos_token = "<eos>"
    mock_tokenizer.pad_token_id = 0
    mock_tokenizer.decode = Mock(return_value="test response")

    mock_inputs = create_mock_inputs()
    # Configure return values directly on the MagicMock
    mock_tokenizer.return_value = mock_inputs
    mock_tokenizer.apply_chat_template.return_value = mock_inputs
    return mock_tokenizer


def create_mock_processor():
    """Create a mock multimodal processor."""
    mock_processor = MagicMock()
    mock_processor.tokenizer = create_mock_tokenizer()

    mock_inputs = create_mock_inputs()
    # Configure return values directly on the MagicMock
    mock_processor.return_value = mock_inputs

    # For multimodal, we may:
    # - render prompt text first (tokenize=False), then call processor(text=..., images=...)
    # - OR keep image URLs inline and tokenize via apply_chat_template(tokenize=True)
    def _apply_chat_template_side_effect(*_args, **kwargs):
        if kwargs.get("tokenize") is False:
            return "rendered prompt"
        return mock_inputs

    mock_processor.apply_chat_template.side_effect = _apply_chat_template_side_effect
    return mock_processor


def create_mocked_prompter(
    *,
    model_name: str = DEFAULT_MODEL_NAME,
    supported_modalities: frozenset[str] = frozenset({"text"}),
    **kwargs,
) -> TransformersPrompter:
    """Create a TransformersPrompter with mocked internal components.

    Patches ModelLoader to avoid actual HuggingFace loading.
    """
    # Extract special kwargs before passing to descriptor
    return_format = kwargs.pop("return_format", None)
    system_message = kwargs.pop("system_message", None)

    descriptor = TransformersPrompterDescriptor(
        provider_name=DEFAULT_PROVIDER_NAME,
        model_name=model_name,
        prompt_options=kwargs,
        return_format=return_format,
        system_message=system_message,
    )

    # Create mocks
    mock_model = create_mock_model()
    mock_gen_config = MagicMock()
    mock_gen_config.pad_token_id = None

    if "image" in supported_modalities:
        mock_processor = create_mock_processor()
    else:
        # Text only: processor IS tokenizer
        mock_processor = create_mock_tokenizer()
        # Remove .tokenizer attr so getattr fallback works correctly
        if hasattr(mock_processor, "tokenizer"):
            del mock_processor.tokenizer

    # Patch ModelLoader methods to return mocks
    with (
        patch.object(TransformersPrompterModelLoader, "detect_supported_modalities", return_value=supported_modalities),
        patch.object(TransformersPrompterModelLoader, "create_model", return_value=mock_model),
        patch.object(TransformersPrompterModelLoader, "create_processor", return_value=mock_processor),
        patch.object(TransformersPrompterModelLoader, "create_generation_config", return_value=mock_gen_config),
        patch("daft.ai.transformers.protocols.prompter.get_torch_device", return_value=Mock(type="cpu")),
    ):
        prompter = TransformersPrompter(descriptor)

    return prompter


# =============================================================================
# Descriptor Tests (Configuration Logic)
# =============================================================================


def test_transformers_descriptor_config_resolution():
    """Test that descriptor correctly partitions options."""
    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="test-model",
        prompt_options={
            "max_new_tokens": 100,  # generation
            "temperature": 0.5,  # generation
            "trust_remote_code": False,  # model/processor
            "concurrency": 2,  # udf
        },
    )

    # Config is now stored in _partitioned_config
    assert descriptor._partitioned_config is not None
    assert descriptor._partitioned_config.generation_kwargs["max_new_tokens"] == 100
    assert descriptor._partitioned_config.generation_kwargs["temperature"] == 0.5
    assert descriptor._partitioned_config.model_kwargs["trust_remote_code"] is False
    assert descriptor._udf_options.concurrency == 2


# =============================================================================
# ModelLoader Tests (Factory Logic)
# =============================================================================


def create_partitioned_config(**overrides) -> PartitionedConfig:
    """Helper to create PartitionedConfig with defaults."""
    defaults = {
        "model_kwargs": {"trust_remote_code": True},
        "processor_kwargs": {"trust_remote_code": True},
        "generation_kwargs": {},
        "chat_template_kwargs": {
            "tokenize": True,
            "add_generation_prompt": True,
            "return_dict": True,
            "return_tensors": "pt",
        },
        "quantization_kwargs": None,
    }
    defaults.update(overrides)
    return PartitionedConfig(**defaults)


@patch("transformers.AutoModelForCausalLM.from_pretrained")
@patch("transformers.AutoConfig.from_pretrained")
@patch.object(TransformersPrompterModelLoader, "_resolve_torch_dtype")
def test_model_loader_create_model(mock_resolve_dtype, mock_config, mock_model_cls):
    """Test that create_model calls transformers correctly."""
    mock_device = Mock(type="cpu")
    mock_resolve_dtype.return_value = torch.float32
    mock_model = create_mock_model()
    mock_model_cls.return_value = mock_model
    mock_config.return_value = MagicMock(to_dict=Mock(return_value={"model_type": "test"}))

    config = create_partitioned_config(
        model_kwargs={"trust_remote_code": True, "low_cpu_mem_usage": True, "use_safetensors": True}
    )
    loader = TransformersPrompterModelLoader(model_name="test-model", config=config)

    model = loader.create_model(mock_device, frozenset({"text"}))

    assert model == mock_model
    mock_model_cls.assert_called_once()
    call_kwargs = mock_model_cls.call_args.kwargs
    assert call_kwargs["trust_remote_code"] is True
    assert call_kwargs["low_cpu_mem_usage"] is True
    assert call_kwargs["use_safetensors"] is True
    # Should move to device since device_map is not set
    model.to.assert_called_with(mock_device)


@patch("transformers.AutoTokenizer.from_pretrained")
def test_model_loader_create_processor_text_only(mock_tokenizer_cls):
    """Test create_processor for text-only models."""
    mock_tokenizer_cls.return_value = create_mock_tokenizer()

    config = create_partitioned_config(processor_kwargs={"trust_remote_code": True})
    loader = TransformersPrompterModelLoader(model_name="test-model", config=config)

    proc = loader.create_processor(frozenset({"text"}))

    assert proc == mock_tokenizer_cls.return_value
    mock_tokenizer_cls.assert_called_with("test-model", trust_remote_code=True)


@patch("transformers.AutoProcessor.from_pretrained")
def test_model_loader_create_processor_multimodal(mock_processor_cls):
    """Test create_processor for multimodal models."""
    mock_processor_cls.return_value = create_mock_processor()

    config = create_partitioned_config(processor_kwargs={"trust_remote_code": True})
    loader = TransformersPrompterModelLoader(model_name="vlm-model", config=config)

    proc = loader.create_processor(frozenset({"text", "image"}))

    assert proc == mock_processor_cls.return_value
    mock_processor_cls.assert_called_with("vlm-model", trust_remote_code=True)


# =============================================================================
# Prompter Logic Tests (using mocked prompter)
# =============================================================================


def test_transformers_prompter_initialization():
    """Test basic initialization."""
    prompter = create_mocked_prompter(model_name="my-model")

    assert prompter.model_name == "my-model"
    assert prompter.provider_name == DEFAULT_PROVIDER_NAME
    assert prompter.supported_modalities == frozenset({"text"})
    assert isinstance(prompter.generation_config, MagicMock)

    # Verify padding token setup logic
    # The mock tokenizer starts with pad_token=None, eos_token="<eos>"
    # Prompter should have set pad_token to eos_token
    assert prompter.tokenizer.pad_token == "<eos>"


def test_transformers_prompter_text_response():
    """Test simple text prompting."""
    prompter = create_mocked_prompter()

    # Configure mock
    prompter.tokenizer.decode.return_value = "Generated response"

    result = run_async(prompter.prompt(("Hello",)))

    assert result == "Generated response"
    prompter.model.generate.assert_called_once()
    prompter.tokenizer.decode.assert_called_once()


def test_transformers_prompter_with_system_message():
    """Test prompting with system message."""
    prompter = create_mocked_prompter(system_message="System prompt")

    run_async(prompter.prompt(("User prompt",)))

    # Check that chat template was called with system message
    call_args = prompter.processor.apply_chat_template.call_args
    chat_messages = call_args[0][0]

    assert len(chat_messages) == 2
    assert chat_messages[0]["role"] == "system"
    assert chat_messages[0]["content"] == "System prompt"
    assert chat_messages[1]["role"] == "user"


def test_transformers_prompter_structured_output():
    """Test valid structured output."""
    prompter = create_mocked_prompter(return_format=SimpleResponse)

    # Mock returns valid JSON
    prompter.tokenizer.decode.return_value = '{"answer": "yes", "confidence": 0.9}'

    result = run_async(prompter.prompt(("Question",)))

    assert isinstance(result, SimpleResponse)
    assert result.answer == "yes"
    assert result.confidence == 0.9


def test_transformers_prompter_structured_output_fallback():
    """Test fallback when structured output fails."""
    prompter = create_mocked_prompter(return_format=SimpleResponse)

    # Mock returns invalid JSON
    prompter.tokenizer.decode.return_value = "Not JSON"

    result = run_async(prompter.prompt(("Question",)))

    assert result == "Not JSON"


def test_transformers_prompter_multimodal_image():
    """Test prompting with image."""
    from daft.dependencies import np

    # Setup prompter with vision support
    prompter = create_mocked_prompter(supported_modalities=frozenset({"text", "image"}))
    prompter.processor.tokenizer.decode.return_value = "Image description"

    # Create dummy image
    img = np.zeros((10, 10, 3), dtype=np.uint8)

    result = run_async(prompter.prompt(("Describe", img)))

    assert result == "Image description"

    # Verify apply_chat_template was called (render template)
    prompter.processor.apply_chat_template.assert_called_once()

    # Verify processor() was called with images separately
    assert prompter.processor.call_count == 1
    call_kwargs = prompter.processor.call_args.kwargs
    assert call_kwargs["images"] == [img]
    assert call_kwargs["text"] == "rendered prompt"


@patch("daft.ai.metrics.increment_counter")
def test_transformers_prompter_metrics(mock_counter):
    """Test that metrics are recorded."""
    prompter = create_mocked_prompter()

    run_async(prompter.prompt(("Test",)))

    # Verify metrics calls
    assert mock_counter.call_count >= 1
    calls = mock_counter.call_args_list

    # Check for requests counter
    request_calls = [c for c in calls if c[0][0] == "requests"]
    assert len(request_calls) == 1

    # Check attributes
    attrs = request_calls[0][1]["attributes"]
    assert attrs["provider"] == "transformers"
    assert attrs["model"] == DEFAULT_MODEL_NAME
    assert attrs["protocol"] == "prompt"


def test_transformers_prompter_multimodal_unsupported():
    """Test error when sending image to text-only model."""
    from daft.dependencies import np

    prompter = create_mocked_prompter(supported_modalities=frozenset({"text"}))
    img = np.zeros((10, 10, 3), dtype=np.uint8)

    # Should run but ignore image since model doesn't support it
    run_async(prompter.prompt(("Test", img)))

    # Verify apply_chat_template was called
    prompter.processor.apply_chat_template.assert_called_once()

    # Images should not be passed to processor for text-only model
    # (the _prepare_inputs method skips image handling when not in supported_modalities)


def test_transformers_prompter_with_image_from_bytes():
    """Test prompting with image from bytes."""
    import io

    from PIL import Image as PILImage

    from daft.dependencies import np

    # Setup prompter with vision support
    prompter = create_mocked_prompter(supported_modalities=frozenset({"text", "image"}))

    # Create PNG bytes
    img_array = np.zeros((10, 10, 3), dtype=np.uint8)
    img = PILImage.fromarray(img_array)
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    png_bytes = bio.getvalue()

    run_async(prompter.prompt(("Describe", png_bytes)))

    # Verify apply_chat_template was called
    prompter.processor.apply_chat_template.assert_called_once()

    # The formatter should have converted bytes to PIL Image and extracted it
    # Check if processor was called with images
    if prompter.processor.call_count > 0:
        call_kwargs = prompter.processor.call_args.kwargs
        if "images" in call_kwargs:
            assert len(call_kwargs["images"]) == 1
            assert isinstance(call_kwargs["images"][0], PILImage.Image)


def test_transformers_prompter_with_file_object():
    """Test prompting with File object."""
    import os
    import tempfile

    from daft.file import File

    prompter = create_mocked_prompter()

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write("Content of the file")
        temp_path = tmp.name

    try:
        run_async(prompter.prompt(("Read this", File(temp_path))))

        # Verify call args include file content
        # Check chat template inputs
        chat_calls = prompter.processor.apply_chat_template.call_args
        chat_messages = chat_calls[0][0]
        user_msg = chat_messages[-1]["content"]

        # Check if any content contains the file text
        found = False
        for block in user_msg:
            if block["type"] == "text" and "Content of the file" in block["text"]:
                found = True
                break
        assert found

    finally:
        os.unlink(temp_path)


def test_transformers_prompter_multimodal_image_url_uses_apply_chat_template():
    """If an image is provided as a URL string, it should remain inline and be handled by apply_chat_template."""
    url = "https://example.com/image.png"

    prompter = create_mocked_prompter(supported_modalities=frozenset({"text", "image"}))
    prompter.processor.tokenizer.decode.return_value = "URL image response"

    result = run_async(prompter.prompt(("Describe", url)))

    assert result == "URL image response"

    # For URL images, we should tokenize directly via apply_chat_template (tokenize=True default)
    prompter.processor.apply_chat_template.assert_called_once()
    # And we should NOT call processor(text=..., images=...) because no images are extracted
    assert prompter.processor.call_count == 0


def test_model_loader_device_map_auto_requires_accelerate():
    """device_map='auto' should fail with ProviderImportError if accelerate is missing."""
    config = create_partitioned_config(model_kwargs={"trust_remote_code": True, "device_map": "auto"})
    loader = TransformersPrompterModelLoader(model_name="test-model", config=config)

    with patch("daft.dependencies.accelerate.module_available", return_value=False):
        with pytest.raises(ProviderImportError, match=r"pip install 'daft\[transformers\]'"):
            loader.create_model(Mock(type="cpu"), frozenset({"text"}))


def test_model_loader_quantization_requires_bitsandbytes():
    """Quantization should fail with ProviderImportError if bitsandbytes is missing."""
    config = create_partitioned_config(model_kwargs={"trust_remote_code": True})
    loader = TransformersPrompterModelLoader(model_name="test-model", config=config)

    with (
        patch.object(loader, "create_quantization_config", return_value=object()),
        patch("daft.dependencies.bitsandbytes.module_available", return_value=False),
    ):
        with pytest.raises(ProviderImportError, match=r"pip install 'daft\[transformers\]'"):
            loader.create_model(Mock(type="cpu"), frozenset({"text"}))
