"""Unit tests for the Transformers Prompter.

Tests cover:
- Provider tests (get_prompter with various options)
- Descriptor tests (instantiation, option partitioning, getters)
- Model Loader tests (vision vs causal detection, processor selection)
- Message Processor tests (all content types, system message)
- Prompter tests (prompt method, structured output, metrics)
"""

from __future__ import annotations

import asyncio
import io
import os
import tempfile
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("transformers")
pytest.importorskip("torch")

import numpy as np
import torch
from PIL import Image as PILImage
from pydantic import BaseModel

from daft.ai.protocols import Prompter
from daft.ai.transformers.protocols.prompter import (
    TransformersPrompter,
    TransformersPrompterDescriptor,
)
from daft.ai.transformers.protocols.prompter.messages import (
    TransformersPrompterMessageProcessor,
)
from daft.ai.transformers.provider import TransformersProvider
from daft.file import File


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


class ImageAnalysis(BaseModel):
    """Pydantic model for image analysis tests."""

    dominant_color: str
    description: str


DEFAULT_MODEL_NAME = "Qwen/Qwen3-VL-2B-instruct"
DEFAULT_TEXT_MODEL_NAME = "Qwen/Qwen3-0.6B"


# =============================================================================
# Provider Tests
# =============================================================================


def test_transformers_provider_get_prompter_default():
    """Test that the provider returns a prompter expression with default settings."""
    provider = TransformersProvider()
    # get_prompter returns an expression, not a descriptor directly
    expression = provider.get_prompter()

    # The expression should be callable (it's a UDF-wrapped class)
    assert expression is not None
    # Check that the default model is used
    assert provider.DEFAULT_PROMPTER == "Qwen/Qwen3-VL-2B-instruct"


def test_transformers_provider_get_prompter_with_model():
    """Test that the provider accepts custom model names."""
    provider = TransformersProvider()
    expression = provider.get_prompter(model="Qwen/Qwen3-0.6B")

    assert expression is not None


def test_transformers_provider_get_prompter_with_return_format_raises():
    """Test that the provider raises error for return_format (not yet supported)."""
    provider = TransformersProvider()
    # return_format is accepted at descriptor level but will raise when instantiated
    # This is because structured output requires outlines library (future PR)
    descriptor = provider.get_prompter(model=DEFAULT_MODEL_NAME, return_format=SimpleResponse)
    assert descriptor is not None
    assert descriptor.return_format == SimpleResponse


def test_transformers_provider_get_prompter_with_system_message():
    """Test that the provider accepts system_message parameter."""
    provider = TransformersProvider()
    expression = provider.get_prompter(
        model=DEFAULT_MODEL_NAME,
        system_message="You are a helpful assistant.",
    )

    assert expression is not None


def test_transformers_provider_get_prompter_with_generation_options():
    """Test that the provider accepts generation config options."""
    provider = TransformersProvider()
    expression = provider.get_prompter(
        model=DEFAULT_MODEL_NAME,
        temperature=0.7,
        max_new_tokens=100,
        do_sample=True,
    )

    assert expression is not None


# =============================================================================
# Descriptor Tests
# =============================================================================


def test_transformers_prompter_descriptor_instantiation():
    """Test that descriptor can be instantiated directly."""
    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name=DEFAULT_MODEL_NAME,
        prompt_options={},
    )

    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == DEFAULT_MODEL_NAME
    assert descriptor.get_options() == {}
    assert descriptor.return_format is None  # Check attribute directly, get_return_format() raises
    assert descriptor.get_system_message() is None


def test_transformers_prompter_descriptor_with_return_format_raises():
    """Test descriptor with return_format raises error (not supported yet)."""
    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name=DEFAULT_MODEL_NAME,
        return_format=SimpleResponse,
        prompt_options={},
    )

    # Attribute is stored
    assert descriptor.return_format == SimpleResponse
    # But getter raises because structured output requires outlines (future PR)
    with pytest.raises(ValueError, match="return_format is not supported"):
        descriptor.get_return_format()


def test_transformers_prompter_descriptor_with_system_message():
    """Test descriptor with system_message."""
    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name=DEFAULT_MODEL_NAME,
        system_message="You are a helpful assistant.",
        prompt_options={},
    )

    assert descriptor.system_message == "You are a helpful assistant."
    assert descriptor.get_system_message() == "You are a helpful assistant."


def test_transformers_prompter_descriptor_option_partitioning():
    """Test that options are correctly split into different config categories."""
    prompt_options = {
        # Model loading options
        "trust_remote_code": True,
        "torch_dtype": "bfloat16",
        "device_map": "auto",
        # Processor options
        "use_fast": True,
        # Generation config options
        "temperature": 0.7,
        "max_new_tokens": 100,
        "do_sample": True,
        # Chat template options
        "add_generation_prompt": True,
    }

    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name=DEFAULT_MODEL_NAME,
        prompt_options=prompt_options,
    )

    # Check model loading options extracted
    model_loading_opts = descriptor.get_model_loading_options()
    assert model_loading_opts.get("trust_remote_code") is True
    assert model_loading_opts.get("torch_dtype") == "bfloat16"
    assert model_loading_opts.get("device_map") == "auto"

    # Check processor options extracted
    processor_opts = descriptor.get_processor_options()
    assert processor_opts.get("use_fast") is True

    # Check generation config options extracted
    gen_opts = descriptor.get_generation_config_options()
    assert gen_opts.get("temperature") == 0.7
    assert gen_opts.get("max_new_tokens") == 100
    assert gen_opts.get("do_sample") is True

    # Check chat template options extracted
    chat_opts = descriptor.get_chat_template_options()
    assert chat_opts.get("add_generation_prompt") is True


def test_transformers_prompter_descriptor_getters():
    """Test all getter methods on the descriptor."""
    descriptor = TransformersPrompterDescriptor(
        provider_name="custom-transformers",
        model_name="test-model",
        system_message="System prompt",
        prompt_options={"temperature": 0.5},
    )

    assert descriptor.get_provider() == "custom-transformers"
    assert descriptor.get_model() == "test-model"
    assert descriptor.get_system_message() == "System prompt"
    # Note: get_return_format() is not tested here as it raises for non-None values
    # Structured output support requires outlines library (future PR)


def test_transformers_prompter_descriptor_instantiate():
    """Test that descriptor can instantiate a TransformersPrompter."""
    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch(
            "daft.ai.transformers.protocols.prompter.model_loader.AutoModelForCausalLM.from_pretrained"
        ) as mock_model,
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoTokenizer.from_pretrained") as mock_tokenizer,
        patch("daft.ai.transformers.protocols.prompter.model_loader.GenerationConfig") as mock_gen_config,
    ):
        # Setup mocks
        mock_config_instance = Mock()
        mock_config_instance.vision_config = None  # Text-only model
        mock_config.return_value = mock_config_instance

        mock_model_instance = Mock()
        mock_model_instance.to = Mock(return_value=mock_model_instance)
        mock_model.return_value = mock_model_instance

        mock_tokenizer_instance = Mock()
        mock_tokenizer.return_value = mock_tokenizer_instance

        mock_gen_config.return_value = Mock()

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=DEFAULT_TEXT_MODEL_NAME,
            prompt_options={},
        )

        prompter = descriptor.instantiate()

        assert isinstance(prompter, TransformersPrompter)
        assert isinstance(prompter, Prompter)
        assert prompter.model_name == DEFAULT_TEXT_MODEL_NAME
        assert prompter.provider_name == "transformers"


# =============================================================================
# Model Loader Tests
# =============================================================================


def test_model_loader_detects_vision_model():
    """Test that model loader uses AutoModelForImageTextToText for vision models."""
    from daft.ai.transformers.protocols.prompter.model_loader import TransformersPrompterModelLoader

    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch(
            "daft.ai.transformers.protocols.prompter.model_loader.AutoModelForImageTextToText.from_pretrained"
        ) as mock_vision_model,
    ):
        # Setup mock - vision model has vision_config
        mock_config_instance = Mock()
        mock_config_instance.vision_config = Mock()  # Has vision config
        mock_config.return_value = mock_config_instance

        mock_model_instance = Mock()
        mock_model_instance.to = Mock(return_value=mock_model_instance)
        mock_vision_model.return_value = mock_model_instance

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=DEFAULT_MODEL_NAME,
            prompt_options={},
        )

        loader = TransformersPrompterModelLoader(descriptor)
        loader.create_model()

        # Should have called AutoModelForImageTextToText, not AutoModelForCausalLM
        mock_vision_model.assert_called_once()


def test_model_loader_detects_text_model():
    """Test that model loader uses AutoModelForCausalLM for text-only models."""
    from daft.ai.transformers.protocols.prompter.model_loader import TransformersPrompterModelLoader

    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch(
            "daft.ai.transformers.protocols.prompter.model_loader.AutoModelForCausalLM.from_pretrained"
        ) as mock_causal_model,
    ):
        # Setup mock - text model has no vision_config
        mock_config_instance = Mock()
        mock_config_instance.vision_config = None  # No vision config
        mock_config.return_value = mock_config_instance

        mock_model_instance = Mock()
        mock_model_instance.to = Mock(return_value=mock_model_instance)
        mock_causal_model.return_value = mock_model_instance

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=DEFAULT_TEXT_MODEL_NAME,
            prompt_options={},
        )

        loader = TransformersPrompterModelLoader(descriptor)
        loader.create_model()

        # Should have called AutoModelForCausalLM
        mock_causal_model.assert_called_once()


def test_model_loader_selects_processor_for_vision():
    """Test that model loader uses AutoProcessor for vision models."""
    from daft.ai.transformers.protocols.prompter.model_loader import TransformersPrompterModelLoader

    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoProcessor.from_pretrained") as mock_processor,
    ):
        # Setup mock - vision model
        mock_config_instance = Mock()
        mock_config_instance.vision_config = Mock()
        mock_config.return_value = mock_config_instance

        mock_processor_instance = Mock()
        mock_processor.return_value = mock_processor_instance

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=DEFAULT_MODEL_NAME,
            prompt_options={},
        )

        loader = TransformersPrompterModelLoader(descriptor)
        result = loader.create_processor()

        mock_processor.assert_called_once()
        assert result == mock_processor_instance


def test_model_loader_selects_tokenizer_for_text():
    """Test that model loader uses AutoTokenizer for text-only models."""
    from daft.ai.transformers.protocols.prompter.model_loader import TransformersPrompterModelLoader

    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoTokenizer.from_pretrained") as mock_tokenizer,
    ):
        # Setup mock - text model
        mock_config_instance = Mock()
        mock_config_instance.vision_config = None
        mock_config.return_value = mock_config_instance

        mock_tokenizer_instance = Mock()
        mock_tokenizer.return_value = mock_tokenizer_instance

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=DEFAULT_TEXT_MODEL_NAME,
            prompt_options={},
        )

        loader = TransformersPrompterModelLoader(descriptor)
        result = loader.create_processor()

        mock_tokenizer.assert_called_once()
        assert result == mock_tokenizer_instance


def test_model_loader_applies_device_map():
    """Test that model loader applies device_map when specified."""
    from daft.ai.transformers.protocols.prompter.model_loader import TransformersPrompterModelLoader

    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch(
            "daft.ai.transformers.protocols.prompter.model_loader.AutoModelForCausalLM.from_pretrained"
        ) as mock_model,
    ):
        mock_config_instance = Mock()
        mock_config_instance.vision_config = None
        mock_config.return_value = mock_config_instance

        mock_model_instance = Mock()
        mock_model_instance.to = Mock(return_value=mock_model_instance)
        mock_model.return_value = mock_model_instance

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=DEFAULT_TEXT_MODEL_NAME,
            prompt_options={"device_map": "auto"},
        )

        loader = TransformersPrompterModelLoader(descriptor)
        loader.create_model()

        # When device_map is specified, model.to() should NOT be called
        call_kwargs = mock_model.call_args.kwargs
        assert call_kwargs.get("device_map") == "auto"
        # model.to() should not be called when device_map is set
        mock_model_instance.to.assert_not_called()


def test_model_loader_generation_config():
    """Test that GenerationConfig is created from options."""
    from daft.ai.transformers.protocols.prompter.model_loader import TransformersPrompterModelLoader

    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch("daft.ai.transformers.protocols.prompter.model_loader.GenerationConfig") as mock_gen_config,
    ):
        mock_config_instance = Mock()
        mock_config_instance.vision_config = None
        mock_config.return_value = mock_config_instance

        mock_gen_config_instance = Mock()
        mock_gen_config.return_value = mock_gen_config_instance

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=DEFAULT_TEXT_MODEL_NAME,
            prompt_options={
                "temperature": 0.8,
                "max_new_tokens": 200,
                "do_sample": True,
            },
        )

        loader = TransformersPrompterModelLoader(descriptor)
        result = loader.create_generation_config()

        mock_gen_config.assert_called_once()
        call_kwargs = mock_gen_config.call_args.kwargs
        assert call_kwargs.get("temperature") == 0.8
        assert call_kwargs.get("max_new_tokens") == 200
        assert call_kwargs.get("do_sample") is True
        assert result == mock_gen_config_instance


# =============================================================================
# Message Processor Tests
# =============================================================================


def test_message_processor_string():
    """Test that plain strings are converted to text content parts."""
    processor = TransformersPrompterMessageProcessor()
    messages = processor.process_messages(("Hello, world!",))

    assert len(messages) == 1
    assert messages[0]["role"] == "user"
    assert len(messages[0]["content"]) == 1
    assert messages[0]["content"][0] == {"type": "text", "text": "Hello, world!"}


def test_message_processor_string_image_url():
    """Test that HTTP URLs are detected as image URLs."""
    processor = TransformersPrompterMessageProcessor()
    messages = processor.process_messages(("https://example.com/image.png",))

    assert len(messages) == 1
    assert messages[0]["content"][0] == {"type": "image", "url": "https://example.com/image.png"}


def test_message_processor_string_data_url():
    """Test that data URLs are detected as image URLs."""
    processor = TransformersPrompterMessageProcessor()
    data_url = "data:image/png;base64,iVBORw0KGgo="
    messages = processor.process_messages((data_url,))

    assert len(messages) == 1
    assert messages[0]["content"][0] == {"type": "image", "url": data_url}


def test_message_processor_bytes_image():
    """Test that image bytes are converted to PIL Image content parts."""
    processor = TransformersPrompterMessageProcessor()

    # Create a simple PNG image as bytes
    img = PILImage.new("RGB", (10, 10), color="red")
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    image_bytes = bio.getvalue()

    messages = processor.process_messages((image_bytes,))

    assert len(messages) == 1
    content = messages[0]["content"][0]
    assert content["type"] == "image"
    assert "image" in content
    # Should be a PIL Image
    assert isinstance(content["image"], PILImage.Image)


def test_message_processor_bytes_text():
    """Test that non-image bytes are converted to text."""
    processor = TransformersPrompterMessageProcessor()

    text_bytes = b"Hello, this is plain text"
    messages = processor.process_messages((text_bytes,))

    assert len(messages) == 1
    content = messages[0]["content"][0]
    assert content["type"] == "text"
    assert content["text"] == "Hello, this is plain text"


def test_message_processor_file_image():
    """Test that image files are converted to PIL Image content parts."""
    processor = TransformersPrompterMessageProcessor()

    # Create a temporary image file
    img = PILImage.new("RGB", (10, 10), color="blue")
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        img.save(tmp.name)
        temp_path = tmp.name

    try:
        file_obj = File(temp_path)
        messages = processor.process_messages((file_obj,))

        assert len(messages) == 1
        content = messages[0]["content"][0]
        assert content["type"] == "image"
        assert "image" in content
        assert isinstance(content["image"], PILImage.Image)
    finally:
        os.unlink(temp_path)


def test_message_processor_file_text():
    """Test that text files are wrapped in XML tags."""
    processor = TransformersPrompterMessageProcessor()

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write("This is the document content.")
        temp_path = tmp.name

    try:
        file_obj = File(temp_path)
        messages = processor.process_messages((file_obj,))

        assert len(messages) == 1
        content = messages[0]["content"][0]
        assert content["type"] == "text"
        assert content["text"] == "<file_text_plain>This is the document content.</file_text_plain>"
    finally:
        os.unlink(temp_path)


def test_message_processor_numpy_array():
    """Test that numpy arrays are converted to image content parts."""
    processor = TransformersPrompterMessageProcessor()

    image_array = np.zeros((10, 10, 3), dtype=np.uint8)
    image_array[:, :, 0] = 255  # Red

    messages = processor.process_messages((image_array,))

    assert len(messages) == 1
    content = messages[0]["content"][0]
    assert content["type"] == "image"
    assert "image" in content
    # Should be the numpy array
    assert isinstance(content["image"], np.ndarray)


def test_message_processor_pil_image():
    """Test that PIL Images are passed through as image content parts."""
    processor = TransformersPrompterMessageProcessor()

    img = PILImage.new("RGB", (10, 10), color="green")
    messages = processor.process_messages((img,))

    assert len(messages) == 1
    content = messages[0]["content"][0]
    assert content["type"] == "image"
    assert content["image"] == img


def test_message_processor_with_system_message():
    """Test that system message is prepended correctly."""
    processor = TransformersPrompterMessageProcessor()
    messages = processor.process_messages(
        ("Hello!",),
        system_message="You are a helpful assistant.",
    )

    assert len(messages) == 2

    # First message should be system
    assert messages[0]["role"] == "system"
    assert messages[0]["content"] == [{"type": "text", "text": "You are a helpful assistant."}]

    # Second message should be user
    assert messages[1]["role"] == "user"
    assert messages[1]["content"] == [{"type": "text", "text": "Hello!"}]


def test_message_processor_multiple_parts():
    """Test processing multiple message parts."""
    processor = TransformersPrompterMessageProcessor()

    img = PILImage.new("RGB", (10, 10), color="red")
    messages = processor.process_messages(("What color is this?", img))

    assert len(messages) == 1
    assert messages[0]["role"] == "user"
    assert len(messages[0]["content"]) == 2
    assert messages[0]["content"][0] == {"type": "text", "text": "What color is this?"}
    assert messages[0]["content"][1]["type"] == "image"


def test_message_processor_unsupported_type():
    """Test that unsupported types raise ValueError."""
    processor = TransformersPrompterMessageProcessor()

    class UnsupportedType:
        pass

    with pytest.raises(ValueError, match="Unsupported content type"):
        processor.process_messages((UnsupportedType(),))


def test_message_processor_raises_without_pillow_on_image_bytes():
    """Test that image processing fails without Pillow."""
    processor = TransformersPrompterMessageProcessor()

    # Create valid PNG bytes
    img = PILImage.new("RGB", (10, 10), color="red")
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    image_bytes = bio.getvalue()

    with patch("daft.dependencies.pil_image.module_available", return_value=False):
        with pytest.raises(
            ImportError,
            match=r"Please `pip install 'daft\[transformers\]'` to use the prompt function with this provider",
        ):
            processor._build_image_from_bytes(image_bytes)


# =============================================================================
# Prompter Tests
# =============================================================================


def _create_mock_prompter(
    *,
    model_name: str = DEFAULT_TEXT_MODEL_NAME,
    system_message: str | None = None,
    is_vision: bool = False,
    generated_text: str = "Test response",
) -> TransformersPrompter:
    """Helper to create a prompter with mocked dependencies.

    Note: return_format is not supported yet (requires outlines library, future PR).
    """
    with (
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoConfig.from_pretrained") as mock_config,
        patch(
            "daft.ai.transformers.protocols.prompter.model_loader.AutoModelForCausalLM.from_pretrained"
        ) as mock_causal_model,
        patch(
            "daft.ai.transformers.protocols.prompter.model_loader.AutoModelForImageTextToText.from_pretrained"
        ) as mock_vision_model,
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoTokenizer.from_pretrained") as mock_tokenizer,
        patch("daft.ai.transformers.protocols.prompter.model_loader.AutoProcessor.from_pretrained") as mock_processor,
        patch("daft.ai.transformers.protocols.prompter.model_loader.GenerationConfig") as mock_gen_config,
    ):
        # Setup config mock
        mock_config_instance = Mock()
        mock_config_instance.vision_config = Mock() if is_vision else None
        mock_config.return_value = mock_config_instance

        # Setup model mock
        mock_model_instance = Mock()
        mock_model_instance.device = torch.device("cpu")
        mock_model_instance.to = Mock(return_value=mock_model_instance)
        # Generate returns token IDs
        mock_model_instance.generate = Mock(return_value=torch.tensor([[1, 2, 3, 4, 5]]))

        if is_vision:
            mock_vision_model.return_value = mock_model_instance
        else:
            mock_causal_model.return_value = mock_model_instance

        # Setup processor/tokenizer mock
        mock_proc_instance = Mock()
        mock_proc_instance.apply_chat_template = Mock(
            return_value={"input_ids": torch.tensor([[1, 2, 3]]), "attention_mask": torch.tensor([[1, 1, 1]])}
        )
        mock_proc_instance.decode = Mock(return_value=generated_text)

        if is_vision:
            mock_processor.return_value = mock_proc_instance
        else:
            mock_tokenizer.return_value = mock_proc_instance

        mock_gen_config.return_value = Mock()

        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name=model_name,
            system_message=system_message,
            prompt_options={},
        )

        prompter = TransformersPrompter(descriptor)
        return prompter


def test_transformers_prompter_plain_text():
    """Test prompting with plain text response."""

    async def _test():
        prompter = _create_mock_prompter(generated_text="This is the response.")

        result = await prompter.prompt(("Hello, world!",))

        assert result == "This is the response."

    run_async(_test())


def test_transformers_prompter_with_system_message():
    """Test prompting with system message."""

    async def _test():
        prompter = _create_mock_prompter(
            system_message="You are a helpful assistant.",
            generated_text="I am here to help.",
        )

        result = await prompter.prompt(("Hello!",))

        assert result == "I am here to help."
        assert prompter.system_message == "You are a helpful assistant."

    run_async(_test())


def test_transformers_prompter_with_image_numpy():
    """Test prompting with image (numpy array)."""

    async def _test():
        prompter = _create_mock_prompter(
            is_vision=True,
            generated_text="This image shows a red square.",
        )

        image = np.zeros((10, 10, 3), dtype=np.uint8)
        image[:, :, 0] = 255  # Red

        result = await prompter.prompt(("What is in this image?", image))

        assert result == "This image shows a red square."

    run_async(_test())


def test_transformers_prompter_records_usage_metrics():
    """Test that token metrics are recorded correctly."""

    async def _test():
        prompter = _create_mock_prompter(generated_text="Response")

        with patch("daft.ai.metrics.increment_counter") as mock_counter:
            await prompter.prompt(("Hello",))

        # Should have recorded input, output, and total tokens
        calls = mock_counter.call_args_list
        assert len(calls) >= 3

        # Check for expected metric names
        metric_names = [c[0][0] for c in calls]
        assert "input tokens" in metric_names
        assert "output tokens" in metric_names
        assert "total tokens" in metric_names

    run_async(_test())


def test_transformers_prompter_error_handling():
    """Test that errors from model propagate correctly."""

    async def _test():
        prompter = _create_mock_prompter()
        # Make generate raise an error
        prompter.model.generate = Mock(side_effect=RuntimeError("Model error"))

        with pytest.raises(RuntimeError, match="Model error"):
            await prompter.prompt(("This will fail",))

    run_async(_test())


def test_protocol_compliance():
    """Test that TransformersPrompter implements the Prompter protocol."""
    prompter = _create_mock_prompter()

    assert isinstance(prompter, Prompter)
    assert hasattr(prompter, "prompt")
    assert callable(prompter.prompt)


def test_transformers_prompter_attributes():
    """Test that TransformersPrompter stores attributes correctly."""
    prompter = _create_mock_prompter(
        model_name="test-model",
        system_message="You are helpful.",
    )

    assert prompter.model_name == "test-model"
    assert prompter.system_message == "You are helpful."
    assert prompter.return_format is None  # return_format not supported yet (outlines PR)
    assert prompter.provider_name == "transformers"


def test_transformers_prompter_with_pil_image():
    """Test prompting with PIL Image input."""

    async def _test():
        prompter = _create_mock_prompter(
            is_vision=True,
            generated_text="A green square image.",
        )

        img = PILImage.new("RGB", (10, 10), color="green")

        result = await prompter.prompt(("What is this?", img))

        assert result == "A green square image."

    run_async(_test())


def test_transformers_prompter_with_image_bytes():
    """Test prompting with image bytes."""

    async def _test():
        prompter = _create_mock_prompter(
            is_vision=True,
            generated_text="A blue square.",
        )

        # Create PNG bytes
        img = PILImage.new("RGB", (10, 10), color="blue")
        bio = io.BytesIO()
        img.save(bio, format="PNG")
        image_bytes = bio.getvalue()

        result = await prompter.prompt(("Describe this", image_bytes))

        assert result == "A blue square."

    run_async(_test())


def test_transformers_prompter_with_file_image():
    """Test prompting with image from file."""

    async def _test():
        prompter = _create_mock_prompter(
            is_vision=True,
            generated_text="A yellow square.",
        )

        # Create temp image file
        img = PILImage.new("RGB", (10, 10), color="yellow")
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            img.save(tmp.name)
            temp_path = tmp.name

        try:
            file_obj = File(temp_path)
            result = await prompter.prompt(("What color?", file_obj))

            assert result == "A yellow square."
        finally:
            os.unlink(temp_path)

    run_async(_test())


def test_transformers_prompter_with_text_file():
    """Test prompting with text file."""

    async def _test():
        prompter = _create_mock_prompter(
            generated_text="The document mentions pineapple.",
        )

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
            tmp.write("The secret word is pineapple.")
            temp_path = tmp.name

        try:
            file_obj = File(temp_path)
            result = await prompter.prompt(("What is the secret word?", file_obj))

            assert result == "The document mentions pineapple."
        finally:
            os.unlink(temp_path)

    run_async(_test())


def test_transformers_prompter_multiple_calls():
    """Test making multiple prompt calls."""

    async def _test():
        prompter = _create_mock_prompter()

        # Mock decode to return different responses
        responses = ["First response", "Second response"]
        prompter.processor.decode = Mock(side_effect=responses)

        result1 = await prompter.prompt(("First message",))
        result2 = await prompter.prompt(("Second message",))

        assert result1 == "First response"
        assert result2 == "Second response"

    run_async(_test())


def test_transformers_prompter_record_usage_metrics_custom_provider():
    """Test that metrics use the custom provider name."""
    prompter = _create_mock_prompter()
    prompter.provider_name = "custom-transformers"

    with patch("daft.ai.metrics.increment_counter") as mock_counter:
        prompter._record_usage_metrics(10, 20, 30)

    calls = mock_counter.call_args_list
    for c in calls:
        attrs = c[1]["attributes"]
        assert attrs["provider"] == "custom-transformers"
