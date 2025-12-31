from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
from pydantic import BaseModel

from daft.ai.protocols import Prompter


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


def create_mock_model():
    """Create a mock transformers model."""
    mock_model = MagicMock()
    mock_model.device = Mock()
    mock_model.device.type = "cpu"
    mock_model.eval = Mock()
    mock_model.to = Mock(return_value=mock_model)
    return mock_model


def create_mock_tokenizer():
    """Create a mock tokenizer."""
    mock_tokenizer = MagicMock()
    mock_tokenizer.pad_token = None
    mock_tokenizer.eos_token = "<eos>"
    mock_tokenizer.pad_token_id = 0
    mock_tokenizer.apply_chat_template = Mock(return_value="formatted prompt")
    mock_tokenizer.decode = Mock(return_value="test response")
    mock_tokenizer.__call__ = Mock(return_value=MagicMock(to=Mock(return_value=MagicMock())))
    return mock_tokenizer


def create_mock_processor():
    """Create a mock multimodal processor."""
    mock_processor = MagicMock()
    mock_processor.tokenizer = create_mock_tokenizer()
    mock_processor.apply_chat_template = Mock(return_value="formatted prompt")
    mock_processor.__call__ = Mock(return_value=MagicMock(to=Mock(return_value=MagicMock())))
    return mock_processor


@pytest.fixture
def mock_transformers():
    """Fixture to mock transformers imports."""
    with (
        patch("daft.ai.transformers.protocols.prompter.AutoModelForCausalLM") as mock_auto_model,
        patch("daft.ai.transformers.protocols.prompter.AutoProcessor") as mock_auto_processor,
        patch("daft.ai.transformers.protocols.prompter.AutoTokenizer") as mock_auto_tokenizer,
        patch("daft.ai.transformers.protocols.prompter.get_torch_device") as mock_device,
    ):
        mock_device.return_value = Mock(type="cpu")
        mock_auto_model.from_pretrained = Mock(return_value=create_mock_model())
        mock_auto_tokenizer.from_pretrained = Mock(return_value=create_mock_tokenizer())
        mock_auto_processor.from_pretrained = Mock(side_effect=Exception("Not multimodal"))

        yield {
            "AutoModelForCausalLM": mock_auto_model,
            "AutoProcessor": mock_auto_processor,
            "AutoTokenizer": mock_auto_tokenizer,
            "get_torch_device": mock_device,
        }


@pytest.fixture
def mock_transformers_multimodal():
    """Fixture to mock transformers imports for multimodal models."""
    with (
        patch("daft.ai.transformers.protocols.prompter.AutoModelForCausalLM") as mock_auto_model,
        patch("daft.ai.transformers.protocols.prompter.AutoProcessor") as mock_auto_processor,
        patch("daft.ai.transformers.protocols.prompter.AutoTokenizer") as mock_auto_tokenizer,
        patch("daft.ai.transformers.protocols.prompter.AutoModelForVision2Seq", create=True) as mock_vision_model,
        patch("daft.ai.transformers.protocols.prompter.AutoModel", create=True) as mock_auto_generic,
        patch("daft.ai.transformers.protocols.prompter.get_torch_device") as mock_device,
    ):
        mock_device.return_value = Mock(type="cpu")
        mock_auto_model.from_pretrained = Mock(side_effect=ValueError("Not a causal LM"))
        mock_vision_model.from_pretrained = Mock(return_value=create_mock_model())
        mock_auto_generic.from_pretrained = Mock(return_value=create_mock_model())
        mock_auto_tokenizer.from_pretrained = Mock(return_value=create_mock_tokenizer())
        mock_auto_processor.from_pretrained = Mock(return_value=create_mock_processor())

        yield {
            "AutoModelForCausalLM": mock_auto_model,
            "AutoModelForVision2Seq": mock_vision_model,
            "AutoModel": mock_auto_generic,
            "AutoProcessor": mock_auto_processor,
            "AutoTokenizer": mock_auto_tokenizer,
            "get_torch_device": mock_device,
        }


def create_prompter(
    *,
    provider_name: str = DEFAULT_PROVIDER_NAME,
    model: str = DEFAULT_MODEL_NAME,
    **kwargs: Any,
):
    """Helper to instantiate TransformersPrompter with sensible defaults."""
    from daft.ai.transformers.protocols.prompter import (
        TransformersPrompter,
        TransformersPromptOptions,
    )

    prompt_options = dict(kwargs)
    return_format = prompt_options.pop("return_format", None)
    system_message = prompt_options.pop("system_message", None)

    return TransformersPrompter(
        provider_name=provider_name,
        model=model,
        return_format=return_format,
        system_message=system_message,
        prompt_options=TransformersPromptOptions(**prompt_options) if prompt_options else None,
    )


# ===== Provider Tests =====


def test_transformers_provider_get_prompter_default():
    """Test that the provider returns a prompter descriptor with default settings."""
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter()

    assert isinstance(descriptor, TransformersPrompterDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "microsoft/Phi-3-mini-4k-instruct"
    assert descriptor.return_format is None


def test_transformers_provider_get_prompter_with_model():
    """Test that the provider accepts custom model names."""
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter(model="Qwen/Qwen3-0.6B")

    assert isinstance(descriptor, TransformersPrompterDescriptor)
    assert descriptor.get_model() == "Qwen/Qwen3-0.6B"


def test_transformers_provider_get_prompter_with_return_format():
    """Test that the provider accepts return_format parameter."""
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter(model="test-model", return_format=SimpleResponse)

    assert isinstance(descriptor, TransformersPrompterDescriptor)
    assert descriptor.return_format == SimpleResponse


def test_transformers_provider_get_prompter_with_options():
    """Test that the provider accepts generation config options."""
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter(
        model="test-model",
        temperature=0.7,
        max_new_tokens=100,
    )

    opts = descriptor.get_options()
    assert opts["temperature"] == 0.7
    assert opts["max_new_tokens"] == 100


def test_transformers_provider_get_prompter_with_output_mode():
    """Test that the provider accepts output_mode parameter."""
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter(
        model="test-model",
        output_mode="logprobs",
        num_logprobs=5,
    )

    opts = descriptor.get_options()
    assert opts["output_mode"] == "logprobs"
    assert opts["num_logprobs"] == 5


# ===== Descriptor Tests =====


def test_transformers_prompter_descriptor_instantiation():
    """Test that descriptor can be instantiated directly."""
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor

    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="test-model",
    )

    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "test-model"
    assert descriptor.return_format is None


def test_transformers_prompter_descriptor_with_return_format():
    """Test descriptor with return_format."""
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor

    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="test-model",
        return_format=SimpleResponse,
    )

    assert descriptor.return_format == SimpleResponse


def test_transformers_prompter_descriptor_get_udf_options():
    """Test that descriptor returns appropriate UDF options."""
    from unittest.mock import patch

    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor
    from daft.ai.typing import UDFOptions

    # Mock get_gpu_udf_options to avoid Ray initialization requirement
    mock_udf_options = UDFOptions(num_gpus=1, max_retries=3)
    with patch(
        "daft.ai.transformers.protocols.prompter.get_gpu_udf_options",
        return_value=mock_udf_options,
    ):
        descriptor = TransformersPrompterDescriptor(
            provider_name="transformers",
            model_name="test-model",
        )

        udf_options = descriptor.get_udf_options()
        # UDF options should be returned (num_gpus may be None or a number depending on hardware)
        assert udf_options is not None
        assert hasattr(udf_options, "num_gpus")
        assert hasattr(udf_options, "max_retries")


def test_transformers_prompter_descriptor_get_output_mode():
    """Test that descriptor returns the configured output mode."""
    from daft.ai.transformers.protocols.prompter import (
        TransformersPrompterDescriptor,
        TransformersPromptOptions,
    )

    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="test-model",
        prompt_options=TransformersPromptOptions(output_mode="logprobs"),
    )

    assert descriptor.get_output_mode() == "logprobs"


def test_transformers_prompter_descriptor_default_output_mode():
    """Test that descriptor returns 'text' as default output mode."""
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor

    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="test-model",
    )

    assert descriptor.get_output_mode() == "text"


def test_transformers_prompter_instantiate(mock_transformers):
    """Test that descriptor can instantiate a TransformersPrompter."""
    from daft.ai.transformers.protocols.prompter import (
        TransformersPrompter,
        TransformersPrompterDescriptor,
    )

    descriptor = TransformersPrompterDescriptor(
        provider_name="transformers",
        model_name="test-model",
    )

    prompter = descriptor.instantiate()
    assert isinstance(prompter, TransformersPrompter)
    assert isinstance(prompter, Prompter)
    assert prompter.model_name == "test-model"
    assert prompter.return_format is None
    assert prompter.provider_name == "transformers"


def test_transformers_prompter_descriptor_custom_provider_name(mock_transformers):
    """Test that descriptor forwards custom provider name."""
    from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor

    descriptor = TransformersPrompterDescriptor(
        provider_name="custom-transformers",
        model_name="test-model",
    )

    prompter = descriptor.instantiate()
    assert prompter.provider_name == "custom-transformers"


# ===== Prompter Initialization Tests =====


def test_transformers_prompter_text_model_initialization(mock_transformers):
    """Test that text-only models are loaded correctly."""
    prompter = create_prompter(model="test-model")

    # Verify model was loaded
    mock_transformers["AutoModelForCausalLM"].from_pretrained.assert_called_once()
    assert prompter.is_multimodal is False


def test_transformers_prompter_device_detection_cpu(mock_transformers):
    """Test device detection for CPU."""
    mock_transformers["get_torch_device"].return_value = Mock(type="cpu")

    prompter = create_prompter()

    assert prompter.device.type == "cpu"


def test_transformers_prompter_device_detection_cuda(mock_transformers):
    """Test device detection for CUDA."""
    mock_transformers["get_torch_device"].return_value = Mock(type="cuda")

    prompter = create_prompter()

    assert prompter.device.type == "cuda"


def test_transformers_prompter_device_detection_mps(mock_transformers):
    """Test device detection for MPS (Apple Silicon)."""
    mock_transformers["get_torch_device"].return_value = Mock(type="mps")

    prompter = create_prompter()

    assert prompter.device.type == "mps"


def test_transformers_prompter_padding_token_set():
    """Test that padding token is set from eos token if not available."""
    with (
        patch("daft.ai.transformers.protocols.prompter.AutoModelForCausalLM") as mock_auto_model,
        patch("daft.ai.transformers.protocols.prompter.AutoProcessor") as mock_auto_processor,
        patch("daft.ai.transformers.protocols.prompter.AutoTokenizer") as mock_auto_tokenizer,
        patch("daft.ai.transformers.protocols.prompter.get_torch_device") as mock_device,
    ):
        mock_device.return_value = Mock(type="cpu")
        mock_auto_model.from_pretrained.return_value = create_mock_model()
        mock_auto_processor.from_pretrained.side_effect = Exception("Not multimodal")

        # Create a tokenizer with None pad_token - use a real object to test attribute setting
        class MockTokenizer:
            pad_token = None
            eos_token = "<eos>"
            pad_token_id = None

            def apply_chat_template(self, *args, **kwargs):
                return "formatted"

            def __call__(self, *args, **kwargs):
                return MagicMock(to=Mock(return_value=MagicMock()))

        mock_tokenizer = MockTokenizer()
        mock_auto_tokenizer.from_pretrained.return_value = mock_tokenizer

        prompter = create_prompter()

        # Verify pad_token was set to eos_token
        assert prompter.tokenizer.pad_token == "<eos>"


def test_transformers_prompter_output_mode_text(mock_transformers):
    """Test prompter with default text output mode."""
    prompter = create_prompter()

    assert prompter.output_mode == "text"


def test_transformers_prompter_output_mode_logprobs(mock_transformers):
    """Test prompter with logprobs output mode."""
    prompter = create_prompter(output_mode="logprobs", num_logprobs=5)

    assert prompter.output_mode == "logprobs"
    assert prompter.num_logprobs == 5


def test_transformers_prompter_generation_config(mock_transformers):
    """Test that generation config options are properly stored."""
    prompter = create_prompter(
        max_new_tokens=100,
        temperature=0.8,
        top_p=0.95,
        do_sample=True,
    )

    assert prompter.generation_config["max_new_tokens"] == 100
    assert prompter.generation_config["temperature"] == 0.8
    assert prompter.generation_config["top_p"] == 0.95
    assert prompter.generation_config["do_sample"] is True


def test_transformers_prompter_system_message(mock_transformers):
    """Test that system message is stored."""
    prompter = create_prompter(system_message="You are a helpful assistant.")

    assert prompter.system_message == "You are a helpful assistant."


def test_transformers_prompter_return_format(mock_transformers):
    """Test that return format is stored."""
    prompter = create_prompter(return_format=SimpleResponse)

    assert prompter.return_format == SimpleResponse


# ===== Prompt Method Tests =====


def test_transformers_prompter_plain_text_response(mock_transformers):
    """Test prompting with plain text response."""

    async def _test():
        import torch

        prompter = create_prompter()

        # Mock the model.generate output
        mock_output = torch.tensor([[1, 2, 3, 4, 5]])
        prompter.model.generate = Mock(return_value=mock_output)
        prompter.tokenizer.decode = Mock(return_value="This is a test response.")

        # Mock input preparation
        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        result = await prompter.prompt(("Hello, world!",))

        assert result == "This is a test response."

    run_async(_test())


def test_transformers_prompter_with_system_message_in_prompt(mock_transformers):
    """Test prompting includes system message via _build_chat_messages."""
    prompter = create_prompter(system_message="You are a helpful assistant.")

    # Test the _build_chat_messages method directly
    processed = [{"type": "text", "text": "Hello!"}]
    messages, images = prompter._build_chat_messages(processed)

    # Should have system message first
    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    assert messages[0]["content"] == "You are a helpful assistant."
    assert messages[1]["role"] == "user"


def test_transformers_prompter_records_usage_metrics(mock_transformers):
    """Ensure token/request counters fire."""

    async def _test():
        import torch

        prompter = create_prompter()

        mock_output = torch.tensor([[1, 2, 3, 4, 5]])
        prompter.model.generate = Mock(return_value=mock_output)
        prompter.tokenizer.decode = Mock(return_value="Metrics test response.")

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        with patch("daft.ai.metrics.increment_counter") as mock_counter:
            result = await prompter.prompt(("Record metrics",))

        assert result == "Metrics test response."

        # Verify counters were called
        expected_attrs = {
            "model": DEFAULT_MODEL_NAME,
            "protocol": "prompt",
            "provider": "transformers",
        }

        # Check that requests counter was called
        calls = mock_counter.call_args_list
        request_call = [c for c in calls if c[0][0] == "requests"]
        assert len(request_call) == 1
        assert request_call[0][1]["attributes"] == expected_attrs

    run_async(_test())


def test_transformers_prompter_with_logprobs_output(mock_transformers):
    """Test that logprobs output mode is configured correctly."""
    prompter = create_prompter(output_mode="logprobs", num_logprobs=3)

    # Verify output mode is set correctly
    assert prompter.output_mode == "logprobs"
    assert prompter.num_logprobs == 3


def test_transformers_prompter_extract_logprobs_structure(mock_transformers):
    """Test logprobs extraction returns correct structure."""
    import torch

    prompter = create_prompter(output_mode="logprobs", num_logprobs=2)

    # Create mock scores with known values
    vocab_size = 100
    mock_scores = (
        torch.randn(1, vocab_size),
        torch.randn(1, vocab_size),
    )

    mock_output_ids = torch.tensor([[10, 20, 30, 40]])  # 2 prompt + 2 generated
    input_length = 2

    logprobs = prompter._extract_logprobs(mock_output_ids, mock_scores, input_length)

    # Verify structure
    assert "token_ids" in logprobs
    assert "tokens" in logprobs
    assert "logprobs" in logprobs
    assert "top_logprobs" in logprobs
    assert len(logprobs["token_ids"]) == 2  # 2 generated tokens
    assert len(logprobs["logprobs"]) == 2
    assert len(logprobs["top_logprobs"]) == 2  # 2 positions
    assert len(logprobs["top_logprobs"][0]) == 2  # top-2 alternatives


def test_transformers_prompter_structured_output(mock_transformers):
    """Test prompting with structured output (Pydantic model)."""

    async def _test():
        import torch

        prompter = create_prompter(return_format=SimpleResponse)

        # Mock output that returns valid JSON
        mock_output = torch.tensor([[1, 2, 3, 4, 5]])
        prompter.model.generate = Mock(return_value=mock_output)
        prompter.tokenizer.decode = Mock(return_value='{"answer": "Yes", "confidence": 0.95}')

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        result = await prompter.prompt(("Is this a test?",))

        assert isinstance(result, SimpleResponse)
        assert result.answer == "Yes"
        assert result.confidence == 0.95

    run_async(_test())


def test_transformers_prompter_structured_output_fallback(mock_transformers):
    """Test that invalid JSON returns raw text."""

    async def _test():
        import torch

        prompter = create_prompter(return_format=SimpleResponse)

        # Mock output that returns invalid JSON
        mock_output = torch.tensor([[1, 2, 3, 4, 5]])
        prompter.model.generate = Mock(return_value=mock_output)
        prompter.tokenizer.decode = Mock(return_value="Not valid JSON")

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        result = await prompter.prompt(("Is this a test?",))

        # Should fall back to raw text
        assert result == "Not valid JSON"

    run_async(_test())


def test_transformers_prompter_complex_structured_output(mock_transformers):
    """Test prompting with complex structured output."""

    async def _test():
        import json

        import torch

        prompter = create_prompter(return_format=ComplexResponse)

        expected_output = {
            "summary": "Test summary",
            "key_points": ["Point 1", "Point 2", "Point 3"],
            "sentiment": "positive",
        }

        mock_output = torch.tensor([[1, 2, 3, 4, 5]])
        prompter.model.generate = Mock(return_value=mock_output)
        prompter.tokenizer.decode = Mock(return_value=json.dumps(expected_output))

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        result = await prompter.prompt(("Summarize this text",))

        assert isinstance(result, ComplexResponse)
        assert result.summary == "Test summary"
        assert len(result.key_points) == 3
        assert result.sentiment == "positive"

    run_async(_test())


def test_transformers_prompter_multiple_messages(mock_transformers):
    """Test prompting with multiple sequential messages."""

    async def _test():
        import torch

        prompter = create_prompter()

        # First call
        mock_output1 = torch.tensor([[1, 2, 3]])
        mock_output2 = torch.tensor([[4, 5, 6]])

        call_count = [0]

        def generate_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_output1
            return mock_output2

        prompter.model.generate = Mock(side_effect=generate_side_effect)

        decode_count = [0]

        def decode_side_effect(ids, **kwargs):
            decode_count[0] += 1
            if decode_count[0] == 1:
                return "First response"
            return "Second response"

        prompter.tokenizer.decode = Mock(side_effect=decode_side_effect)

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        result1 = await prompter.prompt(("First message",))
        result2 = await prompter.prompt(("Second message",))

        assert result1 == "First response"
        assert result2 == "Second response"
        assert prompter.model.generate.call_count == 2

    run_async(_test())


def test_transformers_prompter_error_handling(mock_transformers):
    """Test that errors from the model are propagated."""

    async def _test():
        prompter = create_prompter()

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=MagicMock())
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        prompter.model.generate = Mock(side_effect=Exception("Model Error"))

        with pytest.raises(Exception, match="Model Error"):
            await prompter.prompt(("This will fail",))

    run_async(_test())


def test_protocol_compliance(mock_transformers):
    """Test that TransformersPrompter implements the Prompter protocol."""
    prompter = create_prompter()

    assert isinstance(prompter, Prompter)
    assert hasattr(prompter, "prompt")
    assert callable(prompter.prompt)


def test_transformers_prompter_attributes(mock_transformers):
    """Test that TransformersPrompter stores attributes correctly."""
    prompter = create_prompter(
        model="custom-model",
        return_format=SimpleResponse,
        system_message="You are helpful.",
        max_new_tokens=100,
        temperature=0.5,
    )

    assert prompter.model_name == "custom-model"
    assert prompter.return_format == SimpleResponse
    assert prompter.system_message == "You are helpful."
    assert prompter.generation_config["max_new_tokens"] == 100
    assert prompter.generation_config["temperature"] == 0.5


# ===== Image/Multimodal Tests =====


def test_transformers_prompter_with_image_numpy(mock_transformers_multimodal):
    """Test prompting with text and image (numpy array)."""

    async def _test():
        import torch

        from daft.dependencies import np

        # Create prompter that thinks it's multimodal
        with (
            patch("daft.ai.transformers.protocols.prompter.AutoModelForCausalLM") as mock_model_cls,
            patch("daft.ai.transformers.protocols.prompter.AutoProcessor") as mock_proc_cls,
            patch("daft.ai.transformers.protocols.prompter.AutoTokenizer"),
            patch("daft.ai.transformers.protocols.prompter.get_torch_device") as mock_device,
        ):
            mock_device.return_value = Mock(type="cpu")
            mock_model_cls.from_pretrained = Mock(return_value=create_mock_model())
            mock_proc_cls.from_pretrained = Mock(return_value=create_mock_processor())

            prompter = create_prompter()

        # Mock for actual prompting
        mock_output = torch.tensor([[1, 2, 3, 4, 5]])
        prompter.model.generate = Mock(return_value=mock_output)
        prompter.tokenizer.decode = Mock(return_value="This image shows a cat.")

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.processor.__call__ = Mock(return_value=mock_inputs)

        # Create a dummy numpy array image
        image = np.zeros((100, 100, 3), dtype=np.uint8)

        result = await prompter.prompt(("What is in this image?", image))

        assert result == "This image shows a cat."

    run_async(_test())


def test_transformers_prompter_raises_without_pillow_on_image(mock_transformers):
    """Test that prompting with image fails without Pillow."""

    async def _test():
        from daft.dependencies import np

        prompter = create_prompter()

        # Mock Pillow as not available
        with patch("daft.dependencies.pil_image.module_available", return_value=False):
            image = np.zeros((100, 100, 3), dtype=np.uint8)

            with pytest.raises(
                ImportError,
                match=r"Please `pip install 'daft\[transformers\]'` to use the prompt function with this provider.",
            ):
                await prompter.prompt(("Image", image))

    run_async(_test())


def test_transformers_prompter_text_only(mock_transformers):
    """Test that text-only prompts build correct messages."""
    prompter = create_prompter()

    # Test _build_chat_messages for text-only input
    processed = [{"type": "text", "text": "What is the capital of France?"}]
    messages, images = prompter._build_chat_messages(processed)

    assert len(messages) == 1
    assert messages[0]["role"] == "user"
    assert len(images) == 0


def test_transformers_prompter_with_image_from_bytes(mock_transformers):
    """Test prompting with image from bytes."""

    async def _test():
        import io

        import torch
        from PIL import Image as PILImage

        from daft.dependencies import np

        prompter = create_prompter()

        mock_output = torch.tensor([[1, 2, 3, 4, 5]])
        prompter.model.generate = Mock(return_value=mock_output)
        prompter.tokenizer.decode = Mock(return_value="This image contains test data.")

        mock_inputs = MagicMock()
        mock_inputs.__getitem__ = Mock(return_value=torch.tensor([[1, 2]]))
        mock_inputs.to = Mock(return_value=mock_inputs)
        prompter.tokenizer.__call__ = Mock(return_value=mock_inputs)

        # Create a simple image and convert to PNG bytes
        img_array = np.zeros((50, 50, 3), dtype=np.uint8)
        img_array[:, :, 0] = 255  # Red
        img = PILImage.fromarray(img_array)
        bio = io.BytesIO()
        img.save(bio, format="PNG")
        image_bytes = bio.getvalue()

        # The prompter should detect this as an image
        result = await prompter.prompt(("What is this?", image_bytes))

        assert result == "This image contains test data."

    run_async(_test())


def test_transformers_prompter_with_text_file(mock_transformers):
    """Test processing text file content."""
    import os
    import tempfile

    from daft.file import File

    prompter = create_prompter()

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write("This is a plain text document.")
        temp_path = tmp.name

    try:
        # Test _process_message for File object
        result = prompter._process_message(File(temp_path))

        assert result["type"] == "text"
        assert "<file_text_plain>" in result["text"]
        assert "This is a plain text document." in result["text"]
    finally:
        os.unlink(temp_path)


# ===== Quantization Tests =====


def test_transformers_prompter_quantization_config_option():
    """Test that quantization_config option is properly passed through descriptor."""
    from daft.ai.transformers.provider import TransformersProvider

    provider = TransformersProvider()
    descriptor = provider.get_prompter(
        model="test-model",
        quantization_config={"load_in_4bit": True},
    )

    opts = descriptor.get_options()
    assert "quantization_config" in opts
    assert opts["quantization_config"]["load_in_4bit"] is True


def test_transformers_prompter_quantization_config_in_options():
    """Test that quantization_config is supported in prompt options."""
    from daft.ai.transformers.protocols.prompter import TransformersPromptOptions

    # Verify quantization_config is a valid option
    opts: TransformersPromptOptions = {
        "quantization_config": {"load_in_8bit": True},
        "max_new_tokens": 100,
    }

    assert opts["quantization_config"]["load_in_8bit"] is True


# ===== Chat Message Building Tests =====


def test_transformers_prompter_build_chat_messages_text_only(mock_transformers):
    """Test _build_chat_messages with text-only input."""
    prompter = create_prompter()

    processed = [{"type": "text", "text": "Hello, world!"}]
    messages, images = prompter._build_chat_messages(processed)

    assert len(messages) == 1
    assert messages[0]["role"] == "user"
    assert len(images) == 0


def test_transformers_prompter_build_chat_messages_with_system(mock_transformers):
    """Test _build_chat_messages includes system message."""
    prompter = create_prompter(system_message="You are helpful.")

    processed = [{"type": "text", "text": "Hello!"}]
    messages, images = prompter._build_chat_messages(processed)

    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    assert messages[0]["content"] == "You are helpful."
    assert messages[1]["role"] == "user"


def test_transformers_prompter_build_chat_messages_with_image(mock_transformers):
    """Test _build_chat_messages with image input."""
    from PIL import Image as PILImage

    from daft.dependencies import np

    prompter = create_prompter()

    # Create a mock image
    img_array = np.zeros((50, 50, 3), dtype=np.uint8)
    img = PILImage.fromarray(img_array)

    processed = [
        {"type": "text", "text": "What is this?"},
        {"type": "image", "image": img},
    ]
    messages, images = prompter._build_chat_messages(processed)

    assert len(messages) == 1
    assert len(images) == 1
    assert images[0] == img


# ===== Message Processing Tests =====


def test_transformers_prompter_process_str_message(mock_transformers):
    """Test processing string message."""
    prompter = create_prompter()

    result = prompter._process_message("Hello, world!")

    assert result == {"type": "text", "text": "Hello, world!"}


def test_transformers_prompter_process_bytes_text(mock_transformers):
    """Test processing bytes as text."""
    prompter = create_prompter()

    result = prompter._process_message(b"Hello, world!")

    assert result["type"] == "text"
    assert result["text"] == "Hello, world!"


def test_transformers_prompter_process_bytes_image(mock_transformers):
    """Test processing bytes as image."""
    import io

    from PIL import Image as PILImage

    from daft.dependencies import np

    prompter = create_prompter()

    # Create PNG bytes
    img_array = np.zeros((10, 10, 3), dtype=np.uint8)
    img = PILImage.fromarray(img_array)
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    png_bytes = bio.getvalue()

    result = prompter._process_message(png_bytes)

    assert result["type"] == "image"
    assert "image" in result


def test_transformers_prompter_process_file(mock_transformers):
    """Test processing File object."""
    import os
    import tempfile

    from daft.file import File

    prompter = create_prompter()

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w") as tmp:
        tmp.write("Test content")
        temp_path = tmp.name

    try:
        result = prompter._process_message(File(temp_path))

        assert result["type"] == "text"
        assert "<file_text_plain>" in result["text"]
    finally:
        os.unlink(temp_path)


def test_transformers_prompter_process_numpy_array(mock_transformers):
    """Test processing numpy array as image."""
    from daft.dependencies import np

    prompter = create_prompter()

    image = np.zeros((50, 50, 3), dtype=np.uint8)
    result = prompter._process_message(image)

    assert result["type"] == "image"
    assert "image" in result


def test_transformers_prompter_process_unsupported_type(mock_transformers):
    """Test that unsupported types raise ValueError."""
    prompter = create_prompter()

    with pytest.raises(ValueError, match="Unsupported content type"):
        prompter._process_message(12345)  # int is not supported


# ===== VLM Model Loading Tests =====


def test_transformers_prompter_multimodal_flag_text_only(mock_transformers):
    """Test that text-only models set is_multimodal to False."""
    # Default fixture has AutoProcessor raise an exception, falling back to tokenizer
    prompter = create_prompter(model="test-model")

    # Verify multimodal flag is False for text-only models
    assert prompter.is_multimodal is False
    assert prompter.model is not None


def test_transformers_prompter_model_loading_all_fail():
    """Test that error is raised when all model loading attempts fail."""
    with (
        patch("daft.ai.transformers.protocols.prompter.AutoModelForCausalLM") as mock_causal,
        patch("daft.ai.transformers.protocols.prompter.AutoProcessor") as mock_proc,
        patch("daft.ai.transformers.protocols.prompter.AutoTokenizer") as mock_tok,
        patch("daft.ai.transformers.protocols.prompter.get_torch_device") as mock_device,
    ):
        mock_device.return_value = Mock(type="cpu")
        mock_tok.from_pretrained = Mock(return_value=create_mock_tokenizer())
        mock_proc.from_pretrained = Mock(side_effect=Exception("Not multimodal"))

        # All model classes fail
        mock_causal.from_pretrained = Mock(side_effect=ValueError("Failed"))

        with pytest.raises(ValueError, match="Could not load model"):
            create_prompter(model="bad-model")


# ===== Record Usage Metrics Tests =====


def test_transformers_prompter_record_usage_metrics(mock_transformers):
    """Test that metrics are recorded with correct attributes."""
    prompter = create_prompter()

    with patch("daft.ai.metrics.increment_counter") as mock_counter:
        prompter._record_usage_metrics(10, 20, 30)

    expected_attrs = {
        "model": DEFAULT_MODEL_NAME,
        "protocol": "prompt",
        "provider": "transformers",
    }

    calls = mock_counter.call_args_list
    # Should have input, output, total, and requests counters
    assert len(calls) == 4

    for call in calls:
        assert call[1]["attributes"] == expected_attrs


def test_transformers_prompter_record_usage_metrics_custom_provider(mock_transformers):
    """Test that metrics use custom provider name."""
    prompter = create_prompter(provider_name="custom-transformers")

    with patch("daft.ai.metrics.increment_counter") as mock_counter:
        prompter._record_usage_metrics(5, 10, 15)

    expected_attrs = {
        "model": DEFAULT_MODEL_NAME,
        "protocol": "prompt",
        "provider": "custom-transformers",
    }

    calls = mock_counter.call_args_list
    for call in calls:
        assert call[1]["attributes"] == expected_attrs
