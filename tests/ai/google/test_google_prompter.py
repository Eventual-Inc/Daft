from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Skip if google-genai is not installed
pytest.importorskip("google.genai")

from pydantic import BaseModel

from daft.ai.google.protocols.prompter import GooglePrompter, GooglePrompterDescriptor
from daft.ai.google.provider import GoogleProvider
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


DEFAULT_MODEL_NAME = "gemini-2.5-flash"
DEFAULT_PROVIDER_OPTIONS = {"api_key": "test-key"}


def create_prompter(
    *,
    provider_name: str = "google",
    provider_options: dict[str, Any] | None = None,
    model: str = DEFAULT_MODEL_NAME,
    **kwargs: Any,
) -> GooglePrompter:
    """Helper to instantiate GooglePrompter with sensible defaults."""
    opts = dict(provider_options) if provider_options is not None else dict(DEFAULT_PROVIDER_OPTIONS)
    # Unpack generation_config if it's passed as a dict
    prompt_options = dict(kwargs)

    # Extract return_format and system_message to pass as explicit parameters
    return_format = prompt_options.pop("return_format", None)
    system_message = prompt_options.pop("system_message", None)

    if "generation_config" in prompt_options and isinstance(prompt_options["generation_config"], dict):
        generation_config = prompt_options.pop("generation_config")
        prompt_options.update(generation_config)

    return GooglePrompter(
        provider_name=provider_name,
        provider_options=opts,
        model=model,
        return_format=return_format,
        system_message=system_message,
        prompt_options=prompt_options,
    )


def test_google_provider_get_prompter_default():
    """Test that the provider returns a prompter descriptor with default settings."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_prompter()

    assert isinstance(descriptor, GooglePrompterDescriptor)
    assert descriptor.get_provider() == "google"
    assert descriptor.get_model() == "gemini-2.5-flash"
    assert descriptor.get_options() == {}
    assert descriptor.return_format is None


def test_google_provider_get_prompter_with_model():
    """Test that the provider accepts custom model names."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_prompter(model="gemini-2.0-flash")

    assert isinstance(descriptor, GooglePrompterDescriptor)
    assert descriptor.get_model() == "gemini-2.0-flash"


def test_google_provider_get_prompter_with_return_format():
    """Test that the provider accepts return_format parameter."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_prompter(model="gemini-2.5-flash", return_format=SimpleResponse)

    assert isinstance(descriptor, GooglePrompterDescriptor)
    assert descriptor.return_format == SimpleResponse


def test_google_provider_get_prompter_with_options():
    """Test that the provider accepts generation config options."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_prompter(
        model="gemini-2.5-flash",
        temperature=0.7,
        max_output_tokens=100,
    )

    assert descriptor.get_options() == {"temperature": 0.7, "max_output_tokens": 100}


# ===== Descriptor Tests =====


def test_google_prompter_descriptor_instantiation():
    """Test that descriptor can be instantiated directly."""
    descriptor = GooglePrompterDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-2.5-flash",
        prompt_options={},
    )

    assert descriptor.get_provider() == "google"
    assert descriptor.get_model() == "gemini-2.5-flash"
    assert descriptor.get_options() == {}
    assert descriptor.return_format is None


def test_google_prompter_descriptor_with_return_format():
    """Test descriptor with return_format."""
    descriptor = GooglePrompterDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-2.5-flash",
        return_format=SimpleResponse,
    )

    assert descriptor.return_format == SimpleResponse


def test_google_prompter_descriptor_get_udf_options():
    """Test that descriptor returns appropriate UDF options."""
    descriptor = GooglePrompterDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-2.5-flash",
        prompt_options={},
    )

    udf_options = descriptor.get_udf_options()
    assert udf_options.concurrency is None
    assert udf_options.num_gpus in (0, None)


def test_google_prompter_instantiate():
    """Test that descriptor can instantiate a GooglePrompter."""
    descriptor = GooglePrompterDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-2.5-flash",
        prompt_options={},
    )

    with patch("daft.ai.google.protocols.prompter.genai.Client"):
        prompter = descriptor.instantiate()
        assert isinstance(prompter, GooglePrompter)
        assert isinstance(prompter, Prompter)
        assert prompter.model == "gemini-2.5-flash"
        assert prompter.return_format is None
        assert prompter.provider_name == "google"


def test_google_prompter_descriptor_custom_provider_name():
    """Test that descriptor forwards custom provider name."""
    descriptor = GooglePrompterDescriptor(
        provider_name="vertex-ai",
        provider_options={"api_key": "test-key"},
        model_name="gemini-2.5-flash",
        prompt_options={},
    )

    with patch("daft.ai.google.protocols.prompter.genai.Client"):
        prompter = descriptor.instantiate()
        assert prompter.provider_name == "vertex-ai"


def test_google_prompter_plain_text_response():
    """Test prompting with plain text response."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "This is a test response."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            result = await prompter.prompt(("Hello, world!",))

            assert result == "This is a test response."

            # Verify call arguments
            call_args = mock_client_instance.aio.models.generate_content.call_args
            assert call_args.kwargs["model"] == "gemini-2.5-flash"
            contents = call_args.kwargs["contents"]
            assert len(contents) == 1
            assert contents[0].role == "user"
            assert len(contents[0].parts) == 1
            assert contents[0].parts[0].text == "Hello, world!"

    run_async(_test())


def test_google_prompter_records_usage_metrics():
    """Ensure token/request counters fire."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "Metrics test response."

            # Mock usage metadata
            usage = Mock()
            usage.prompt_token_count = 10
            usage.candidates_token_count = 20
            usage.total_token_count = 30
            mock_response.usage_metadata = usage

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            with patch("daft.ai.metrics.increment_counter") as mock_counter:
                result = await prompter.prompt(("Record metrics",))

            assert result == "Metrics test response."

            expected_attrs = {
                "model": "gemini-2.5-flash",
                "protocol": "prompt",
                "provider": "google",
            }

            # Check counters
            calls = mock_counter.call_args_list
            assert len(calls) == 4  # input, output, total, requests

            # Check for specific calls
            has_input = False
            has_output = False
            has_total = False

            for c in calls:
                name = c[0][0]
                val = c[0][1] if len(c[0]) > 1 else None
                attrs = c[1]["attributes"]

                assert attrs == expected_attrs

                if name == "input tokens":
                    assert val == 10
                    has_input = True
                elif name == "output tokens":
                    assert val == 20
                    has_output = True
                elif name == "total tokens":
                    assert val == 30
                    has_total = True

            assert has_input and has_output and has_total

    run_async(_test())


def test_google_prompter_structured_output():
    """Test prompting with structured output (Pydantic model)."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            expected_output = SimpleResponse(answer="Yes", confidence=0.95)
            mock_response.parsed = expected_output
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter(return_format=SimpleResponse)

            result = await prompter.prompt(("Is this a test?",))

            assert isinstance(result, SimpleResponse)
            assert result.answer == "Yes"
            assert result.confidence == 0.95

            # Verify config has response_schema
            call_args = mock_client_instance.aio.models.generate_content.call_args
            config = call_args.kwargs["config"]
            assert config.response_mime_type == "application/json"
            # response_schema is converted to JSON schema dict
            assert isinstance(config.response_schema, dict)
            assert config.response_schema["title"] == "SimpleResponse"

    run_async(_test())


def test_google_prompter_with_generation_config():
    """Test prompting with custom generation configuration."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "Response with custom config."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter(
                generation_config={
                    "temperature": 0.9,
                    "max_output_tokens": 500,
                    "top_p": 0.95,
                }
            )

            result = await prompter.prompt(("Test",))

            assert result == "Response with custom config."

            # Verify config was passed
            call_args = mock_client_instance.aio.models.generate_content.call_args
            config = call_args.kwargs["config"]
            assert config.temperature == 0.9
            assert config.max_output_tokens == 500
            assert config.top_p == 0.95

    run_async(_test())


def test_google_prompter_complex_structured_output():
    """Test prompting with complex structured output."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            expected_output = ComplexResponse(
                summary="Test summary",
                key_points=["Point 1", "Point 2", "Point 3"],
                sentiment="positive",
            )
            mock_response.parsed = expected_output
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter(return_format=ComplexResponse)

            result = await prompter.prompt(("Analyze this",))

            assert isinstance(result, ComplexResponse)
            assert result.summary == "Test summary"
            assert result.key_points == ["Point 1", "Point 2", "Point 3"]
            assert result.sentiment == "positive"

    run_async(_test())


def test_google_prompter_client_params_separation():
    """Test that client params are properly separated from generation params."""
    with patch("daft.ai.google.protocols.prompter.genai.Client") as mock_genai_class:
        mock_client = Mock()
        mock_genai_class.return_value = mock_client

        prompter = create_prompter(
            provider_options={"api_key": "test-key", "project": "test-project"},
            generation_config={
                "temperature": 0.7,
                "max_output_tokens": 100,
                "http_options": {"timeout": 30.0},  # This should go to client
            },
        )

        # Verify client was created with correct params
        mock_genai_class.assert_called_once()
        client_call_kwargs = mock_genai_class.call_args.kwargs
        assert client_call_kwargs["api_key"] == "test-key"
        assert client_call_kwargs["project"] == "test-project"
        assert client_call_kwargs["http_options"] == {"timeout": 30.0}

        # Verify generation config doesn't include client params
        assert "http_options" not in prompter.generation_config
        assert "api_key" not in prompter.generation_config
        assert "project" not in prompter.generation_config
        # Verify generation params are correctly stored
        assert prompter.generation_config.temperature == 0.7
        assert prompter.generation_config.max_output_tokens == 100


def test_google_prompter_error_handling():
    """Test that errors from the API are propagated."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_client_instance.aio.models.generate_content = AsyncMock(side_effect=Exception("API Error"))

            prompter = create_prompter()

            with pytest.raises(Exception, match="API Error"):
                await prompter.prompt(("This will fail",))

    run_async(_test())


def test_protocol_compliance():
    """Test that GooglePrompter implements the Prompter protocol."""
    prompter = create_prompter()

    assert isinstance(prompter, Prompter)
    assert hasattr(prompter, "prompt")
    assert callable(prompter.prompt)


def test_google_prompter_attributes():
    """Test that GooglePrompter stores attributes correctly."""
    prompter = create_prompter(
        model="gemini-2.0-flash",
        return_format=SimpleResponse,
        system_message="You are helpful.",
    )

    assert prompter.model == "gemini-2.0-flash"
    assert prompter.return_format == SimpleResponse
    assert prompter.system_message == "You are helpful."
    assert prompter.provider_name == "google"


def test_google_prompter_with_system_message():
    """Test prompting with system message."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "Response with system context."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter(system_message="You are a helpful assistant.")

            result = await prompter.prompt(("Hello!",))

            assert result == "Response with system context."

            # Verify system message in config
            call_args = mock_client_instance.aio.models.generate_content.call_args
            config = call_args.kwargs["config"]
            assert config.system_instruction == "You are a helpful assistant."

    run_async(_test())


def test_google_prompter_with_image_numpy():
    """Test prompting with text and image (numpy array)."""

    async def _test():
        from daft.dependencies import np

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "This image shows a cat."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            # Create a dummy numpy array image
            image = np.zeros((100, 100, 3), dtype=np.uint8)

            result = await prompter.prompt(("What is in this image?", image))

            assert result == "This image shows a cat."

            # Verify that the call was made with both text and image parts
            call_args = mock_client_instance.aio.models.generate_content.call_args
            contents = call_args.kwargs["contents"]
            assert len(contents) == 1
            parts = contents[0].parts
            assert len(parts) == 2

            # First part text
            assert parts[0].text == "What is in this image?"

            # Second part image bytes (not text)
            assert parts[1].text is None


def test_google_prompter_raises_without_pillow_on_image():
    """Test that prompting with image fails without Pillow."""
    from daft.dependencies import np

    async def _test():
        # Mock Pillow as not available
        with patch("daft.dependencies.pil_image.module_available", return_value=False):
            prompter = create_prompter()
            image = np.zeros((100, 100, 3), dtype=np.uint8)

            with pytest.raises(
                ImportError,
                match=r"Please `pip install 'daft\[google\]'` to use the prompt function with this provider.",
            ):
                # We use run_async here because prompt is async
                await prompter.prompt(("Image", image))

    run_async(_test())


def test_google_prompter_with_image_from_bytes():
    """Test prompting with image input from raw bytes."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "This is a PNG image."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            # Create fake PNG bytes (simplified header)
            png_bytes = b"\x89PNG\r\n\x1a\n"

            result = await prompter.prompt(("What is this?", png_bytes))

            assert result == "This is a PNG image."

            # Verify bytes were processed
            call_args = mock_client_instance.aio.models.generate_content.call_args
            contents = call_args.kwargs["contents"]
            parts = contents[0].parts
            assert len(parts) == 2
            # Second part should not be text
            assert parts[1].text is None

    run_async(_test())


def test_google_prompter_with_text_file():
    """Test prompting with a plain text document."""

    async def _test():
        import os
        import tempfile

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "This appears to be a text document."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
                tmp.write("This is a plain text document content.")
                temp_path = tmp.name

            try:
                from daft.file import File

                result = await prompter.prompt(("Summarize this document.", File(temp_path)))

                assert result == "This appears to be a text document."

                # Verify that the file content was wrapped in XML tags
                call_args = mock_client_instance.aio.models.generate_content.call_args
                contents = call_args.kwargs["contents"]
                assert len(contents) == 1
                parts = contents[0].parts
                assert len(parts) == 2

                # First part: text question
                assert parts[0].text == "Summarize this document."

                # Second part: file content wrapped in XML tags
                assert parts[1].text == "<file_text_plain>This is a plain text document content.</file_text_plain>"

            finally:
                os.unlink(temp_path)

    run_async(_test())


def test_google_prompter_multiple_messages():
    """Test prompting with multiple sequential messages."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value

            # First call
            mock_response1 = Mock()
            mock_response1.text = "First response"
            mock_response1.usage_metadata = None

            # Second call
            mock_response2 = Mock()
            mock_response2.text = "Second response"
            mock_response2.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(side_effect=[mock_response1, mock_response2])

            prompter = create_prompter()

            result1 = await prompter.prompt(("First message",))
            result2 = await prompter.prompt(("Second message",))

            assert result1 == "First response"
            assert result2 == "Second response"
            assert mock_client_instance.aio.models.generate_content.call_count == 2

    run_async(_test())


def test_google_prompter_with_image_structured_output():
    """Test prompting with image and structured output."""

    async def _test():
        from daft.dependencies import np

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            expected_output = ComplexResponse(
                summary="Image of a cat",
                key_points=["fluffy", "orange", "sleeping"],
                sentiment="positive",
            )
            mock_response.parsed = expected_output
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter(return_format=ComplexResponse)

            # Create a dummy numpy array image
            image = np.zeros((100, 100, 3), dtype=np.uint8)

            result = await prompter.prompt(("Describe this image", image))

            assert isinstance(result, ComplexResponse)
            assert result.summary == "Image of a cat"
            assert len(result.key_points) == 3
            assert result.sentiment == "positive"

            # Verify the call was made with image
            call_args = mock_client_instance.aio.models.generate_content.call_args
            contents = call_args.kwargs["contents"]
            assert len(contents) == 1
            parts = contents[0].parts
            assert len(parts) == 2
            assert parts[0].text == "Describe this image"
            assert parts[1].text is None  # Image part

    run_async(_test())


def test_google_prompter_text_only():
    """Test that text-only prompts still work (no image)."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "Paris is the capital."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            result = await prompter.prompt(("What is the capital of France?",))

            assert result == "Paris is the capital."

            # Verify that content has single text part
            call_args = mock_client_instance.aio.models.generate_content.call_args
            contents = call_args.kwargs["contents"]
            assert len(contents) == 1
            assert contents[0].role == "user"
            parts = contents[0].parts
            assert len(parts) == 1
            assert parts[0].text == "What is the capital of France?"

    run_async(_test())


def test_google_prompter_with_system_message_and_image():
    """Test prompting with system message and image."""

    async def _test():
        from daft.dependencies import np

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "I see a cat in the image."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter(system_message="You are an image recognition expert.")

            image = np.zeros((50, 50, 3), dtype=np.uint8)
            result = await prompter.prompt(("What do you see?", image))

            assert result == "I see a cat in the image."

            # Verify both system message and image were included
            call_args = mock_client_instance.aio.models.generate_content.call_args
            config = call_args.kwargs["config"]
            assert config.system_instruction == "You are an image recognition expert."

            contents = call_args.kwargs["contents"]
            assert len(contents) == 1
            assert contents[0].role == "user"
            parts = contents[0].parts
            assert len(parts) == 2
            assert parts[0].text == "What do you see?"
            assert parts[1].text is None  # Image part

    run_async(_test())


def test_google_prompter_with_image_from_file_path():
    """Test prompting with image from file path string."""

    async def _test():
        import tempfile

        from PIL import Image as PILImage

        from daft.dependencies import np

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "I see a file-based image."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            # Create a temporary image file
            img_array = np.zeros((50, 50, 3), dtype=np.uint8)
            img_array[:, :, 2] = 255  # Blue
            img = PILImage.fromarray(img_array)

            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                img.save(tmp.name)
                temp_path = tmp.name

            try:
                from daft.file import File

                result = await prompter.prompt(("Describe this", File(temp_path)))

                assert result == "I see a file-based image."

                # Verify that the image was processed
                call_args = mock_client_instance.aio.models.generate_content.call_args
                contents = call_args.kwargs["contents"]
                parts = contents[0].parts
                assert len(parts) == 2
                assert parts[1].text is None  # Image part, not text
            finally:
                import os

                os.unlink(temp_path)

    run_async(_test())


def test_google_prompter_with_image_from_file_object():
    """Test prompting with image from daft.File object."""

    async def _test():
        import tempfile

        from PIL import Image as PILImage

        from daft.dependencies import np
        from daft.file import File

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "File object image processed."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            # Create a temporary image file
            img_array = np.zeros((50, 50, 3), dtype=np.uint8)
            img_array[:, :, 1] = 255  # Green
            img = PILImage.fromarray(img_array)

            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                img.save(tmp.name)
                temp_path = tmp.name

            try:
                # Create a File object
                file_obj = File(temp_path)
                result = await prompter.prompt(("What color?", file_obj))

                assert result == "File object image processed."

                # Verify that the image was processed
                call_args = mock_client_instance.aio.models.generate_content.call_args
                contents = call_args.kwargs["contents"]
                parts = contents[0].parts
                assert len(parts) == 2
                assert parts[1].text is None  # Image part
            finally:
                import os

                os.unlink(temp_path)

    run_async(_test())


def test_google_prompter_with_file():
    """Test prompting with a non-image file (e.g., audio/video)."""

    async def _test():
        import tempfile

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "This appears to be an audio file."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            # Create a temporary file with .mp3 extension (mock audio)
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False, mode="wb") as tmp:
                tmp.write(b"mock audio data")
                temp_path = tmp.name

            try:
                from daft.file import File

                result = await prompter.prompt(("What type of file is this?", File(temp_path)))

                assert result == "This appears to be an audio file."

                # Verify that the file was processed as bytes (not text)
                call_args = mock_client_instance.aio.models.generate_content.call_args
                contents = call_args.kwargs["contents"]
                parts = contents[0].parts
                assert len(parts) == 2
                assert parts[1].text is None  # Binary file part
            finally:
                import os

                os.unlink(temp_path)

    run_async(_test())


def test_google_prompter_with_mixed_modalities():
    """Test prompting with mixed modalities (text, image, and document)."""

    async def _test():
        import tempfile

        from daft.dependencies import np

        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "Mixed modality response."
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.generate_content = AsyncMock(return_value=mock_response)

            prompter = create_prompter()

            # Create image
            image = np.zeros((50, 50, 3), dtype=np.uint8)

            # Create a temporary PDF
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False, mode="wb") as tmp:
                tmp.write(b"%PDF-1.4 test")
                temp_path = tmp.name

            try:
                from daft.file import File

                result = await prompter.prompt(("Compare these", image, File(temp_path)))

                assert result == "Mixed modality response."

                # Verify that both image and document were included
                call_args = mock_client_instance.aio.models.generate_content.call_args
                contents = call_args.kwargs["contents"]
                parts = contents[0].parts
                assert len(parts) == 3  # text + image + document
                assert parts[0].text == "Compare these"
                assert parts[1].text is None  # Image
                assert parts[2].text is None  # PDF
            finally:
                import os

                os.unlink(temp_path)

    run_async(_test())


def test_google_prompter_record_usage_metrics_custom_provider():
    """Test that metrics use the custom provider name."""
    prompter = create_prompter(provider_name="vertex-ai")

    with patch("daft.ai.metrics.increment_counter") as mock_counter:
        prompter._record_usage_metrics(1, 2, 3)

    expected_attrs = {
        "model": "gemini-2.5-flash",
        "protocol": "prompt",
        "provider": "vertex-ai",
    }

    calls = mock_counter.call_args_list
    assert len(calls) == 4

    for c in calls:
        attrs = c[1]["attributes"]
        assert attrs == expected_attrs
