from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

pytest.importorskip("openai")

from pydantic import BaseModel

from daft.ai.openai.protocols.prompter import OpenAIPrompter, OpenAIPrompterDescriptor
from daft.ai.openai.provider import OpenAIProvider
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


def test_openai_provider_get_prompter_default():
    """Test that the provider returns a prompter descriptor with default settings."""
    provider = OpenAIProvider(api_key="test-key")
    descriptor = provider.get_prompter()

    assert isinstance(descriptor, OpenAIPrompterDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "gpt-4o-mini"
    assert descriptor.get_options() == {}
    assert descriptor.return_format is None


def test_openai_provider_get_prompter_with_model():
    """Test that the provider accepts custom model names."""
    provider = OpenAIProvider(api_key="test-key")
    descriptor = provider.get_prompter(model="gpt-4o")

    assert isinstance(descriptor, OpenAIPrompterDescriptor)
    assert descriptor.get_model() == "gpt-4o"


def test_openai_provider_get_prompter_with_return_format():
    """Test that the provider accepts return_format parameter."""
    provider = OpenAIProvider(api_key="test-key")
    descriptor = provider.get_prompter(model="gpt-4o-mini", return_format=SimpleResponse)

    assert isinstance(descriptor, OpenAIPrompterDescriptor)
    assert descriptor.return_format == SimpleResponse


def test_openai_provider_get_prompter_with_options():
    """Test that the provider accepts generation config options."""
    provider = OpenAIProvider(api_key="test-key")
    descriptor = provider.get_prompter(
        model="gpt-4o-mini",
        temperature=0.7,
        max_tokens=100,
    )

    assert descriptor.get_options() == {"temperature": 0.7, "max_tokens": 100}


def test_openai_prompter_descriptor_instantiation():
    """Test that descriptor can be instantiated directly."""
    descriptor = OpenAIPrompterDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="gpt-4o-mini",
        model_options={},
    )

    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "gpt-4o-mini"
    assert descriptor.get_options() == {}
    assert descriptor.return_format is None


def test_openai_prompter_descriptor_with_return_format():
    """Test descriptor with return_format."""
    descriptor = OpenAIPrompterDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="gpt-4o-mini",
        model_options={},
        return_format=SimpleResponse,
    )

    assert descriptor.return_format == SimpleResponse


def test_openai_prompter_descriptor_get_udf_options():
    """Test that descriptor returns appropriate UDF options."""
    descriptor = OpenAIPrompterDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="gpt-4o-mini",
        model_options={},
    )

    udf_options = descriptor.get_udf_options()
    assert udf_options.concurrency is None
    # num_gpus is None for HTTP-based models
    assert udf_options.num_gpus in (0, None)


def test_openai_prompter_instantiate():
    """Test that descriptor can instantiate an OpenAIPrompter."""
    descriptor = OpenAIPrompterDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="gpt-4o-mini",
        model_options={},
    )

    prompter = descriptor.instantiate()
    assert isinstance(prompter, OpenAIPrompter)
    assert isinstance(prompter, Prompter)
    assert prompter.model == "gpt-4o-mini"
    assert prompter.return_format is None


def test_openai_prompter_plain_text_response():
    """Test prompting with plain text response."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "This is a test response."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Hello, world!",))

        assert result == "This is a test response."
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": [{"type": "input_text", "text": "Hello, world!"}]}],
        )

    run_async(_test())


def test_openai_prompter_structured_output():
    """Test prompting with structured output (Pydantic model)."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        expected_output = SimpleResponse(answer="Yes", confidence=0.95)
        mock_response.output_parsed = expected_output
        mock_client.responses.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            return_format=SimpleResponse,
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Is this a test?",))

        assert isinstance(result, SimpleResponse)
        assert result.answer == "Yes"
        assert result.confidence == 0.95
        mock_client.responses.parse.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": [{"type": "input_text", "text": "Is this a test?"}]}],
            text_format=SimpleResponse,
        )

    run_async(_test())


def test_openai_prompter_with_generation_config():
    """Test prompting with generation configuration parameters."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "Response with custom config."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            generation_config={"temperature": 0.8, "max_tokens": 50},
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Tell me a story",))

        assert result == "Response with custom config."
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": [{"type": "input_text", "text": "Tell me a story"}]}],
            temperature=0.8,
            max_tokens=50,
        )

    run_async(_test())


def test_openai_prompter_complex_structured_output():
    """Test prompting with a more complex Pydantic model."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        expected_output = ComplexResponse(
            summary="Test summary",
            key_points=["Point 1", "Point 2", "Point 3"],
            sentiment="positive",
        )
        mock_response.output_parsed = expected_output
        mock_client.responses.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            return_format=ComplexResponse,
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Summarize this text",))

        assert isinstance(result, ComplexResponse)
        assert result.summary == "Test summary"
        assert len(result.key_points) == 3
        assert result.sentiment == "positive"

    run_async(_test())


def test_openai_prompter_multiple_messages():
    """Test prompting with multiple sequential messages."""

    async def _test():
        mock_client = AsyncMock()

        # First call
        mock_response1 = Mock()
        mock_response1.output_text = "First response"

        # Second call
        mock_response2 = Mock()
        mock_response2.output_text = "Second response"

        mock_client.responses.create = AsyncMock(side_effect=[mock_response1, mock_response2])

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

        result1 = await prompter.prompt(("First message",))
        result2 = await prompter.prompt(("Second message",))

        assert result1 == "First response"
        assert result2 == "Second response"
        assert mock_client.responses.create.call_count == 2

    run_async(_test())


def test_openai_prompter_client_params_separation():
    """Test that client params are properly separated from generation params."""
    with patch("daft.ai.openai.protocols.prompter.AsyncOpenAI") as mock_openai_class:
        mock_client = AsyncMock()
        mock_openai_class.return_value = mock_client

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key", "base_url": "https://api.example.com"},
            model="gpt-4o-mini",
            generation_config={
                "temperature": 0.7,
                "max_tokens": 100,
                "timeout": 30.0,  # This should go to client
            },
        )

        # Verify client was created with correct params
        mock_openai_class.assert_called_once()
        client_call_kwargs = mock_openai_class.call_args.kwargs
        assert client_call_kwargs["api_key"] == "test-key"
        assert client_call_kwargs["base_url"] == "https://api.example.com"
        assert client_call_kwargs["timeout"] == 30.0

        # Verify generation config doesn't include client params
        assert "timeout" not in prompter.generation_config
        assert "api_key" not in prompter.generation_config
        assert "base_url" not in prompter.generation_config
        assert prompter.generation_config["temperature"] == 0.7
        assert prompter.generation_config["max_tokens"] == 100


def test_openai_prompter_error_handling():
    """Test that errors from the API are propagated."""

    async def _test():
        mock_client = AsyncMock()
        mock_client.responses.create = AsyncMock(side_effect=Exception("API Error"))

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

        with pytest.raises(Exception, match="API Error"):
            await prompter.prompt(("This will fail",))

    run_async(_test())


def test_protocol_compliance():
    """Test that OpenAIPrompter implements the Prompter protocol."""
    prompter = OpenAIPrompter(
        provider_options={"api_key": "test-key"},
        model="gpt-4o-mini",
    )

    assert isinstance(prompter, Prompter)
    assert hasattr(prompter, "prompt")
    assert callable(prompter.prompt)


def test_openai_prompter_attributes():
    """Test that OpenAIPrompter stores attributes correctly."""
    prompter = OpenAIPrompter(
        provider_options={"api_key": "test-key"},
        model="gpt-4o",
        return_format=SimpleResponse,
        generation_config={"temperature": 0.5},
    )

    assert prompter.model == "gpt-4o"
    assert prompter.return_format == SimpleResponse
    assert prompter.generation_config == {"temperature": 0.5}
    assert hasattr(prompter, "llm")


def test_openai_prompter_with_image_numpy():
    """Test prompting with text and image (numpy array)."""

    async def _test():
        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "This image shows a cat."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

        # Create a dummy numpy array image
        image = np.zeros((100, 100, 3), dtype=np.uint8)

        result = await prompter.prompt(("What is in this image?", image))

        assert result == "This image shows a cat."

        # Verify that the call was made with both text and image
        call_args = mock_client.responses.create.call_args
        assert call_args.kwargs["model"] == "gpt-4o-mini"
        messages = call_args.kwargs["input"]
        assert len(messages) == 1
        assert messages[0]["role"] == "user"
        assert isinstance(messages[0]["content"], list)
        assert messages[0]["content"][0]["type"] == "input_text"
        assert messages[0]["content"][0]["text"] == "What is in this image?"
        assert messages[0]["content"][1]["type"] == "input_image"
        assert "image_url" in messages[0]["content"][1]
        assert messages[0]["content"][1]["image_url"].startswith("data:image/png;base64,")

    run_async(_test())


def test_openai_prompter_with_image_structured_output():
    """Test prompting with image and structured output."""

    async def _test():
        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        expected_output = ComplexResponse(
            summary="Image of a cat",
            key_points=["fluffy", "orange", "sleeping"],
            sentiment="positive",
        )
        mock_response.output_parsed = expected_output
        mock_client.responses.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            return_format=ComplexResponse,
        )
        prompter.llm = mock_client

        # Create a dummy numpy array image
        image = np.zeros((100, 100, 3), dtype=np.uint8)

        result = await prompter.prompt(("Describe this image", image))

        assert isinstance(result, ComplexResponse)
        assert result.summary == "Image of a cat"
        assert len(result.key_points) == 3
        assert result.sentiment == "positive"

        # Verify the call was made with image
        call_args = mock_client.responses.parse.call_args
        messages = call_args.kwargs["input"]
        assert len(messages) == 1
        assert isinstance(messages[0]["content"], list)
        assert messages[0]["content"][0]["type"] == "input_text"
        assert messages[0]["content"][1]["type"] == "input_image"

    run_async(_test())


def test_openai_prompter_text_only():
    """Test that text-only prompts still work (no image)."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "Paris is the capital."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("What is the capital of France?",))

        assert result == "Paris is the capital."

        # Verify that content is a list with input_text
        call_args = mock_client.responses.create.call_args
        messages = call_args.kwargs["input"]
        assert len(messages) == 1
        assert messages[0]["role"] == "user"
        assert isinstance(messages[0]["content"], list)
        assert messages[0]["content"][0]["type"] == "input_text"
        assert messages[0]["content"][0]["text"] == "What is the capital of France?"

    run_async(_test())


def test_openai_prompter_with_system_message():
    """Test prompting with system message."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "Response with system context."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            system_message="You are a helpful assistant.",
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Hello!",))

        assert result == "Response with system context."

        # Verify system message was included
        call_args = mock_client.responses.create.call_args
        messages = call_args.kwargs["input"]
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == "You are a helpful assistant."
        assert messages[1]["role"] == "user"

    run_async(_test())


def test_openai_prompter_with_system_message_and_image():
    """Test prompting with system message and image."""

    async def _test():
        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "I see a cat in the image."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            system_message="You are an image recognition expert.",
        )
        prompter.llm = mock_client

        image = np.zeros((50, 50, 3), dtype=np.uint8)
        result = await prompter.prompt(("What do you see?", image))

        assert result == "I see a cat in the image."

        # Verify both system message and image were included
        call_args = mock_client.responses.create.call_args
        messages = call_args.kwargs["input"]
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == "You are an image recognition expert."
        assert messages[1]["role"] == "user"
        assert isinstance(messages[1]["content"], list)
        assert messages[1]["content"][0]["type"] == "input_text"
        assert messages[1]["content"][1]["type"] == "input_image"

    run_async(_test())


def test_openai_prompter_with_image_from_bytes():
    """Test prompting with image from bytes."""

    async def _test():
        import io

        from PIL import Image as PILImage

        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "This image contains test data."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

        # Create a simple image and convert to PNG bytes
        img_array = np.zeros((50, 50, 3), dtype=np.uint8)
        img_array[:, :, 0] = 255  # Red
        img = PILImage.fromarray(img_array)
        bio = io.BytesIO()
        img.save(bio, format="PNG")
        image_bytes = bio.getvalue()

        result = await prompter.prompt(("What is this?", image_bytes))

        assert result == "This image contains test data."

        # Verify that the image was encoded
        call_args = mock_client.responses.create.call_args
        messages = call_args.kwargs["input"]
        assert len(messages) == 1
        assert isinstance(messages[0]["content"], list)
        assert messages[0]["content"][1]["type"] == "input_image"
        # Should start with data URI for PNG
        assert messages[0]["content"][1]["image_url"].startswith("data:image/")

    run_async(_test())


def test_openai_prompter_with_image_from_file_path():
    """Test prompting with image from file path string."""

    async def _test():
        import tempfile

        from PIL import Image as PILImage

        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "I see a file-based image."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

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

            # Verify that the image was encoded
            call_args = mock_client.responses.create.call_args
            messages = call_args.kwargs["input"]
            assert len(messages) == 1
            assert isinstance(messages[0]["content"], list)
            assert messages[0]["content"][1]["type"] == "input_image"
            assert messages[0]["content"][1]["image_url"].startswith("data:image/")
        finally:
            import os

            os.unlink(temp_path)

    run_async(_test())


def test_openai_prompter_with_image_from_file_object():
    """Test prompting with image from daft.File object."""

    async def _test():
        import tempfile

        from PIL import Image as PILImage

        from daft.dependencies import np
        from daft.file import File

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "File object image processed."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

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

            # Verify that the image was encoded
            call_args = mock_client.responses.create.call_args
            messages = call_args.kwargs["input"]
            assert len(messages) == 1
            assert isinstance(messages[0]["content"], list)
            assert messages[0]["content"][1]["type"] == "input_image"
            assert messages[0]["content"][1]["image_url"].startswith("data:image/")
        finally:
            import os

            os.unlink(temp_path)

    run_async(_test())


def test_openai_prompter_with_file():
    """Test prompting with a file (e.g., audio/video)."""

    async def _test():
        import tempfile

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "This appears to be an audio file."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

        # Create a temporary file with .mp3 extension (mock audio)
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False, mode="wb") as tmp:
            tmp.write(b"mock audio data")
            temp_path = tmp.name

        try:
            from daft.file import File

            result = await prompter.prompt(("What type of file is this?", File(temp_path)))

            assert result == "This appears to be an audio file."

            # Verify that the file was encoded
            call_args = mock_client.responses.create.call_args
            messages = call_args.kwargs["input"]
            assert len(messages) == 1
            assert isinstance(messages[0]["content"], list)
            assert messages[0]["content"][1]["type"] == "input_file"
            assert "file_data" in messages[0]["content"][1]
            assert messages[0]["content"][1]["file_data"].startswith("data:")
        finally:
            import os

            os.unlink(temp_path)

    run_async(_test())


def test_openai_prompter_with_mixed_modalities():
    """Test prompting with mixed modalities (text, image, and document)."""

    async def _test():
        import tempfile

        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "Mixed modality response."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
        )
        prompter.llm = mock_client

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
            call_args = mock_client.responses.create.call_args
            messages = call_args.kwargs["input"]
            assert len(messages) == 1
            assert isinstance(messages[0]["content"], list)
            assert len(messages[0]["content"]) == 3  # text + image + document
            assert messages[0]["content"][0]["type"] == "input_text"
            assert messages[0]["content"][1]["type"] == "input_image"
            assert messages[0]["content"][2]["type"] == "input_file"
        finally:
            import os

            os.unlink(temp_path)

    run_async(_test())


# Tests with use_chat_completions=True


def test_openai_prompter_chat_completions_plain_text():
    """Test prompting with plain text using Chat Completions API."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "This is a test response."
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            use_chat_completions=True,
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Hello, world!",))

        assert result == "This is a test response."
        mock_client.chat.completions.create.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": [{"type": "text", "text": "Hello, world!"}]}],
        )

    run_async(_test())


def test_openai_prompter_chat_completions_structured_output():
    """Test prompting with structured output using Chat Completions API."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        expected_output = SimpleResponse(answer="Yes", confidence=0.95)
        mock_message.parsed = expected_output
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            return_format=SimpleResponse,
            use_chat_completions=True,
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Is this a test?",))

        assert isinstance(result, SimpleResponse)
        assert result.answer == "Yes"
        assert result.confidence == 0.95
        mock_client.chat.completions.parse.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": [{"type": "text", "text": "Is this a test?"}]}],
            response_format=SimpleResponse,
        )

    run_async(_test())


def test_openai_prompter_chat_completions_with_system_message():
    """Test Chat Completions API with system message."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "Response with system context."
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            system_message="You are a helpful assistant.",
            use_chat_completions=True,
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Hello!",))

        assert result == "Response with system context."

        # Verify system message was included
        call_args = mock_client.chat.completions.create.call_args
        messages = call_args.kwargs["messages"]
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == "You are a helpful assistant."
        assert messages[1]["role"] == "user"

    run_async(_test())


def test_openai_prompter_chat_completions_with_image():
    """Test Chat Completions API with image (numpy array)."""

    async def _test():
        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "This image shows a cat."
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            use_chat_completions=True,
        )
        prompter.llm = mock_client

        # Create a dummy numpy array image
        image = np.zeros((100, 100, 3), dtype=np.uint8)

        result = await prompter.prompt(("What is in this image?", image))

        assert result == "This image shows a cat."

        # Verify that the call was made with both text and image
        call_args = mock_client.chat.completions.create.call_args
        assert call_args.kwargs["model"] == "gpt-4o-mini"
        messages = call_args.kwargs["messages"]
        assert len(messages) == 1
        assert messages[0]["role"] == "user"
        assert isinstance(messages[0]["content"], list)
        assert messages[0]["content"][0]["type"] == "text"
        assert messages[0]["content"][0]["text"] == "What is in this image?"
        assert messages[0]["content"][1]["type"] == "image_url"
        assert "image_url" in messages[0]["content"][1]
        assert messages[0]["content"][1]["image_url"]["url"].startswith("data:image/png;base64,")

    run_async(_test())


def test_openai_prompter_chat_completions_with_generation_config():
    """Test Chat Completions API with generation config parameters."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "Response with custom config."
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            generation_config={"temperature": 0.8, "max_tokens": 50},
            use_chat_completions=True,
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Tell me a story",))

        assert result == "Response with custom config."
        mock_client.chat.completions.create.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": [{"type": "text", "text": "Tell me a story"}]}],
            temperature=0.8,
            max_tokens=50,
        )

    run_async(_test())


def test_openai_prompter_chat_completions_complex_structured_output():
    """Test Chat Completions API with complex Pydantic model."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        expected_output = ComplexResponse(
            summary="Test summary",
            key_points=["Point 1", "Point 2", "Point 3"],
            sentiment="positive",
        )
        mock_message.parsed = expected_output
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            return_format=ComplexResponse,
            use_chat_completions=True,
        )
        prompter.llm = mock_client

        result = await prompter.prompt(("Summarize this text",))

        assert isinstance(result, ComplexResponse)
        assert result.summary == "Test summary"
        assert len(result.key_points) == 3
        assert result.sentiment == "positive"

    run_async(_test())


def test_openai_provider_get_prompter_with_use_chat_completions():
    """Test that the provider accepts use_chat_completions parameter."""
    provider = OpenAIProvider(api_key="test-key")
    descriptor = provider.get_prompter(model="gpt-4o-mini", use_chat_completions=True)

    assert isinstance(descriptor, OpenAIPrompterDescriptor)
    assert descriptor.use_chat_completions is True

    # Test instantiation
    prompter = descriptor.instantiate()
    assert isinstance(prompter, OpenAIPrompter)
    assert prompter.use_chat_completions is True


def test_openai_prompter_chat_completions_with_image_structured_output():
    """Test Chat Completions API with image and structured output."""

    async def _test():
        from daft.dependencies import np

        mock_client = AsyncMock()
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        expected_output = ComplexResponse(
            summary="Image of a cat",
            key_points=["fluffy", "orange", "sleeping"],
            sentiment="positive",
        )
        mock_message.parsed = expected_output
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            provider_options={"api_key": "test-key"},
            model="gpt-4o-mini",
            return_format=ComplexResponse,
            use_chat_completions=True,
        )
        prompter.llm = mock_client

        # Create a dummy numpy array image
        image = np.zeros((100, 100, 3), dtype=np.uint8)

        result = await prompter.prompt(("Describe this image", image))

        assert isinstance(result, ComplexResponse)
        assert result.summary == "Image of a cat"
        assert len(result.key_points) == 3
        assert result.sentiment == "positive"

        # Verify the call was made with image
        call_args = mock_client.chat.completions.parse.call_args
        messages = call_args.kwargs["messages"]
        assert len(messages) == 1
        assert isinstance(messages[0]["content"], list)
        assert messages[0]["content"][0]["type"] == "text"
        assert messages[0]["content"][1]["type"] == "image_url"

    run_async(_test())
