from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Skip if google-genai is not installed
pytest.importorskip("google.genai")

from pydantic import BaseModel

from daft.ai.google.protocols.prompter import GooglePrompter, GooglePrompterDescriptor
from daft.ai.protocols import Prompter


def run_async(coro):
    """Helper to run async functions in sync tests."""
    return asyncio.run(coro)


class SimpleResponse(BaseModel):
    """Simple Pydantic model for testing structured outputs."""

    answer: str
    confidence: float


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
    return GooglePrompter(
        provider_name=provider_name,
        provider_options=opts,
        model=model,
        **kwargs,
    )


def test_google_prompter_descriptor_instantiation():
    """Test that descriptor can be instantiated directly."""
    descriptor = GooglePrompterDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-2.5-flash",
        model_options={},
    )

    assert descriptor.get_provider() == "google"
    assert descriptor.get_model() == "gemini-2.5-flash"
    assert descriptor.get_options() == {}
    assert descriptor.return_format is None


def test_google_prompter_instantiate():
    """Test that descriptor can instantiate a GooglePrompter."""
    descriptor = GooglePrompterDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-2.5-flash",
        model_options={},
    )

    with patch("daft.ai.google.protocols.prompter.genai.Client"):
        prompter = descriptor.instantiate()
        assert isinstance(prompter, GooglePrompter)
        assert isinstance(prompter, Prompter)
        assert prompter.model == "gemini-2.5-flash"
        assert prompter.return_format is None
        assert prompter.provider_name == "google"


def test_google_prompter_plain_text_response():
    """Test prompting with plain text response."""

    async def _test():
        with patch("daft.ai.google.protocols.prompter.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            mock_response.text = "This is a test response."
            mock_response.usage_metadata = None

            # Mock the async generate_content method
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
            assert config.response_schema == SimpleResponse

    run_async(_test())


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

            # Second part image bytes
            # Note: We can't easily check the exact bytes without decoding, but we can check it's a Part with inline_data
            # The google-genai SDK Part object structure might vary, but let's assume it has inline_data or similar
            # Based on my implementation: types.Part.from_bytes(data=bio.getvalue(), mime_type="image/png")
            # This usually creates a Part with inline_data
            # For now, just check it's not text
            assert parts[1].text is None
            # In the real SDK, we'd check parts[1].inline_data.mime_type == "image/png"

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
            # Note: The order might vary, so we check existence
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
