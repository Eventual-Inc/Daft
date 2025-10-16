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
    assert udf_options.concurrency is not None
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

        result = await prompter.prompt("Hello, world!")

        assert result == "This is a test response."
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": "Hello, world!"}],
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

        result = await prompter.prompt("Is this a test?")

        assert isinstance(result, SimpleResponse)
        assert result.answer == "Yes"
        assert result.confidence == 0.95
        mock_client.responses.parse.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": "Is this a test?"}],
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

        result = await prompter.prompt("Tell me a story")

        assert result == "Response with custom config."
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": "Tell me a story"}],
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

        result = await prompter.prompt("Summarize this text")

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

        result1 = await prompter.prompt("First message")
        result2 = await prompter.prompt("Second message")

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
            await prompter.prompt("This will fail")

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
