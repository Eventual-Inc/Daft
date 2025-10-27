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
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
        descriptor = provider.get_prompter()

    assert isinstance(descriptor, OpenAIPrompterDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "gpt-4o-mini"
    assert descriptor.get_options() == {}
    assert descriptor.return_format is None


def test_openai_provider_get_prompter_with_model():
    """Test that the provider accepts custom model names."""
    provider = OpenAIProvider(api_key="test-key")
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
        descriptor = provider.get_prompter(model="gpt-4o")

    assert isinstance(descriptor, OpenAIPrompterDescriptor)
    assert descriptor.get_model() == "gpt-4o"


def test_openai_provider_get_prompter_with_return_format():
    """Test that the provider accepts return_format parameter."""
    provider = OpenAIProvider(api_key="test-key")
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
        descriptor = provider.get_prompter(model="gpt-4o-mini", return_format=SimpleResponse)

    assert isinstance(descriptor, OpenAIPrompterDescriptor)
    assert descriptor.return_format == SimpleResponse


def test_openai_provider_get_prompter_with_options():
    """Test that the provider accepts generation config options."""
    provider = OpenAIProvider(api_key="test-key")
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
        descriptor = provider.get_prompter(
            model="gpt-4o-mini",
            temperature=0.7,
            max_tokens=100,
        )

    assert descriptor.get_options() == {"temperature": 0.7, "max_tokens": 100}


def test_openai_provider_get_prompter_with_use_chat_completions():
    """Test that the provider accepts use_chat_completions parameter."""
    provider = OpenAIProvider(api_key="test-key", use_chat_completions=True)
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
        descriptor: OpenAIPrompterDescriptor = provider.get_prompter(model="gpt-4o-mini")

    assert descriptor.get_use_chat_completions() is True
    assert descriptor.get_options() == {}
    assert descriptor.get_provider_options() == {"api_key": "test-key"}


def test_openai_prompter_descriptor_instantiation():
    """Test that descriptor can be instantiated directly."""
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
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
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
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
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
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
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
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


def test_openai_prompter_instantiate_with_use_chat_completions():
    """Test that descriptor can instantiate an OpenAIPrompter."""
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
        descriptor = OpenAIPrompterDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key"},
            model_name="gpt-4o-mini",
            model_options={"use_chat_completions": True},
        )

    prompter = descriptor.instantiate()
    assert isinstance(prompter, OpenAIPrompter)
    assert isinstance(prompter, Prompter)
    assert prompter.model == "gpt-4o-mini"
    assert prompter.return_format is None
    assert prompter.use_chat_completions is True
    assert descriptor.get_use_chat_completions() is True
    assert descriptor.get_options() == {}


def test_openai_prompter_plain_text_response():
    """Test prompting with plain text response."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "This is a test response."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=None,
            use_chat_completions=False,
        )

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
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=SimpleResponse,
            use_chat_completions=False,
        )

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
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=None,
            use_chat_completions=False,
            generation_config={"temperature": 0.8, "max_tokens": 50},
        )

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
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=ComplexResponse,
            use_chat_completions=False,
        )

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
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=None,
            use_chat_completions=False,
        )

        result1 = await prompter.prompt("First message")
        result2 = await prompter.prompt("Second message")

        assert result1 == "First response"
        assert result2 == "Second response"
        assert mock_client.responses.create.call_count == 2

    run_async(_test())


def test_openai_prompter_client_and_generation_config_forwarding():
    """Test client creation uses provider options and generation_config forwards to API calls."""
    with (
        patch("daft.ai.openai.protocols.prompter.AsyncOpenAI") as mock_openai_class,
        patch("daft.ai.openai.protocols.prompter.validate_model_availability"),
    ):
        mock_client = AsyncMock()
        mock_openai_class.return_value = mock_client

        from daft.ai.openai.protocols.prompter import OpenAIPrompterDescriptor

        descriptor = OpenAIPrompterDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key", "base_url": "https://api.example.com"},
            model_name="gpt-4o-mini",
            model_options={
                "temperature": 0.7,
                "max_tokens": 100,
                "timeout": 30.0,  # forwarded to API call now
            },
        )

        prompter = descriptor.instantiate()

        # Verify client was created with only provider options
        mock_openai_class.assert_called_once()
        client_call_kwargs = mock_openai_class.call_args.kwargs
        assert client_call_kwargs["api_key"] == "test-key"
        assert client_call_kwargs["base_url"] == "https://api.example.com"
        assert "temperature" not in client_call_kwargs
        assert "max_tokens" not in client_call_kwargs
        assert "timeout" not in client_call_kwargs

        # Prepare response
        mock_response = Mock()
        mock_response.output_text = "Response with forwarded config."
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        # Call prompt and verify forwarding
        result = asyncio.run(prompter.prompt("Tell me a story"))
        assert result == "Response with forwarded config."
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": "Tell me a story"}],
            temperature=0.7,
            max_tokens=100,
            timeout=30.0,
        )


def test_openai_prompter_error_handling():
    """Test that errors from the API are propagated."""

    async def _test():
        mock_client = AsyncMock()
        mock_client.responses.create = AsyncMock(side_effect=Exception("API Error"))

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=None,
            use_chat_completions=False,
        )

        with pytest.raises(ValueError, match="Error prompting model: API Error"):
            await prompter.prompt("This will fail")

    run_async(_test())


def test_protocol_compliance():
    """Test that OpenAIPrompter implements the Prompter protocol."""
    prompter = OpenAIPrompter(
        client=AsyncMock(),
        model="gpt-4o-mini",
        system_message=None,
        return_format=None,
        use_chat_completions=False,
    )

    assert isinstance(prompter, Prompter)
    assert hasattr(prompter, "prompt")
    assert callable(prompter.prompt)


def test_openai_prompter_attributes():
    """Test that OpenAIPrompter stores attributes correctly."""
    prompter = OpenAIPrompter(
        client=AsyncMock(),
        model="gpt-4o",
        system_message=None,
        return_format=SimpleResponse,
        use_chat_completions=False,
        generation_config={"temperature": 0.5},
    )

    assert prompter.model == "gpt-4o"
    assert prompter.return_format == SimpleResponse
    assert prompter.generation_config == {"temperature": 0.5}
    assert hasattr(prompter, "client")


def test_openai_prompter_chat_structured_output():
    """Test chat.completions with structured output (Pydantic model)."""

    async def _test():
        mock_client = AsyncMock()
        mock_choice = Mock()
        mock_message = Mock()
        expected_output = ComplexResponse(
            summary="Test summary",
            key_points=["Point 1", "Point 2"],
            sentiment="positive",
        )
        mock_message.parsed = expected_output
        mock_message.refusal = None
        mock_choice.message = mock_message
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=ComplexResponse,
            use_chat_completions=True,
        )

        result = await prompter.prompt("Summarize this text")
        assert isinstance(result, ComplexResponse)
        assert result.summary == "Test summary"
        mock_client.chat.completions.parse.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Summarize this text"}],
            response_format=ComplexResponse,
        )

    run_async(_test())


def test_openai_prompter_chat_refusal_raises():
    """Test that a refusal in chat.completions raises a ValueError with context."""

    async def _test():
        mock_client = AsyncMock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.refusal = "policy refusal"
        mock_choice.message = mock_message
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message="sys",
            return_format=ComplexResponse,
            use_chat_completions=True,
        )

        with pytest.raises(ValueError, match="Model refused"):
            await prompter.prompt("Hi")

    run_async(_test())


def test_openai_prompter_system_message_injection_responses():
    """System message should be prepended for responses path."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "ok"
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message="sys",
            return_format=None,
            use_chat_completions=False,
        )

        await prompter.prompt("Hi")
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "system", "content": "sys"}, {"role": "user", "content": "Hi"}],
        )

    run_async(_test())


def test_openai_prompter_system_message_injection_chat():
    """System message should be prepended for chat path."""

    async def _test():
        mock_client = AsyncMock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "ok"
        mock_choice.message = mock_message
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message="sys",
            return_format=None,
            use_chat_completions=True,
        )

        result = await prompter.prompt("Hi")
        assert result == "ok"
        mock_client.chat.completions.create.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "sys"}, {"role": "user", "content": "Hi"}],
        )

    run_async(_test())


def test_openai_prompter_chat_generation_config_forwarding():
    """Chat path should forward generation_config kwargs to the API call."""

    async def _test():
        mock_client = AsyncMock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "conf ok"
        mock_choice.message = mock_message
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=None,
            use_chat_completions=True,
            generation_config={"temperature": 0.6, "max_tokens": 42, "timeout": 12.3},
        )

        result = await prompter.prompt("Hi")
        assert result == "conf ok"
        mock_client.chat.completions.create.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Hi"}],
            temperature=0.6,
            max_tokens=42,
            timeout=12.3,
        )

    run_async(_test())


def test_openai_prompter_system_message_injection_chat_parse():
    """System message should be prepended for chat parse path as well."""

    async def _test():
        mock_client = AsyncMock()
        mock_choice = Mock()
        mock_message = Mock()
        expected_output = SimpleResponse(answer="ok", confidence=1.0)
        mock_message.parsed = expected_output
        mock_message.refusal = None
        mock_choice.message = mock_message
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_client.chat.completions.parse = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message="sys",
            return_format=SimpleResponse,
            use_chat_completions=True,
        )

        result = await prompter.prompt("Hi")
        assert isinstance(result, SimpleResponse)
        assert result.answer == "ok"
        mock_client.chat.completions.parse.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "sys"}, {"role": "user", "content": "Hi"}],
            response_format=SimpleResponse,
        )

    run_async(_test())


def test_openai_prompter_parsing_json_list_input():
    """Test that prompt accepts a JSON string list of messages."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "ok"
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=None,
            use_chat_completions=False,
        )

        messages_json = '[{"role": "user", "content": "hi"}]'
        await prompter.prompt(messages_json)
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": "hi"}],
        )

    run_async(_test())


def test_openai_prompter_parsing_single_message_dict():
    """Test that prompt accepts a single message dict JSON string and wraps it in a list."""

    async def _test():
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.output_text = "ok"
        mock_client.responses.create = AsyncMock(return_value=mock_response)

        prompter = OpenAIPrompter(
            client=mock_client,
            model="gpt-4o-mini",
            system_message=None,
            return_format=None,
            use_chat_completions=False,
        )

        messages_json = '{"role": "user", "content": "hi"}'
        await prompter.prompt(messages_json)
        mock_client.responses.create.assert_called_once_with(
            model="gpt-4o-mini",
            input=[{"role": "user", "content": "hi"}],
        )

    run_async(_test())


def test_openai_prompter_descriptor_normalize_and_validate():
    """Descriptor should normalize model name and validate availability."""
    with (
        patch("daft.ai.openai.protocols.prompter.normalize_model_name", return_value="normalized") as mock_norm,
        patch("daft.ai.openai.protocols.prompter.validate_model_availability") as mock_validate,
        patch("daft.ai.openai.protocols.prompter.OpenAI") as mock_openai,
    ):
        descriptor = OpenAIPrompterDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key", "base_url": "https://api.example.com"},
            model_name="gpt-4o-mini",
            model_options={},
        )

        # normalized applied
        assert descriptor.get_model() == "normalized"
        mock_norm.assert_called_once_with("gpt-4o-mini", "https://api.example.com")

        # validate called with OpenAI instance and normalized name
        assert mock_openai.called
        mock_validate.assert_called_once()


def test_openai_prompter_use_chat_completions_flag_resolution():
    """use_chat_completions should resolve from provider or model options."""
    with patch("daft.ai.openai.protocols.prompter.validate_model_availability"):
        d1 = OpenAIPrompterDescriptor(
            provider_name="openai",
            provider_options={"api_key": "k", "use_chat_completions": True},
            model_name="gpt-4o-mini",
            model_options={},
        )
        assert d1.get_use_chat_completions() is True

        d2 = OpenAIPrompterDescriptor(
            provider_name="openai",
            provider_options={"api_key": "k"},
            model_name="gpt-4o-mini",
            model_options={"use_chat_completions": True},
        )
        assert d2.get_use_chat_completions() is True
