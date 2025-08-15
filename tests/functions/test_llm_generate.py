from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from daft import Expression, Series, lit
from daft.functions.llm import _OpenAIGenerator, _vLLMGenerator, llm_generate

vllm = pytest.importorskip("vllm")
openai = pytest.importorskip("openai")


@patch("vllm.LLM")
def test_llm_generate_init(mock_llm: MagicMock):
    _vLLMGenerator(model="facebook/opt-125m", generation_config={"temperature": 0.5})
    assert mock_llm.call_count == 1


@patch("vllm.LLM")
def test_llm_generate_init_error(mock_llm: MagicMock):
    mock_llm.side_effect = ImportError("Please install the vllm package to use this provider.")
    with pytest.raises(ImportError):
        _vLLMGenerator(model="facebook/opt-125m", generation_config={"temperature": 0.5})


def test_unsupported_provider_error():
    with pytest.raises(ValueError, match="Unsupported provider: unsupported"):
        llm_generate(lit("foo"), provider="unsupported")


def test_returns_expression():
    out = llm_generate(lit("foo"), provider="vllm")
    assert isinstance(out, Expression)
    assert repr(out) == 'py_udf(lit("foo"))'


def test_returns_expression_openai():
    out = llm_generate(lit("foo"), provider="openai")
    assert isinstance(out, Expression)
    assert repr(out) == 'py_udf(lit("foo"))'


@patch("vllm.LLM")
@patch("vllm.SamplingParams")
def test_llm_generate_generate(mock_sampling_params: MagicMock, mock_llm: MagicMock):
    # Create mock components
    mock_llm.return_value.generate.return_value = [MagicMock(outputs=[MagicMock(text="This is a mocked response")])]

    llm_generator = _vLLMGenerator(model="facebook/opt-125m", generation_config={"temperature": 0.5})
    series = Series.from_pylist(["This is a test prompt"])
    res = llm_generator(series)
    assert res == ["This is a mocked response"]
    assert mock_sampling_params.call_count == 1
    assert mock_llm.call_count == 1


@patch("openai.AsyncOpenAI")
def test_init_with_client_params_openai(mock_llm: MagicMock):
    """Test that _OpenAIGenerator initializes with client parameters correctly."""
    config = {"api_key": "test_key", "base_url": "http://test.url", "timeout": 30, "max_retries": 3, "temperature": 0.5}

    generator = _OpenAIGenerator(generation_config=config)

    mock_llm.assert_called_once_with(api_key="test_key", base_url="http://test.url", timeout=30, max_retries=3)
    assert generator.generation_config == {"temperature": 0.5}


@patch("openai.AsyncOpenAI")
def test_llm_generate_generate_openai(mock_llm: MagicMock):
    # Create mock components
    mock_completion = MagicMock()
    mock_completion.choices = [MagicMock(message=MagicMock(content="This is a mocked response"))]

    # Create an async mock that returns the completion
    async def async_mock(*args, **kwargs):
        return mock_completion

    mock_llm.return_value.chat.completions.create = async_mock

    llm_generator = _OpenAIGenerator(generation_config={"temperature": 0.5})
    series = Series.from_pylist(["This is a test prompt"])
    res = llm_generator(series)
    assert res == ["This is a mocked response"]
