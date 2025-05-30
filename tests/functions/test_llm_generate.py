from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from daft import Expression, Series, lit
from daft.functions.llm_generate import _vLLMGenerator, llm_generate

vllm = pytest.importorskip("vllm")


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
    with pytest.raises(ValueError):
        llm_generate(lit("foo"), provider="unsupported")


def test_returns_expression():
    out = llm_generate(lit("foo"), provider="vllm")
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
