from unittest.mock import MagicMock, patch

import pytest

from daft import Series
from daft.functions.llm_generate import _LLMGenerator


@patch("vllm.LLM")
def test_llm_generate_init(mock_llm: MagicMock):
    _LLMGenerator(model="facebook/opt-125m", provider="vllm", generation_config={"temperature": 0.5})
    assert mock_llm.call_count == 1


@patch("vllm.LLM")
def test_llm_generate_init_error(mock_llm: MagicMock):
    mock_llm.side_effect = ImportError("Please install the vllm package to use this provider.")
    with pytest.raises(ImportError):
        _LLMGenerator(model="facebook/opt-125m", provider="vllm", generation_config={"temperature": 0.5})


@patch("vllm.LLM")
@patch("vllm.SamplingParams")
def test_llm_generate_generate(mock_sampling_params: MagicMock, mock_llm: MagicMock):
    # Create mock components
    mock_llm.return_value.generate.return_value = [MagicMock(outputs=[MagicMock(text="This is a mocked response")])]

    instance = _LLMGenerator(model="facebook/opt-125m", provider="vllm", generation_config={"temperature": 0.5})
    series = Series.from_pylist(["This is a test prompt"])
    res = instance.__call__(series)
    assert res == ["This is a mocked response"]
    assert mock_sampling_params.call_count == 1
    assert mock_llm.call_count == 1
