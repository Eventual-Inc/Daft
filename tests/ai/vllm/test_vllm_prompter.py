from __future__ import annotations

import pytest

import daft
from daft.datatype import DataType
from daft.functions import prompt


@pytest.mark.skip(
    reason="Running any language model locally takes a lot of resources. This is just here for manual testing."
)
def test_vllm_prompter():
    df = daft.from_pydict({"prompt": ["Hello, world!", "This is a test", "Goodbye, world!"]})

    df = df.with_column(
        "response",
        prompt(
            df["prompt"],
            provider="vllm-prefix-caching",
            model="facebook/opt-125m",
        ),
    )

    schema = df.schema()
    assert schema.column_names() == ["prompt", "response"]
    assert schema["prompt"].dtype == DataType.string()
    assert schema["response"].dtype == DataType.string()

    result = df.to_pydict()

    assert len(result["prompt"]) == 3
    assert len(result["response"]) == 3
