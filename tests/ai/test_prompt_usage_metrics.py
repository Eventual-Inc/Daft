from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

pytest.importorskip("google.genai")
pytest.importorskip("openai")
from google.genai.types import GenerateContentResponseUsageMetadata
from openai.types.completion_usage import CompletionTokensDetails, CompletionUsage
from openai.types.responses import ResponseUsage
from openai.types.responses.response_usage import OutputTokensDetails

from daft.ai.google.protocols.prompter import GooglePrompter
from daft.ai.openai.protocols.prompter import OpenAIPrompter
from daft.udf.metrics import _metrics_context


@pytest.mark.parametrize("provider", ["google", "chat", "responses"])
@pytest.mark.parametrize("thinking", [None, 0, 5])
def test_provider_to_native_counter(provider, thinking):
    async def run():
        if provider == "google":
            with patch("daft.ai.google.protocols.prompter.genai.Client"):
                obj = GooglePrompter(
                    provider_name="google", provider_options={"api_key": "test-key"}, model="synthetic"
                )
            usage = GenerateContentResponseUsageMetadata(
                prompt_token_count=10,
                candidates_token_count=20,
                thoughts_token_count=thinking,
                total_token_count=30 + (thinking or 0),
            )
            obj.client.aio.models.generate_content = AsyncMock(
                return_value=SimpleNamespace(text="synthetic", usage_metadata=usage)
            )
            output = 20 + (thinking or 0)
        else:
            obj = OpenAIPrompter(
                provider_name="openai",
                provider_options={"api_key": "test-key"},
                model="synthetic",
                prompt_options={"use_chat_completions": provider == "chat"},
            )
            obj.llm = Mock()
            output = 25
            if provider == "chat":
                usage = CompletionUsage(
                    prompt_tokens=10,
                    completion_tokens=25,
                    total_tokens=35,
                    completion_tokens_details=None
                    if thinking is None
                    else CompletionTokensDetails(reasoning_tokens=thinking),
                )
                obj.llm.chat.completions.create = AsyncMock(
                    return_value=SimpleNamespace(
                        usage=usage, choices=[SimpleNamespace(message=SimpleNamespace(content="synthetic"))]
                    )
                )
            else:
                usage = ResponseUsage.model_construct(
                    input_tokens=10,
                    output_tokens=25,
                    total_tokens=35,
                    output_tokens_details=None if thinking is None else OutputTokensDetails(reasoning_tokens=thinking),
                )
                obj.llm.responses.create = AsyncMock(return_value=SimpleNamespace(usage=usage, output_text="synthetic"))
        with _metrics_context() as metrics:
            assert await obj.prompt(("synthetic",)) == "synthetic"
        snap = metrics.snapshot()
        assert snap["output tokens"][0]["value"] == output
        assert snap["input tokens"][0]["value"] == 10
        assert snap["total tokens"][0]["value"] == 10 + output
        assert snap["requests"][0]["value"] == 1
        if thinking is None:
            assert "thinking tokens" not in snap
        else:
            assert snap["thinking tokens"][0]["value"] == thinking
            assert snap["thinking tokens"][0]["attributes"] == {
                "model": "synthetic",
                "protocol": "prompt",
                "provider": "google" if provider == "google" else "openai",
            }

    asyncio.run(run())


@pytest.mark.parametrize("provider", ["google", "chat", "responses"])
@pytest.mark.parametrize("thinking", [None, 0, 5])
def test_dataframe_prompt_records_thinking_tokens(provider, thinking):
    import daft
    from daft.ai.google.provider import GoogleProvider
    from daft.ai.openai.provider import OpenAIProvider
    from daft.functions import prompt

    client = Mock()
    if provider == "google":
        usage = GenerateContentResponseUsageMetadata(
            prompt_token_count=10,
            candidates_token_count=20,
            thoughts_token_count=thinking,
            total_token_count=30 + (thinking or 0),
        )
        client.aio.models.generate_content = AsyncMock(
            return_value=SimpleNamespace(text="synthetic", usage_metadata=usage)
        )
        api_provider = GoogleProvider(api_key="test-key")
        constructor = "daft.ai.google.protocols.prompter.genai.Client"
        output = 20 + (thinking or 0)
        options = {}
    else:
        output = 25
        api_provider = OpenAIProvider(api_key="test-key")
        constructor = "daft.ai.openai.protocols.prompter.AsyncOpenAI"
        options = {"use_chat_completions": provider == "chat"}
        if provider == "chat":
            usage = CompletionUsage(
                prompt_tokens=10,
                completion_tokens=25,
                total_tokens=35,
                completion_tokens_details=(
                    None if thinking is None else CompletionTokensDetails(reasoning_tokens=thinking)
                ),
            )
            client.chat.completions.create = AsyncMock(
                return_value=SimpleNamespace(
                    usage=usage, choices=[SimpleNamespace(message=SimpleNamespace(content="synthetic"))]
                )
            )
        else:
            usage = ResponseUsage.model_construct(
                input_tokens=10,
                output_tokens=25,
                total_tokens=35,
                output_tokens_details=None if thinking is None else OutputTokensDetails(reasoning_tokens=thinking),
            )
            client.responses.create = AsyncMock(return_value=SimpleNamespace(usage=usage, output_text="synthetic"))

    # Use the public prompt expression with its normal async execution defaults.
    with patch(constructor, return_value=client):
        df = daft.from_pydict({"question": ["synthetic one", "synthetic two"]}).select(
            prompt(daft.col("question"), provider=api_provider, model="synthetic", **options).alias("answer")
        )
        df.collect()
    assert df.to_pydict() == {"answer": ["synthetic", "synthetic"]}
    assert df.metrics is not None
    counters = {}
    for operator in df.metrics.to_pylist():
        for name, metric in operator["stats"]:
            counters[name] = counters.get(name, 0) + metric["value"]
    assert counters["input tokens"] == 20
    assert counters["output tokens"] == 2 * output
    assert counters["total tokens"] == 2 * (10 + output)
    assert counters["requests"] == 2
    if thinking is None:
        assert "thinking tokens" not in counters
    else:
        assert counters["thinking tokens"] == 2 * thinking
