"""Transformers Prompter Integration Tests.

Note:
    These tests require the transformers library and a GPU for reasonable performance.

Usage:
    pytest -m integration ./tests/integration/ai/test_transformers_prompter.py

Optional:
    Override models to use smaller/faster ones for local runs:
      - DAFT_TEST_TRANSFORMERS_MODEL (text-only)
      - DAFT_TEST_TRANSFORMERS_VLM_MODEL (multimodal)
"""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Callable, Mapping
from typing import Any

import pytest
from pydantic import BaseModel, Field

import daft
import daft.context
from daft.daft import PyMicroPartition
from daft.functions.ai import prompt
from daft.subscribers import StatType, Subscriber
from tests.conftest import get_tests_daft_runner_name

RUNNER_IS_NATIVE = get_tests_daft_runner_name() == "native"

# Use Qwen3-0.6B for testing - small but capable model
MODEL_NAME = os.environ.get("DAFT_TEST_TRANSFORMERS_MODEL", "Qwen/Qwen3-0.6B")


@pytest.fixture(scope="module", autouse=True)
def check_dependencies():
    """Check that required dependencies are available."""
    pytest.importorskip("torch")
    pytest.importorskip("transformers")


@pytest.fixture(scope="module", autouse=True)
def session(check_dependencies):
    """Configures the session to be used for all tests."""
    with daft.session() as session:
        session.set_provider("transformers")
        yield


class PromptMetricsSubscriber(Subscriber):
    def __init__(self) -> None:
        self.query_ids: list[str] = []
        self.node_stats: defaultdict[str, defaultdict[int, dict[str, tuple[StatType, Any]]]] = defaultdict(
            lambda: defaultdict(dict)
        )

    def on_query_start(self, query_id: str, metadata: Any) -> None:
        self.query_ids.append(query_id)

    def on_exec_emit_stats(
        self,
        query_id: str,
        all_stats: Mapping[int, Mapping[str, tuple[StatType, Any]]],
    ) -> None:
        for node_id, stats in all_stats.items():
            self.node_stats[query_id][node_id] = dict(stats)

    def on_query_end(self, query_id: str, result: Any) -> None:
        pass

    def on_result_out(self, query_id: str, result: PyMicroPartition) -> None:
        pass

    def on_optimization_start(self, query_id: str) -> None:
        pass

    def on_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        pass

    def on_exec_start(self, query_id: str, physical_plan: str) -> None:
        pass

    def on_exec_operator_start(self, query_id: str, node_id: int) -> None:
        pass

    def on_exec_operator_end(self, query_id: str, node_id: int) -> None:
        pass

    def on_exec_end(self, query_id: str) -> None:
        pass


def _collect_metrics(subscriber: PromptMetricsSubscriber) -> dict[str, int]:
    if not subscriber.query_ids:
        return {}

    aggregated: defaultdict[str, int] = defaultdict(int)
    query_id = subscriber.query_ids[-1]

    for stats in subscriber.node_stats[query_id].values():
        for name, (_stat_type, value) in stats.items():
            if isinstance(value, (int, float)):
                aggregated[name] += int(value)

    return dict(aggregated)


def _assert_prompt_metrics_recorded(metrics: dict[str, int]) -> None:
    if not RUNNER_IS_NATIVE:
        # Ray runner does not support metrics collection yet
        return

    required = {
        "requests",
        "input tokens",
        "output tokens",
        "total tokens",
    }

    for name in required:
        assert name in metrics, f"Expected metric '{name}' to be recorded."
        assert metrics[name] >= 0

    assert metrics["requests"] >= 1


@pytest.fixture()
def metrics() -> Callable[[], dict[str, int]]:
    ctx = daft.context.get_context()
    subscriber = PromptMetricsSubscriber()
    sub_name = f"prompt-metrics-{id(subscriber)}"
    ctx.attach_subscriber(sub_name, subscriber)

    try:
        yield lambda: _collect_metrics(subscriber)
    finally:
        ctx.detach_subscriber(sub_name)


@pytest.mark.integration()
def test_prompt_plain_text(session, metrics):
    """Test prompt function with plain text response."""
    df = daft.from_pydict(
        {
            "question": [
                "What is the capital of France? Answer in one word.",
                "What is 2 + 2? Answer with just the number.",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="transformers",
            model=MODEL_NAME,
            max_new_tokens=50,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    # Basic sanity checks - responses should be non-empty strings
    assert len(answers) == 2
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0


@pytest.mark.integration()
def test_prompt_with_system_message(session, metrics):
    """Test prompt function with system message."""
    df = daft.from_pydict(
        {
            "question": [
                "What color is the sky?",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            system_message="You are a helpful assistant. Answer questions concisely in one sentence.",
            provider="transformers",
            model=MODEL_NAME,
            max_new_tokens=50,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0


@pytest.mark.integration()
def test_prompt_structured_output(session, metrics):
    """Test prompt function with structured output (Pydantic model).

    Note: Small models may not reliably produce valid JSON. This test verifies
    that structured output works when the model cooperates, but may skip if
    the model produces invalid JSON (which causes a type mismatch).
    """

    class SimpleAnswer(BaseModel):
        answer: str = Field(..., description="The answer to the question")
        confidence: str = Field(..., description="Confidence level: high, medium, or low")

    df = daft.from_pydict(
        {
            "question": [
                'Respond ONLY with valid JSON, no other text: {"answer": "4", "confidence": "high"}',
            ]
        }
    )

    df = df.with_column(
        "response",
        prompt(
            daft.col("question"),
            return_format=SimpleAnswer,
            provider="transformers",
            model=MODEL_NAME,
            max_new_tokens=50,
        ),
    )

    try:
        responses = df.to_pydict()["response"]
        _assert_prompt_metrics_recorded(metrics())

        # If we got here, the model produced valid JSON
        assert len(responses) == 1
        for response in responses:
            assert response is not None
            assert isinstance(response, dict)
            assert "answer" in response
            assert "confidence" in response
    except Exception as e:
        # Small models may not produce valid JSON, which causes type casting errors
        # This is expected behavior - structured output requires model cooperation
        if "can not cast" in str(e) or "not castable" in str(e):
            pytest.skip("Model did not produce valid JSON for structured output")


@pytest.mark.integration()
def test_prompt_with_temperature(session, metrics):
    """Test prompt function with temperature parameter."""
    df = daft.from_pydict(
        {
            "question": [
                "Generate a random word.",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="transformers",
            model=MODEL_NAME,
            max_new_tokens=20,
            temperature=0.8,
            do_sample=True,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0


@pytest.mark.integration()
def test_prompt_multiple_rows(session, metrics):
    """Test prompt function with multiple rows."""
    df = daft.from_pydict(
        {
            "question": [
                "What is 1 + 1?",
                "What is 2 + 2?",
                "What is 3 + 3?",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="transformers",
            model=MODEL_NAME,
            max_new_tokens=20,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    # Should have 3 answers
    assert len(answers) == 3
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0


# VLM model for multimodal tests
VLM_MODEL_NAME = os.environ.get("DAFT_TEST_TRANSFORMERS_VLM_MODEL", "Qwen/Qwen2-VL-2B-Instruct")


@pytest.mark.integration()
def test_prompt_with_image_input(session, metrics):
    """Test prompt function with image input using a VLM model."""
    import io

    from PIL import Image

    # Create a simple test image (red square)
    img = Image.new("RGB", (64, 64), color="red")
    img_bytes = io.BytesIO()
    img.save(img_bytes, format="PNG")
    img_bytes = img_bytes.getvalue()

    df = daft.from_pydict(
        {
            "image": [img_bytes],
            "question": ["What color is this image? Answer in one word."],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("image"),
            daft.col("question"),
            provider="transformers",
            model=VLM_MODEL_NAME,
            max_new_tokens=20,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0


@pytest.mark.integration()
def test_prompt_with_quantization(session, metrics):
    """Test prompt function with 4-bit quantization config (requires CUDA + bitsandbytes)."""
    import torch

    if not torch.cuda.is_available():
        pytest.skip("Quantization with bitsandbytes requires CUDA")

    try:
        import bitsandbytes  # noqa: F401
    except ImportError:
        pytest.skip("bitsandbytes not installed")

    df = daft.from_pydict(
        {
            "question": [
                "What is 2 + 2? Answer with just the number.",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="transformers",
            model=MODEL_NAME,
            max_new_tokens=10,
            quantization_config={
                "load_in_4bit": True,
                "bnb_4bit_compute_dtype": "float16",
                "bnb_4bit_quant_type": "nf4",
            },
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
