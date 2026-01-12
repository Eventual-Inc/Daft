"""Transformers Integration Tests.

Note:
    These tests require GPU hardware and will download models from HuggingFace.

Models used:
    - Text-only: Qwen/Qwen3-0.6B (uses AutoModelForCausalLM)
    - Vision: Qwen/Qwen3-VL-2B-instruct (uses AutoModelForImageTextToText)

Usage:
    pytest -m integration ./tests/integration/ai/test_transformers.py --credentials
"""

from __future__ import annotations

import io
import os
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Mapping
from typing import Any

import pytest

import daft
import daft.context
from daft.daft import PyMicroPartition
from daft.functions.ai import prompt
from daft.subscribers import StatType, Subscriber
from tests.conftest import get_tests_daft_runner_name

RUNNER_IS_NATIVE = get_tests_daft_runner_name() == "native"

# Model names
TEXT_MODEL = "Qwen/Qwen3-0.6B"
VISION_MODEL = "Qwen/Qwen3-VL-2B-instruct"


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    """Skip tests if --credentials flag not provided or no GPU available."""
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="Transformers integration tests require the `--credentials` flag.")

    try:
        import torch

        if not torch.cuda.is_available():
            # Also check for MPS on Mac
            if not (hasattr(torch.backends, "mps") and torch.backends.mps.is_available()):
                pytest.skip(reason="Transformers integration tests require GPU (CUDA or MPS).")
    except ImportError:
        pytest.skip(reason="Transformers integration tests require torch.")


@pytest.fixture(scope="module", autouse=True)
def session(skip_no_credential):
    """Configures the session to be used for all tests."""
    with daft.session() as session:
        session.set_provider("transformers")
        yield


class PromptMetricsSubscriber(Subscriber):
    """Subscriber to collect prompt metrics during test execution."""

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
    """Collect metrics from the subscriber."""
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
    """Assert that prompt metrics were recorded correctly."""
    if not RUNNER_IS_NATIVE:
        # Ray runner does not support metrics collection yet
        return

    required = {
        "input tokens",
        "output tokens",
        "total tokens",
    }

    for name in required:
        assert name in metrics, f"Expected metric '{name}' to be recorded."
        assert metrics[name] >= 0


@pytest.fixture()
def metrics() -> Callable[[], dict[str, int]]:
    """Fixture to collect metrics during tests."""
    ctx = daft.context.get_context()
    subscriber = PromptMetricsSubscriber()
    sub_name = f"prompt-metrics-{id(subscriber)}"
    ctx.attach_subscriber(sub_name, subscriber)

    try:
        yield lambda: _collect_metrics(subscriber)
    finally:
        ctx.detach_subscriber(sub_name)


# =============================================================================
# Text-Only Model Tests (Qwen3-0.6B)
# =============================================================================


@pytest.mark.integration()
def test_prompt_plain_text_causal(session, metrics):
    """Test prompt function with plain text response using text-only model."""
    df = daft.from_pydict(
        {
            "question": [
                "What is the capital of France? Answer in one word.",
                "What is 2+2? Answer in one word.",
                "What is the opposite of up? Answer in one word.",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="transformers",
            model=TEXT_MODEL,
            max_new_tokens=50,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    # Basic sanity checks - responses should be non-empty strings
    assert len(answers) == 3
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0

    # Check content roughly
    assert "paris" in answers[0].lower()
    assert "4" in answers[1]
    assert "down" in answers[2].lower()

    time.sleep(1)  # Brief pause between tests


@pytest.mark.integration()
def test_prompt_with_system_message_causal(session, metrics):
    """Test prompt function with system message using text-only model."""
    df = daft.from_pydict(
        {
            "question": [
                "Hi!",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            system_message="You are a pirate. Always respond like a pirate.",
            provider="transformers",
            model=TEXT_MODEL,
            max_new_tokens=50,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0

    time.sleep(1)


# =============================================================================
# Vision Model Tests (Qwen3-VL-2B-instruct)
# =============================================================================


@pytest.mark.integration()
def test_prompt_plain_text_vision(session, metrics):
    """Test that text-only prompts work with vision model."""
    df = daft.from_pydict(
        {
            "question": [
                "Name one primary color. Answer in one word.",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="transformers",
            model=VISION_MODEL,
            max_new_tokens=20,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        assert ("red" in answer.lower()) or ("green" in answer.lower()) or ("blue" in answer.lower()), (
            f"Expected 'red', 'green', or 'blue' in answer, got: {answer}"
        )

    time.sleep(1)


@pytest.mark.integration()
def test_prompt_with_image_red(session, metrics):
    """Test prompt function with red image - should detect red color."""
    import numpy as np

    # Create a red square
    red_square = np.zeros((100, 100, 3), dtype=np.uint8)
    red_square[:, :, 0] = 255  # Red channel

    df = daft.from_pydict(
        {
            "question": [
                "What color is dominant in this image? Answer with just the color name.",
            ],
            "image": [red_square],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image")],
            provider="transformers",
            model=VISION_MODEL,
            max_new_tokens=20,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        # Check if "red" appears in the answer (case-insensitive)
        assert "red" in answer.lower(), f"Expected 'red' in answer, got: {answer}"

    time.sleep(1)


@pytest.mark.integration()
def test_prompt_with_image_green(session, metrics):
    """Test prompt function with green image - should detect green color."""
    import numpy as np

    # Create a green square
    green_square = np.zeros((100, 100, 3), dtype=np.uint8)
    green_square[:, :, 1] = 255  # Green channel

    df = daft.from_pydict(
        {
            "question": [
                "What color is dominant in this image? Answer with just the color name.",
            ],
            "image": [green_square],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image")],
            provider="transformers",
            model=VISION_MODEL,
            max_new_tokens=20,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        # Check if "green" appears in the answer (case-insensitive)
        assert "green" in answer.lower(), f"Expected 'green' in answer, got: {answer}"

    time.sleep(1)


@pytest.mark.integration()
def test_prompt_with_image_blue(session, metrics):
    """Test prompt function with blue image - should detect blue color."""
    import numpy as np

    # Create a blue square
    blue_square = np.zeros((100, 100, 3), dtype=np.uint8)
    blue_square[:, :, 2] = 255  # Blue channel

    df = daft.from_pydict(
        {
            "question": [
                "What color is dominant in this image? Answer with just the color name.",
            ],
            "image": [blue_square],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image")],
            provider="transformers",
            model=VISION_MODEL,
            max_new_tokens=20,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        # Check if "blue" appears in the answer (case-insensitive)
        assert "blue" in answer.lower(), f"Expected 'blue' in answer, got: {answer}"

    time.sleep(1)


@pytest.mark.integration()
def test_prompt_with_image_from_bytes(session, metrics):
    """Test prompt function with image input from bytes column."""
    import numpy as np
    from PIL import Image

    # Create a cyan square image
    cyan_square = np.zeros((100, 100, 3), dtype=np.uint8)
    cyan_square[:, :, 1] = 255  # Green channel
    cyan_square[:, :, 2] = 255  # Blue channel

    # Convert to PNG bytes
    img = Image.fromarray(cyan_square)
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    image_bytes = bio.getvalue()

    df = daft.from_pydict(
        {
            "question": [
                "Describe the color in this image briefly.",
            ],
            "image_bytes": [image_bytes],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image_bytes")],
            provider="transformers",
            model=VISION_MODEL,
            max_new_tokens=30,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        # Cyan could be described as cyan, blue-green, turquoise, etc.
        answer_lower = answer.lower()
        assert any(color in answer_lower for color in ["cyan", "blue", "green", "turquoise", "teal"]), (
            f"Expected cyan-related color in answer, got: {answer}"
        )

    time.sleep(1)


@pytest.mark.integration()
def test_prompt_with_image_from_file(session, metrics):
    """Test prompt function with image input from File column."""
    import numpy as np
    from PIL import Image

    # Create a magenta square
    magenta_square = np.zeros((100, 100, 3), dtype=np.uint8)
    magenta_square[:, :, 0] = 255  # Red channel
    magenta_square[:, :, 2] = 255  # Blue channel

    # Save to a temporary file
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        img = Image.fromarray(magenta_square)
        img.save(tmp.name)
        temp_path = tmp.name

    try:
        df = daft.from_pydict(
            {
                "question": [
                    "What color is this image? Just say the color.",
                ],
                "image_path": [temp_path],
            }
        )

        df = df.with_column(
            "answer",
            prompt(
                [daft.col("question"), daft.functions.file(daft.col("image_path"))],
                provider="transformers",
                model=VISION_MODEL,
                max_new_tokens=20,
            ),
        )

        answers = df.to_pydict()["answer"]
        _assert_prompt_metrics_recorded(metrics())

        assert len(answers) == 1
        for answer in answers:
            assert isinstance(answer, str)
            assert len(answer) > 0
            # Magenta could be described as magenta, pink, purple, etc.
            answer_lower = answer.lower()
            assert any(color in answer_lower for color in ["magenta", "pink", "purple", "red", "violet"]), (
                f"Expected magenta-related color in answer, got: {answer}"
            )

    finally:
        os.unlink(temp_path)

    time.sleep(1)


@pytest.mark.integration()
def test_prompt_with_text_document(session, metrics):
    """Test prompt function with plain text document input."""
    document_contents = "The secret word hidden in this document is pineapple."

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write(document_contents)
        temp_path = tmp.name

    try:
        df = daft.from_pydict(
            {
                "question": [
                    "Read the document and tell me the secret word. Answer with just the word.",
                ],
                "document_path": [temp_path],
            }
        )

        df = df.with_column(
            "answer",
            prompt(
                [daft.col("question"), daft.functions.file(daft.col("document_path"))],
                provider="transformers",
                model=TEXT_MODEL,  # Text model is sufficient for text documents
                max_new_tokens=128,
            ),
        )

        answers = df.to_pydict()["answer"]
        _assert_prompt_metrics_recorded(metrics())

        assert len(answers) == 1
        for answer in answers:
            assert isinstance(answer, str)
            assert len(answer) > 0
            assert "pineapple" in answer.lower(), f"Expected 'pineapple' in answer, got: {answer}"

    finally:
        os.unlink(temp_path)

    time.sleep(1)


@pytest.mark.integration()
def test_prompt_with_mixed_image_and_text(session, metrics):
    """Test prompt function with both image and text in the same prompt."""
    import numpy as np

    # Create an orange square
    orange_square = np.zeros((100, 100, 3), dtype=np.uint8)
    orange_square[:, :, 0] = 255  # Red channel
    orange_square[:, :, 1] = 165  # Some green

    df = daft.from_pydict(
        {
            "instruction": [
                "Look at this image.",
            ],
            "image": [orange_square],
            "question": [
                "What color is it? Just say the color name.",
            ],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("instruction"), daft.col("image"), daft.col("question")],
            provider="transformers",
            model=VISION_MODEL,
            max_new_tokens=20,
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        # Orange could be described as orange, red-orange, amber, etc.
        answer_lower = answer.lower()
        assert any(color in answer_lower for color in ["orange", "red", "amber", "yellow"]), (
            f"Expected orange-related color in answer, got: {answer}"
        )

    time.sleep(1)


@pytest.mark.integration()
def test_batched_mixed_requests(session, metrics):
    """Test processing multiple rows with mixed content types (text-only and text+image)."""
    import numpy as np

    # 1. Plain text question
    q1 = "What is 1 + 1? Answer with just the number."
    img1 = None

    # 2. Image question (Red)
    q2 = "What color is this? Answer with just the color."
    img2 = np.zeros((100, 100, 3), dtype=np.uint8)
    img2[:, :, 0] = 255

    # 3. Another text question
    q3 = "Say 'hello' in uppercase."
    img3 = None

    df = daft.from_pydict(
        {
            "question": [q1, q2, q3],
            "image": [img1, img2, img3],
            "id": [1, 2, 3],
        }
    )

    # Use VISION_MODEL as it can handle images. It should also handle text-only if image is None (skipped).
    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image")],
            provider="transformers",
            model=VISION_MODEL,
            max_new_tokens=20,
        ),
    )

    results = df.sort("id").to_pydict()
    answers = results["answer"]

    _assert_prompt_metrics_recorded(metrics())

    assert len(answers) == 3

    # Check 1
    assert "2" in answers[0]

    # Check 2
    assert "red" in answers[1].lower()

    # Check 3
    assert "HELLO" in answers[2]

    time.sleep(1)


# =============================================================================
# Metrics Tests
# =============================================================================


@pytest.mark.integration()
def test_prompt_metrics_recorded(session, metrics):
    """Test that token metrics are recorded correctly."""
    df = daft.from_pydict(
        {
            "question": [
                "Say hello.",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="transformers",
            model=TEXT_MODEL,
            max_new_tokens=10,
        ),
    )

    df.collect()
    recorded_metrics = metrics()

    # Verify metrics were recorded
    _assert_prompt_metrics_recorded(recorded_metrics)

    if RUNNER_IS_NATIVE:
        # Input tokens should be > 0
        assert recorded_metrics.get("input tokens", 0) > 0
        # Output tokens should be > 0
        assert recorded_metrics.get("output tokens", 0) > 0
        # Total should be sum of input + output
        assert recorded_metrics.get("total tokens", 0) > 0

    time.sleep(1)
