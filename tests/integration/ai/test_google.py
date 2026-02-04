"""Google Integration Tests.

Note:
    These tests require a GOOGLE_API_KEY environment variable.

Usage:
    pytest -m integration ./tests/integration/ai/test_google.py --credentials
"""

from __future__ import annotations

import os
import time
from collections import defaultdict

import pytest
from pydantic import BaseModel, Field

import daft
from daft.functions.ai import prompt
from daft.recordbatch import RecordBatch


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="Google integration tests require the `--credentials` flag.")
    if os.environ.get("GOOGLE_API_KEY") is None:
        pytest.skip(reason="Google integration tests require the GOOGLE_API_KEY environment variable.")


@pytest.fixture(scope="module", autouse=True)
def session(skip_no_credential):
    """Configures the session to be used for all tests."""
    with daft.session() as session:
        session.set_provider("google", api_key=os.environ["GOOGLE_API_KEY"])
        yield


def _collect_metrics(metrics: list[dict]) -> dict[str, int]:
    aggregated: defaultdict[str, int] = defaultdict(int)
    for stats in metrics:
        for name, (_stat_type, value) in stats.items():
            if isinstance(value, (int, float)):
                aggregated[name] += int(value)

    return dict(aggregated)


def _assert_prompt_metrics_recorded(metrics: RecordBatch | None) -> None:
    required = {
        "requests",
        "input tokens",
        "output tokens",
        "total tokens",
    }

    metrics_list = _collect_metrics(metrics.to_pylist())

    for name in required:
        assert name in metrics_list, f"Expected metric '{name}' to be recorded."
        assert metrics_list[name] >= 0

    assert metrics_list["requests"] >= 1


@pytest.mark.integration()
def test_prompt_plain_text(session):
    """Test prompt function with plain text response."""
    df = daft.from_pydict(
        {
            "question": [
                "What is the capital of France?",
                "What is 2 + 2?",
                "Name one primary color.",
            ]
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            daft.col("question"),
            provider="google",
            model="gemini-2.5-flash",
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(df.metrics)

    # Basic sanity checks - responses should be non-empty strings
    assert len(answers) == 3
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_structured_output(session):
    """Test prompt function with structured output (Pydantic model)."""

    class MovieReview(BaseModel):
        rating: int = Field(..., description="Rating from 1-5")
        summary: str = Field(..., description="Brief summary")

    df = daft.from_pydict(
        {
            "anime": [
                "Naruto",
                "One Piece",
                "Dragon Ball Z",
            ]
        }
    )

    df = df.with_column(
        "review",
        prompt(
            daft.functions.format(
                "You are an avid anime lover. Rate this anime on a scale of 1-5 and provide a brief summary: {}",
                daft.col("anime"),
            ),
            return_format=MovieReview,
            provider="google",
            model="gemini-2.5-flash",
        ),
    )

    reviews = df.to_pydict()["review"]
    _assert_prompt_metrics_recorded(df.metrics)

    # Verify structured output
    assert len(reviews) == 3
    for review in reviews:
        assert 1 <= review["rating"] <= 5
        assert isinstance(review["summary"], str)
        assert len(review["summary"]) > 0

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_with_image(session):
    """Test prompt function with image input."""
    import numpy as np

    # Create a simple test image (a red square)
    red_square = np.zeros((100, 100, 3), dtype=np.uint8)
    red_square[:, :, 0] = 255  # Red channel

    df = daft.from_pydict(
        {
            "question": [
                "What color is dominant in this image?",
            ],
            "image": [red_square],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image")],
            provider="google",
            model="gemini-2.5-flash",
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(df.metrics)

    # Basic sanity checks - responses should be non-empty strings
    # and should mention red/reddish color
    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        # Check if "red" appears in the answer (case-insensitive)
        assert "red" in answer.lower()

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_with_image_from_bytes(session):
    """Test prompt function with image input from bytes column."""
    import io

    import numpy as np
    from PIL import Image

    # Create a green square image
    green_square = np.zeros((100, 100, 3), dtype=np.uint8)
    green_square[:, :, 1] = 255  # Green channel

    # Convert to PNG bytes directly using BytesIO
    img = Image.fromarray(green_square)
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    image_bytes = bio.getvalue()

    df = daft.from_pydict(
        {
            "question": [
                "Describe this image briefly.",
            ],
            "image_bytes": [image_bytes],
        }
    )

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image_bytes")],
            provider="google",
            model="gemini-2.5-flash",
        ),
    )

    answers = df.to_pydict()["answer"]
    _assert_prompt_metrics_recorded(df.metrics)

    # Basic sanity checks - responses should be non-empty strings
    assert len(answers) == 1
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0
        # Check if "green" appears in the answer (case-insensitive)
        assert "green" in answer.lower()

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_with_image_from_file(session):
    """Test prompt function with image input from File column."""
    import tempfile

    import numpy as np
    from PIL import Image

    # Create a temporary image file (yellow square)
    yellow_square = np.zeros((100, 100, 3), dtype=np.uint8)
    yellow_square[:, :, 0] = 255  # Red channel
    yellow_square[:, :, 1] = 255  # Green channel

    # Save to a temporary file
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        img = Image.fromarray(yellow_square)
        img.save(tmp.name)
        temp_path = tmp.name

    try:
        df = daft.from_pydict(
            {
                "question": [
                    "What color is this image?",
                ],
                "image_path": [temp_path],
            }
        )

        df = df.with_column(
            "answer",
            prompt(
                [daft.col("question"), daft.functions.file(daft.col("image_path"))],
                provider="google",
                model="gemini-2.5-flash",
            ),
        )

        answers = df.to_pydict()["answer"]
        _assert_prompt_metrics_recorded(df.metrics)

        # Basic sanity checks - responses should be non-empty strings
        assert len(answers) == 1
        for answer in answers:
            assert isinstance(answer, str)
            assert len(answer) > 0
            # Check if "yellow" appears in the answer (case-insensitive)
            assert "yellow" in answer.lower()

    finally:
        # Clean up temp file
        import os

        os.unlink(temp_path)

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_with_text_document(session):
    """Test prompt function with plain text document input."""
    import tempfile

    document_contents = "The secret word hidden in this document is pineapple."

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write(document_contents)
        temp_path = tmp.name

    try:
        df = daft.from_pydict(
            {
                "question": [
                    "Read the attached document and tell me the secret word it mentions.",
                ],
                "document_path": [temp_path],
            }
        )

        df = df.with_column(
            "answer",
            prompt(
                [daft.col("question"), daft.functions.file(daft.col("document_path"))],
                provider="google",
                model="gemini-2.5-flash",
            ),
        )

        answers = df.to_pydict()["answer"]
        _assert_prompt_metrics_recorded(df.metrics)

        assert len(answers) == 1
        for answer in answers:
            assert isinstance(answer, str)
            assert len(answer) > 0
            assert "pineapple" in answer.lower()
    finally:
        import os

        os.unlink(temp_path)

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_with_pdf_document(session):
    """Test prompt function with PDF file input."""
    import tempfile

    pytest.importorskip("reportlab")
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    # Create a simple PDF file
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        temp_path = tmp.name

    try:
        # Create PDF with content
        c = canvas.Canvas(temp_path, pagesize=letter)
        c.drawString(100, 750, "This is a test document about artificial intelligence and machine learning.")
        c.drawString(100, 730, "AI and ML are transforming technology and business.")
        c.save()

        df = daft.from_pydict(
            {
                "question": [
                    "Summarize this document in one sentence.",
                ],
                "document_path": [temp_path],
            }
        )

        df = df.with_column(
            "answer",
            prompt(
                [daft.col("question"), daft.functions.file(daft.col("document_path"))],
                provider="google",
                model="gemini-2.5-flash",
            ),
        )

        answers = df.to_pydict()["answer"]
        _assert_prompt_metrics_recorded(df.metrics)

        # Basic sanity checks
        assert len(answers) == 1
        for answer in answers:
            assert isinstance(answer, str)
            assert len(answer) > 0
            # Check if relevant keywords appear in the answer (case-insensitive)
            answer_lower = answer.lower()
            assert (
                "artificial intelligence" in answer_lower or "machine learning" in answer_lower or "ai" in answer_lower
            )

    finally:
        import os

        os.unlink(temp_path)

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_with_image_structured_output(session):
    """Test prompt function with image input and structured output."""
    import numpy as np

    class ImageAnalysis(BaseModel):
        dominant_color: str = Field(..., description="The dominant color in the image")
        description: str = Field(..., description="Brief description of the image")

    # Create a simple test image (a blue square)
    blue_square = np.zeros((100, 100, 3), dtype=np.uint8)
    blue_square[:, :, 2] = 255  # Blue channel

    df = daft.from_pydict(
        {
            "question": [
                "Analyze this image and describe it.",
            ],
            "image": [blue_square],
        }
    )

    df = df.with_column(
        "analysis",
        prompt(
            [daft.col("question"), daft.col("image")],
            return_format=ImageAnalysis,
            provider="google",
            model="gemini-2.5-flash",
        ),
    )

    analyses = df.to_pydict()["analysis"]
    _assert_prompt_metrics_recorded(df.metrics)

    # Verify structured output
    assert len(analyses) == 1
    for analysis in analyses:
        assert isinstance(analysis["dominant_color"], str)
        assert isinstance(analysis["description"], str)
        assert len(analysis["dominant_color"]) > 0
        assert len(analysis["description"]) > 0
        # Check if "blue" appears in the response (case-insensitive)
        response_text = (analysis["dominant_color"] + " " + analysis["description"]).lower()
        assert "blue" in response_text

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
def test_prompt_with_mixed_image_and_document(session):
    """Test prompt function with both image and PDF document inputs."""
    import tempfile

    import numpy as np

    pytest.importorskip("reportlab")
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    # Create a red square image
    red_square = np.zeros((100, 100, 3), dtype=np.uint8)
    red_square[:, :, 0] = 255  # Red channel

    # Create a simple PDF file
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        temp_path = tmp.name

    try:
        # Create PDF with content
        c = canvas.Canvas(temp_path, pagesize=letter)
        c.drawString(100, 750, "This document describes a red colored object.")
        c.save()

        df = daft.from_pydict(
            {
                "question": [
                    "Does the image match the description in the document?",
                ],
                "image": [red_square],
                "document_path": [temp_path],
            }
        )

        df = df.with_column(
            "answer",
            prompt(
                [daft.col("question"), daft.col("image"), daft.functions.file(daft.col("document_path"))],
                provider="google",
                model="gemini-2.5-flash",
            ),
        )

        answers = df.to_pydict()["answer"]
        _assert_prompt_metrics_recorded(df.metrics)

        # Basic sanity checks
        assert len(answers) == 1
        for answer in answers:
            assert isinstance(answer, str)
            assert len(answer) > 0
            # Check if answer indicates a match or mentions red color (case-insensitive)
            answer_lower = answer.lower()
            assert "yes" in answer_lower or "match" in answer_lower or "red" in answer_lower

    finally:
        import os

        os.unlink(temp_path)

    time.sleep(1)  # self limit to ~1 tps.
