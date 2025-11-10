"""OpenAI Integration Tests.

Note:
    These tests require an OPENAI_API_KEY environment variable WITH credit.

Usage:
    pytest -m integration ./tests/integration/ai/test_openai.py --credentials
"""

from __future__ import annotations

import os
import time

import pytest
from pydantic import BaseModel, Field

import daft
from daft.functions.ai import embed_text, prompt


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="OpenAI integration tests require the `--credentials` flag.")
    if os.environ.get("OPENAI_API_KEY") is None:
        pytest.skip(reason="OpenAI integration tests require the OPENAI_API_KEY environment variable.")


@pytest.fixture(scope="module", autouse=True)
def session(skip_no_credential):
    """Configures the session to be used for all tests."""
    with daft.session() as session:
        # the key is not explicitly needed, but was added with angry lookup for clarity.
        session.set_provider("openai", api_key=os.environ["OPENAI_API_KEY"])
        yield


@pytest.mark.integration()
def test_embed_text_sanity_all_models(session):
    """This tests end-to-end doesn't throw for all models. It does not check outputs."""
    from daft.ai.openai.protocols.text_embedder import _models

    df = daft.from_pydict(
        {
            "text": [
                "Alice was beginning to get very tired of sitting by her sister on the bank.",
                "So she was considering in her own mind (as well as she could, for the hot day made her feel very sleepy and stupid),",
                "whether the pleasure of making a daisy-chain would be worth the trouble of getting up and picking the daisies,",
                "when suddenly a White Rabbit with pink eyes ran close by her.",
                "There was nothing so very remarkable in that;",
                "nor did Alice think it so very much out of the way to hear the Rabbit say to itself, 'Oh dear! Oh dear! I shall be late!'",
            ]
        }
    )

    # assert success for all models
    for model in _models.keys():
        df = df.with_column("embedding", embed_text(df["text"], model=model))
        df.collect()
        time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
@pytest.mark.parametrize("use_chat_completions", [False, True])
def test_prompt_plain_text(session, use_chat_completions):
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
            use_chat_completions=use_chat_completions,
        ),
    )

    answers = df.to_pydict()["answer"]

    # Basic sanity checks - responses should be non-empty strings
    assert len(answers) == 3
    for answer in answers:
        assert isinstance(answer, str)
        assert len(answer) > 0

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
@pytest.mark.parametrize("use_chat_completions", [False, True])
def test_prompt_structured_output(session, use_chat_completions):
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
            use_chat_completions=use_chat_completions,
        ),
    )

    reviews = df.to_pydict()["review"]

    # Verify structured output
    assert len(reviews) == 3
    for review in reviews:
        assert 1 <= review["rating"] <= 5
        assert isinstance(review["summary"], str)
        assert len(review["summary"]) > 0

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
@pytest.mark.parametrize("use_chat_completions", [False, True])
def test_prompt_with_image(session, use_chat_completions):
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
            use_chat_completions=use_chat_completions,
        ),
    )

    answers = df.to_pydict()["answer"]

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
    import tempfile

    import numpy as np
    from PIL import Image

    # Create a temporary image file (green square)
    green_square = np.zeros((100, 100, 3), dtype=np.uint8)
    green_square[:, :, 1] = 255  # Green channel

    # Save to a temporary file
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        img = Image.fromarray(green_square)
        img.save(tmp.name)
        temp_path = tmp.name

    df = daft.from_pydict(
        {
            "question": [
                "Describe this image briefly.",
            ],
            "image_url": [
                temp_path,
            ],
        }
    )

    # Download the image to get bytes
    df = df.with_column("image_bytes", daft.col("image_url").url.download())

    df = df.with_column(
        "answer",
        prompt(
            [daft.col("question"), daft.col("image_bytes")],
        ),
    )

    answers = df.to_pydict()["answer"]

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
            ),
        )

        answers = df.to_pydict()["answer"]

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
@pytest.mark.parametrize("use_chat_completions", [False, True])
def test_prompt_with_image_structured_output(session, use_chat_completions):
    """Test prompt function with image input and structured output."""
    import numpy as np

    class ImageAnalysis(BaseModel):
        dominant_color: str = Field(..., description="The dominant color in the image in hex format")
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
            use_chat_completions=use_chat_completions,
        ),
    )

    analyses = df.to_pydict()["analysis"]

    # Verify structured output
    assert len(analyses) == 1
    for analysis in analyses:
        assert isinstance(analysis["dominant_color"], str)
        assert isinstance(analysis["description"], str)
        assert len(analysis["dominant_color"]) > 0
        assert len(analysis["description"]) > 0
        # Check if "blue" appears in the dominant color (case-insensitive)
        assert "#0000ff" in analysis["dominant_color"].lower()

    time.sleep(1)  # self limit to ~1 tps.


@pytest.mark.integration()
@pytest.mark.parametrize("use_chat_completions", [False, True])
def test_prompt_with_pdf_document(session, use_chat_completions):
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
                use_chat_completions=use_chat_completions,
            ),
        )

        answers = df.to_pydict()["answer"]

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
            ),
        )

        answers = df.to_pydict()["answer"]

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
