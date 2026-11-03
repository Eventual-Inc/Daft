from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from daft.ai.parallel.protocols.text_classifier import ParallelTextClassifier
from daft.ai.parallel.provider import ParallelProvider


def _classifier(response_payload):
    response = MagicMock()
    response.json.return_value = response_payload
    session = MagicMock()
    session.post.return_value = response
    with patch("requests.Session", return_value=session):
        classifier = ParallelTextClassifier("test-key", "https://api.parallel.ai", 12.0, "fast")
    return classifier, session, response


def test_provider_defaults():
    provider = ParallelProvider(api_key="test-key")
    descriptor = provider.get_text_classifier()

    assert provider.name == "parallel"
    assert descriptor.get_provider() == "parallel"
    assert descriptor.get_model() == "fast"
    assert descriptor.get_options() == {
        "batch_size": 25,
        "concurrency": 16,
        "max_retries": 3,
        "on_error": "raise",
    }


def test_provider_reads_api_key_from_environment():
    with patch.dict("os.environ", {"PARALLEL_API_KEY": "env-key"}):
        descriptor = ParallelProvider().get_text_classifier()
    assert descriptor.api_key == "env-key"


def test_provider_validates_mode_and_batch_size():
    provider = ParallelProvider(api_key="test-key")
    with pytest.raises(ValueError, match="Unsupported Parallel Decisions mode"):
        provider.get_text_classifier("other")
    with pytest.raises(ValueError, match="batch_size must be between 1 and 100"):
        provider.get_text_classifier(batch_size=101)


def test_classify_text_batches_questions_and_preserves_nulls():
    classifier, session, response = _classifier(
        {
            "answers": {
                "row_0": {"type": "choice", "choice": "billing", "probabilities": {}},
                "row_2": {"type": "choice", "choice": "security", "probabilities": {}},
            }
        }
    )

    result = classifier.classify_text(
        ["I was charged twice", None, "Someone accessed my account"],
        ["billing", "security"],
    )

    assert result == ["billing", None, "security"]
    session.post.assert_called_once_with(
        "https://api.parallel.ai/v1/decisions",
        json={
            "mode": "fast",
            "state": {"texts": ["I was charged twice", "Someone accessed my account"]},
            "questions": {
                "row_0": {
                    "type": "choice",
                    "instructions": "Which label best classifies texts[0]?",
                    "criteria": {
                        "billing": "Use when 'billing' best describes the text.",
                        "security": "Use when 'security' best describes the text.",
                    },
                },
                "row_2": {
                    "type": "choice",
                    "instructions": "Which label best classifies texts[1]?",
                    "criteria": {
                        "billing": "Use when 'billing' best describes the text.",
                        "security": "Use when 'security' best describes the text.",
                    },
                },
            },
        },
        timeout=12.0,
    )
    response.raise_for_status.assert_called_once_with()


def test_classify_text_validates_labels():
    classifier, _, _ = _classifier({"answers": {}})
    with pytest.raises(ValueError, match="between 2 and 255 unique labels"):
        classifier.classify_text(["text"], ["same", "same"])


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "no answers object"),
        ({"answers": {}}, "no answer for row_0"),
        ({"answers": {"row_0": {"type": "refusal"}}}, "refused to classify row_0"),
        (
            {"answers": {"row_0": {"type": "choice", "choice": "other"}}},
            "returned an invalid choice for row_0",
        ),
    ],
)
def test_classify_text_rejects_invalid_responses(payload, message):
    classifier, _, _ = _classifier(payload)
    with pytest.raises(RuntimeError, match=message):
        classifier.classify_text(["text"], ["billing", "security"])
