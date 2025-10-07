from __future__ import annotations

import pytest

pytest.importorskip("transformers")

from unittest.mock import MagicMock, Mock, patch

from daft.ai.provider import Provider
from daft.ai.transformers.protocols.text_classifier import (
    TransformersTextClassifier,
    TransformersTextClassifierDescriptor,
)


class MockProvider(Provider):
    @property
    def name(self) -> str:
        return "mock"


@pytest.fixture
def mock_pipeline():
    yield Mock()


@pytest.fixture
def mock_text_classifier(mock_pipeline):
    with patch("daft.ai.transformers.protocols.text_classifier.pipeline", return_value=mock_pipeline):
        yield TransformersTextClassifier("mock")


@pytest.fixture
def mock_provider(mock_text_classifier):
    provider = MockProvider()
    # Create a mock descriptor that returns our mock classifier
    mock_descriptor = MagicMock()
    mock_descriptor.instantiate.return_value = mock_text_classifier
    provider.get_text_classifier = MagicMock(return_value=mock_descriptor)

    # Mock the classify_text method to return expected results
    def mock_classify_text(texts, labels):
        return ["mock!"] * len(texts)

    mock_text_classifier.classify_text = mock_classify_text
    yield provider


def test_mock_text_classifier(mock_pipeline, mock_text_classifier):
    """Sanity check that our mocks are properly plumbed."""
    assert mock_text_classifier._model == "mock"
    assert mock_text_classifier._pipeline is mock_pipeline


def test_text_classifier(mock_text_classifier):
    """Test text classification using the model."""
    # Test the classify_text method directly
    texts = ["This is a statement.", "Is this a question?"]
    labels = ["statement", "question"]

    # Mock the pipeline's return value
    mock_text_classifier._pipeline.return_value = [
        {"labels": ["statement", "question"], "scores": [0.8, 0.2]},
        {"labels": ["question", "statement"], "scores": [0.9, 0.1]},
    ]

    results = mock_text_classifier.classify_text(texts, labels)
    assert results == ["statement", "question"]


def test_classify_text_with_mock(mock_provider):
    """Test text classification using the expression."""
    # Test that the provider returns the expected descriptor
    descriptor = mock_provider.get_text_classifier()
    assert descriptor is not None

    # Test that the descriptor can be instantiated
    classifier = descriptor.instantiate()
    assert classifier is not None

    # Test that the classifier returns the expected results
    results = classifier.classify_text(["test"], ["statement", "question"])
    assert results == ["mock!"]


@pytest.mark.integration()
def test_instantiate():
    """Test to instantiate a TransformersTextClassifier, this instantiates a pipeline."""
    descriptor = TransformersTextClassifierDescriptor(
        provider_name="transformers",
        model_name="facebook/bart-large-mnli",
        model_options={},
    )

    classifier = descriptor.instantiate()
    assert isinstance(classifier, TransformersTextClassifier)
    assert classifier._model == "facebook/bart-large-mnli"
    assert classifier._pipeline, "Current implementation should have a pipeline."


@pytest.mark.integration()
def test_classify_text_with_default():
    """Test text classification using the expression."""
    import daft
    from daft.functions import classify_text

    df = daft.from_pydict(
        {"comment": ["These snozzberries taste like snozzberries.", "Snozzberries? Who ever heard of a snozzberry?"]}
    )

    df = df.with_column("label", classify_text(df["comment"], labels=["statement", "question"]))

    assert df.select("label").to_pylist() == [
        {"label": "statement"},
        {"label": "question"},
    ]
