from __future__ import annotations

import pytest

pytest.importorskip("transformers")
from unittest.mock import MagicMock, Mock, patch

import numpy as np
from PIL import Image

from daft.ai.provider import Provider
from daft.ai.transformers.protocols.image_classifier import (
    TransformersImageClassifier,
    TransformersImageClassifierDescriptor,
)


class MockProvider(Provider):
    @property
    def name(self) -> str:
        return "mock"


@pytest.fixture
def mock_pipeline():
    yield Mock()


@pytest.fixture
def mock_image_classifier(mock_pipeline):
    with patch("daft.ai.transformers.protocols.image_classifier.pipeline", return_value=mock_pipeline):
        yield TransformersImageClassifier("mock")


@pytest.fixture
def mock_provider(mock_image_classifier):
    provider = MockProvider()
    # Create a mock descriptor that returns our mock classifier
    mock_descriptor = MagicMock()
    mock_descriptor.instantiate.return_value = mock_image_classifier
    provider.get_image_classifier = MagicMock(return_value=mock_descriptor)

    # Mock the classify_image method to return expected results
    def mock_classify_image(texts, labels):
        return ["mock!"] * len(texts)

    mock_image_classifier.classify_image = mock_classify_image
    yield provider


def test_mock_image_classifier(mock_pipeline, mock_image_classifier):
    """Sanity check that our mocks are properly plumbed."""
    assert mock_image_classifier._model == "mock"
    assert mock_image_classifier._pipeline is mock_pipeline


def test_image_classifier(mock_image_classifier):
    """Test text classification using the model."""
    # Test the classify_image method directly
    images = [
        Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8)),
        Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8)),
    ]
    labels = ["dog", "cat"]

    # Mock the pipeline's return value
    mock_image_classifier._pipeline.return_value = [
        [{"label": "dog", "score": 0.8}, {"label": "cat", "score": 0.2}],
        [{"label": "cat", "score": 0.9}, {"label": "dog", "score": 0.1}],
    ]

    results = mock_image_classifier.classify_image(images, labels)
    assert results == ["dog", "cat"]


def test_classify_image_with_mock(mock_provider):
    """Test text classification using the expression."""
    # Test that the provider returns the expected descriptor
    descriptor = mock_provider.get_image_classifier()
    assert descriptor is not None

    # Test that the descriptor can be instantiated
    classifier = descriptor.instantiate()
    assert classifier is not None

    # Test that the classifier returns the expected results
    results = classifier.classify_image(["test"], ["statement", "question"])
    assert results == ["mock!"]


@pytest.mark.integration()
def test_instantiate():
    """Test to instantiate a TransformersImageClassifier, this instantiates a pipeline."""
    descriptor = TransformersImageClassifierDescriptor(
        provider_name="transformers",
        model_name="facebook/bart-large-mnli",
        model_options={},
    )

    classifier = descriptor.instantiate()
    assert isinstance(classifier, TransformersImageClassifier)
    assert classifier._model == "facebook/bart-large-mnli"
    assert classifier._pipeline, "Current implementation should have a pipeline."


@pytest.mark.integration()
def test_classify_image_with_default():
    """Test text classification using the expression."""
    import daft
    from daft.functions import classify_image

    df = daft.from_pydict(
        {"comment": ["These snozzberries taste like snozzberries.", "Snozzberries? Who ever heard of a snozzberry?"]}
    )

    df = df.with_column("label", classify_image(df["comment"], labels=["statement", "question"]))

    assert df.select("label").to_pylist() == [
        {"label": "statement"},
        {"label": "question"},
    ]
