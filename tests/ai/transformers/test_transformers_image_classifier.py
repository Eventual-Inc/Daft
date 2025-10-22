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
    def mock_classify_image(images, labels):
        return ["mock!"] * len(images)

    mock_image_classifier.classify_image = mock_classify_image
    yield provider


def test_mock_image_classifier(mock_pipeline, mock_image_classifier):
    """Sanity check that our mocks are properly plumbed."""
    assert mock_image_classifier._model == "mock"
    assert mock_image_classifier._pipeline is mock_pipeline


def test_image_classifier(mock_image_classifier):
    """Test image classification using the model."""
    # Test the classify_image method directly
    images = [
        Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8)),
        Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8)),
    ]
    labels = ["dog", "cat"]

    # Mock the pipeline's return value
    mock_image_classifier._pipeline.return_value = ["dog", "cat"]

    results = mock_image_classifier.classify_image(images, labels)
    assert results == ["dog", "cat"]


def test_classify_image_with_mock(mock_provider):
    """Test image classification using the expression."""
    # Test that the provider returns the expected descriptor
    descriptor = mock_provider.get_image_classifier()
    assert descriptor is not None

    # Test that the descriptor can be instantiated
    classifier = descriptor.instantiate()
    assert classifier is not None

    # Test that the classifier returns the expected results
    results = classifier.classify_image(
        [
            Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8)),
        ],
        ["cat", "dog"],
    )
    assert results == ["mock!"]


@pytest.mark.integration()
@pytest.mark.skipif(pytest.importorskip("torch").__version__ < "2.6.0", reason="requires torch>=2.6.0")
def test_instantiate():
    """Test to instantiate a TransformersImageClassifier, this instantiates a pipeline."""
    descriptor = TransformersImageClassifierDescriptor(
        provider_name="transformers",
        model_name="openai/clip-vit-base-patch32",
        model_options={},
    )

    classifier = descriptor.instantiate()
    assert isinstance(classifier, TransformersImageClassifier)
    assert classifier._model == "openai/clip-vit-base-patch32"
    assert classifier._pipeline, "Current implementation should have a pipeline."


@pytest.mark.integration()
@pytest.mark.skipif(pytest.importorskip("torch").__version__ < "2.6.0", reason="requires torch>=2.6.0")
def test_classify_image_with_default():
    """Test image classification using the expression."""
    import daft
    from daft.functions import classify_image

    # Create a red image (255, 0, 0)
    red_image = Image.fromarray(np.full((100, 100, 3), [255, 0, 0], dtype=np.uint8))

    # Create a blue image (0, 0, 255)
    blue_image = Image.fromarray(np.full((100, 100, 3), [0, 0, 255], dtype=np.uint8))

    df = daft.from_pydict({"image": [red_image, blue_image]})

    df = df.with_column("label", classify_image(df["image"], labels=["red", "blue"]))

    assert df.select("label").to_pylist() == [
        {"label": "red"},
        {"label": "blue"},
    ]
