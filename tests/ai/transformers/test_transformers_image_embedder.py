from __future__ import annotations

import pytest

pytest.importorskip("transformers")
pytest.importorskip("torch")
pytest.importorskip("PIL")


import time

import numpy as np

from daft.ai.protocols import ImageEmbedderDescriptor
from daft.ai.transformers.provider import TransformersProvider
from daft.ai.typing import EmbeddingDimensions
from daft.datatype import DataType
from tests.benchmarks.conftest import IS_CI


def test_transformers_image_embedder_default():
    provider = TransformersProvider()
    descriptor = provider.get_image_embedder()
    assert isinstance(descriptor, ImageEmbedderDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "openai/clip-vit-base-patch32"
    assert descriptor.get_dimensions() == EmbeddingDimensions(512, dtype=DataType.float32())


@pytest.mark.parametrize(
    "model_name, dimensions, run_model_in_ci",
    [
        ("openai/clip-vit-base-patch32", 512, True),
        ("openai/clip-vit-large-patch14", 768, False),
        ("openai/clip-vit-base-patch16", 512, True),
        ("openai/clip-vit-large-patch14-336", 768, False),
    ],
)
def test_transformers_image_embedder_other(model_name, dimensions, run_model_in_ci):
    mock_options = {"arg1": "val1", "arg2": "val2"}

    provider = TransformersProvider()
    descriptor = provider.get_image_embedder(model_name, **mock_options)
    assert isinstance(descriptor, ImageEmbedderDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == model_name
    assert descriptor.get_options() == mock_options

    if not IS_CI or run_model_in_ci:
        retry_delay = 5  # seconds
        max_retries = 5

        # retry instantiation because Hugging Face is flaky
        for attempt in range(max_retries):
            try:
                embedder = descriptor.instantiate()
            except OSError as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise e
            else:
                break

        # Test with a variety of image sizes and shapes that should be preprocessed correctly.
        # TODO(desmond): This doesn't work with greyscale images. I wonder if we should require users to cast
        # to RGB or if we want to handle this automagically.
        test_image1 = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
        test_image2 = np.random.randint(0, 255, (500, 10, 3), dtype=np.uint8)
        # test_image3 = np.random.randint(0, 255, (100, 100, 1), dtype=np.uint8)
        test_images = [test_image1, test_image2] * 8

        embeddings = embedder.embed_image(test_images)
        assert len(embeddings) == 16
        assert len(embeddings[0]) == dimensions
    else:
        assert descriptor.get_dimensions() == EmbeddingDimensions(dimensions, dtype=DataType.float32())
