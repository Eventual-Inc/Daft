from __future__ import annotations

import pytest

pytest.importorskip("transformers")
pytest.importorskip("torch")


import torch

from daft.ai.transformers.provider import TransformersProvider


def test_transformers_device_selection():
    """Test that the embedder uses the correct device selection."""
    provider = TransformersProvider()
    descriptor = provider.get_image_embedder("openai/clip-vit-base-patch32")
    embedder = descriptor.instantiate()

    if torch.cuda.is_available():
        expected_device = "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        expected_device = "mps"
    else:
        expected_device = "cpu"

    embedder_device = next(embedder.model.parameters()).device
    assert expected_device in str(embedder_device)
