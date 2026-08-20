from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("transformers")
pytest.importorskip("torch")

import torch
from torch import nn

from daft.ai.transformers.provider import TransformersProvider


class _DummyModel(nn.Module):
    """Minimal stand-in for a HuggingFace model.

    Used to avoid network access (and HF Hub rate limiting) in CI while still
    exposing a real `parameters()` iterable so that device-placement assertions
    remain meaningful.
    """

    def __init__(self) -> None:
        super().__init__()
        self.dummy = nn.Linear(2, 2)


def test_transformers_device_selection():
    """Test that the embedder uses the correct device selection."""
    # Patch the symbols imported into image_embedder module so that no HTTP
    # request is made to huggingface.co during model instantiation.
    with (
        patch("daft.ai.transformers.protocols.image_embedder.AutoModel") as mock_auto_model,
        patch("daft.ai.transformers.protocols.image_embedder.AutoProcessor") as mock_auto_proc,
    ):
        mock_auto_model.from_pretrained.return_value = _DummyModel()
        mock_auto_proc.from_pretrained.return_value = MagicMock()

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
