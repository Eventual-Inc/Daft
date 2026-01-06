from __future__ import annotations

from unittest.mock import patch

import pytest

from daft.ai.provider import ProviderImportError
from daft.ai.transformers.provider import TransformersProvider


def test_transformers_provider_init_raises_without_torch():
    with (
        patch("daft.dependencies.transformers.module_available", return_value=True),
        patch("daft.dependencies.np.module_available", return_value=True),
        patch("daft.dependencies.torch.module_available", return_value=False),
    ):
        with pytest.raises(ImportError, match=r"Please `pip install 'daft\[transformers\]'` with this provider"):
            TransformersProvider()


def test_transformers_provider_init_raises_without_numpy():
    with (
        patch("daft.dependencies.transformers.module_available", return_value=True),
        patch("daft.dependencies.torch.module_available", return_value=True),
        patch("daft.dependencies.np.module_available", return_value=False),
    ):
        with pytest.raises(ImportError, match=r"Please `pip install 'daft\[transformers\]'` with this provider"):
            TransformersProvider()


def test_transformers_provider_init_raises_without_transformers():
    """Provider should raise a clean ProviderImportError if transformers isn't installed."""
    with (
        patch("daft.dependencies.torch.module_available", return_value=True),
        patch("daft.dependencies.np.module_available", return_value=True),
        patch("daft.dependencies.transformers.module_available", return_value=False),
    ):
        with pytest.raises(ProviderImportError, match=r"pip install 'daft\[transformers\]'"):
            TransformersProvider()


def test_transformers_provider_get_image_embedder_raises_without_torchvision():
    # We need torch available for init
    with (
        patch("daft.dependencies.torch.module_available", return_value=True),
        patch("daft.dependencies.np.module_available", return_value=True),
        patch("daft.dependencies.transformers.module_available", return_value=True),
    ):
        provider = TransformersProvider()
        with patch("daft.dependencies.torchvision.module_available", return_value=False):
            with pytest.raises(
                ImportError,
                match=r"Please `pip install 'daft\[transformers\]'` to use the embed_image function with this provider",
            ):
                provider.get_image_embedder()


def test_transformers_provider_get_image_embedder_raises_without_pillow():
    with (
        patch("daft.dependencies.torch.module_available", return_value=True),
        patch("daft.dependencies.np.module_available", return_value=True),
        patch("daft.dependencies.transformers.module_available", return_value=True),
    ):
        provider = TransformersProvider()
        # torchvision available, but pillow not
        with patch("daft.dependencies.torchvision.module_available", return_value=True):
            with patch("daft.dependencies.pil_image.module_available", return_value=False):
                with pytest.raises(
                    ImportError,
                    match=r"Please `pip install 'daft\[transformers\]'` to use the embed_image function with this provider",
                ):
                    provider.get_image_embedder()


def test_transformers_provider_get_image_classifier_raises_without_torchvision():
    with (
        patch("daft.dependencies.torch.module_available", return_value=True),
        patch("daft.dependencies.np.module_available", return_value=True),
        patch("daft.dependencies.transformers.module_available", return_value=True),
    ):
        provider = TransformersProvider()
        with patch("daft.dependencies.torchvision.module_available", return_value=False):
            with pytest.raises(
                ImportError,
                match=r"Please `pip install 'daft\[transformers\]'` to use the classify_image function with this provider",
            ):
                provider.get_image_classifier()


def test_transformers_provider_get_image_classifier_raises_without_pillow():
    with (
        patch("daft.dependencies.torch.module_available", return_value=True),
        patch("daft.dependencies.np.module_available", return_value=True),
        patch("daft.dependencies.transformers.module_available", return_value=True),
    ):
        provider = TransformersProvider()
        with patch("daft.dependencies.torchvision.module_available", return_value=True):
            with patch("daft.dependencies.pil_image.module_available", return_value=False):
                with pytest.raises(
                    ImportError,
                    match=r"Please `pip install 'daft\[transformers\]'` to use the classify_image function with this provider",
                ):
                    provider.get_image_classifier()
