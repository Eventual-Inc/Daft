from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("openai")

from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedderDescriptor
from daft.ai.openai.provider import OpenAIProvider


def test_openai_provider_upsert():
    import daft

    # test upsert via session object
    sess = daft.Session()
    sess.set_provider("openai", api_key="fake_key")
    assert isinstance(sess.current_provider(), OpenAIProvider)

    # test upsert via active session in context
    with daft.session():
        daft.set_provider("openai", api_key="fake_key")
        assert isinstance(daft.current_provider(), OpenAIProvider)


def test_openai_text_embedder_default():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder()

    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-3-small"
    assert descriptor.get_dimensions().size == 1536


def test_openai_text_embedder_overridden_dimensions():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder(dimensions=256)

    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-3-small"
    assert descriptor.get_dimensions().size == 256


def test_openai_text_embedder_other():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder(model="text-embedding-3-large", api_key="test-key")

    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-3-large"
    assert descriptor.get_dimensions().size == 3072


def test_openai_text_embedder_instantiation():
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-ada-002",
        dimensions=None,
        model_options={},
    )

    embedder = descriptor.instantiate()
    assert embedder._model == "text-embedding-ada-002"
    assert hasattr(embedder, "_client")


def test_openai_text_embedder_dimensions():
    descriptor_ada = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-ada-002",
        dimensions=None,
        model_options={},
    )
    assert descriptor_ada.get_dimensions().size == 1536

    descriptor_small = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-3-small",
        dimensions=None,
        model_options={},
    )
    assert descriptor_small.get_dimensions().size == 1536

    descriptor_large = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-3-large",
        dimensions=None,
        model_options={},
    )
    assert descriptor_large.get_dimensions().size == 3072


def test_openai_text_embedder_descriptor_overridden_dimensions():
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-3-large",
        dimensions=256,
        model_options={},
    )

    assert descriptor.get_dimensions().size == 256


def test_openai_provider_raises_import_error_without_numpy():
    with patch("daft.dependencies.np.module_available", return_value=False):
        with pytest.raises(ImportError, match="numpy is required for the OpenAIProvider"):
            OpenAIProvider(api_key="test-key")


def test_openai_provider_raises_import_error_without_openai():
    with patch("daft.dependencies.openai.module_available", return_value=False):
        with pytest.raises(ImportError, match="openai is required for the OpenAIProvider"):
            OpenAIProvider(api_key="test-key")
