from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("openai")

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
    descriptor = provider.get_text_embedder_descriptor()

    assert descriptor["provider"] == "openai"
    assert descriptor["model"] == "text-embedding-3-small"
    assert descriptor["dimensions"].size == 1536


def test_openai_text_embedder_overridden_dimensions():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder_descriptor(dimensions=256)

    assert descriptor["provider"] == "openai"
    assert descriptor["model"] == "text-embedding-3-small"
    assert descriptor["dimensions"].size == 256


def test_openai_text_embedder_other():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder_descriptor(model="text-embedding-3-large", api_key="test-key")

    assert descriptor["provider"] == "openai"
    assert descriptor["model"] == "text-embedding-3-large"
    assert descriptor["dimensions"].size == 3072


def test_openai_text_embedder_instantiation():
    provider = OpenAIProvider(api_key="test_key")
    descriptor = provider.get_text_embedder_descriptor(model="text-embedding-ada-002")

    embedder = provider.get_text_embedder(descriptor)
    assert embedder._model == "text-embedding-ada-002"
    assert hasattr(embedder, "_client")


def test_openai_text_embedder_dimensions():
    provider = OpenAIProvider()

    descriptor_ada = provider.get_text_embedder_descriptor(model="text-embedding-ada-002")
    assert descriptor_ada["dimensions"].size == 1536

    descriptor_small = provider.get_text_embedder_descriptor(model="text-embedding-3-small")
    assert descriptor_small["dimensions"].size == 1536

    descriptor_large = provider.get_text_embedder_descriptor(model="text-embedding-3-large")
    assert descriptor_large["dimensions"].size == 3072


def test_openai_text_embedder_descriptor_overridden_dimensions():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder_descriptor(model="text-embedding-3-large", dimensions=256)

    assert descriptor["dimensions"].size == 256


def test_openai_get_udf_options():
    """Test that OpenAI provider sets max_retries=0 (client handles retries)."""
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder_descriptor()

    udf_options = provider._get_udf_options(descriptor)
    assert udf_options.max_retries == 0


def test_openai_get_udf_options_with_custom_options():
    """Test that embed_options are passed through to UDF options."""
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder_descriptor(batch_size=32, on_error="ignore")

    udf_options = provider._get_udf_options(descriptor)
    assert udf_options.max_retries == 0
    assert udf_options.batch_size == 32
    assert udf_options.on_error == "ignore"


def test_openai_embed_text_returns_expression():
    """Test that embed_text() returns a Daft Expression."""
    import daft

    provider = OpenAIProvider()
    expr = provider.embed_text(daft.col("text"))
    assert isinstance(expr, daft.Expression)


def test_openai_provider_raises_import_error_without_numpy():
    with patch("daft.dependencies.np.module_available", return_value=False):
        with pytest.raises(ImportError, match=r"Please `pip install 'daft\[openai\]'` with this provider"):
            OpenAIProvider(api_key="test-key")


def test_openai_provider_raises_import_error_without_openai():
    # We need to mock openai module if it is installed
    with patch.dict("sys.modules", {"openai": None}):
        with pytest.raises(ImportError, match=r"Please `pip install 'daft\[openai\]'` with this provider"):
            OpenAIProvider(api_key="test-key")
