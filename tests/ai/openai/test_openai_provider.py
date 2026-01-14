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
    expr = provider.create_text_embedder()
    descriptor = expr._daft_setup_args[0][0]

    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-3-small"
    assert descriptor.get_dimensions().size == 1536

    # Factory returns a Daft UDF object, not a descriptor.
    # Calling it with an Expression returns an Expression with the correct return dtype.
    import daft
    from daft.expressions import Expression

    call_func = expr.__getattr__("__call__")
    assert call_func.return_dtype == descriptor.get_dimensions().as_dtype()
    assert isinstance(expr(daft.col("text")), Expression)


def test_openai_text_embedder_overridden_dimensions():
    provider = OpenAIProvider()
    expr = provider.create_text_embedder(dimensions=256)
    descriptor = expr._daft_setup_args[0][0]

    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-3-small"
    assert descriptor.get_dimensions().size == 256

    import daft
    from daft.expressions import Expression

    call_func = expr.__getattr__("__call__")
    assert call_func.return_dtype == descriptor.get_dimensions().as_dtype()
    assert isinstance(expr(daft.col("text")), Expression)


def test_openai_text_embedder_other():
    provider = OpenAIProvider()
    expr = provider.create_text_embedder(model="text-embedding-3-large", api_key="test-key")
    descriptor = expr._daft_setup_args[0][0]

    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-3-large"
    assert descriptor.get_dimensions().size == 3072

    import daft
    from daft.expressions import Expression

    call_func = expr.__getattr__("__call__")
    assert call_func.return_dtype == descriptor.get_dimensions().as_dtype()
    assert isinstance(expr(daft.col("text")), Expression)


def test_openai_text_embedder_instantiation():
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-ada-002",
        dimensions=None,
        embed_options={},
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
        embed_options={},
    )
    assert descriptor_ada.get_dimensions().size == 1536

    descriptor_small = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-3-small",
        dimensions=None,
        embed_options={},
    )
    assert descriptor_small.get_dimensions().size == 1536

    descriptor_large = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-3-large",
        dimensions=None,
        embed_options={},
    )
    assert descriptor_large.get_dimensions().size == 3072


def test_openai_text_embedder_descriptor_overridden_dimensions():
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test_key"},
        model_name="text-embedding-3-large",
        dimensions=256,
        embed_options={},
    )

    assert descriptor.get_dimensions().size == 256


def test_openai_provider_raises_import_error_without_numpy():
    with patch("daft.dependencies.np.module_available", return_value=False):
        with pytest.raises(ImportError, match=r"Please `pip install 'daft\[openai\]'` with this provider"):
            OpenAIProvider(api_key="test-key")


def test_openai_provider_raises_import_error_without_openai():
    # We need to mock openai module if it is installed
    with patch.dict("sys.modules", {"openai": None}):
        with pytest.raises(ImportError, match=r"Please `pip install 'daft\[openai\]'` with this provider"):
            OpenAIProvider(api_key="test-key")
