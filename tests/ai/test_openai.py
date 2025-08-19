from __future__ import annotations

import pytest

import daft

# Skip if openai package is not available
pytest.importorskip("openai")

from daft.ai.openai import OpenAIProvider
from daft.ai.openai.text_embedder import OpenAITextEmbedderDescriptor
from daft.ai.provider import load_openai


def test_openai_provider():


    # sets to openai with defaults
    daft.set_provider("openai")

    # sets to openai with options
    daft.set_provider("openai", **{
        "project": "my_project",
    })

    provider = load_openai(
        name="my_custom_openai",
        project="foo",
    )

    daft.attach_provider(provider)
    daft.set_provider("my_custom_openai")


    pass


def test_openai_text_embedder_default():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder()

    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-ada-002"
    assert descriptor.get_dimensions().size == 1536


def test_openai_text_embedder_other():
    provider = OpenAIProvider()
    descriptor = provider.get_text_embedder(
        model="text-embedding-3-large",
        api_key="test-key"
    )
    
    assert isinstance(descriptor, OpenAITextEmbedderDescriptor)
    assert descriptor.get_provider() == "openai"
    assert descriptor.get_model() == "text-embedding-3-large"
    assert descriptor.get_dimensions().size == 3072


def test_openai_text_embedder_instantiation():
    descriptor = OpenAITextEmbedderDescriptor(
        model="text-embedding-ada-002",
        options={}
    )
    
    embedder = descriptor.instantiate()
    assert embedder.model == "text-embedding-ada-002"
    assert hasattr(embedder, 'client')


def test_openai_text_embedder_dimensions():
    # Test ada-002 dimensions
    descriptor_ada = OpenAITextEmbedderDescriptor(
        model="text-embedding-ada-002",
        options={}
    )
    assert descriptor_ada.get_dimensions().size == 1536
    
    # Test 3-small dimensions
    descriptor_small = OpenAITextEmbedderDescriptor(
        model="text-embedding-3-small",
        options={}
    )
    assert descriptor_small.get_dimensions().size == 1536
    
    # Test 3-large dimensions
    descriptor_large = OpenAITextEmbedderDescriptor(
        model="text-embedding-3-large",
        options={}
    )
    assert descriptor_large.get_dimensions().size == 3072
    
    # Test unknown model defaults to ada-002 dimensions
    descriptor_unknown = OpenAITextEmbedderDescriptor(
        model="unknown-model",
        options={}
    )
    assert descriptor_unknown.get_dimensions().size == 1536 