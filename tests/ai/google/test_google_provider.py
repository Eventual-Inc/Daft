from __future__ import annotations

from unittest.mock import patch

import pytest

from daft.ai.google.protocols.prompter import GooglePrompterDescriptor
from daft.ai.google.provider import GoogleProvider


def test_google_provider_get_prompter_default():
    """Test that the provider returns a prompter descriptor with default settings."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_prompter()

    assert isinstance(descriptor, GooglePrompterDescriptor)
    assert descriptor.get_provider() == "google"
    assert descriptor.get_model() == "gemini-2.5-flash"
    assert descriptor.get_options() == {}
    assert descriptor.prompt_options.get("return_format") is None


def test_google_provider_get_prompter_with_model():
    """Test that the provider accepts custom model names."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_prompter(model="gemini-1.5-pro")

    assert isinstance(descriptor, GooglePrompterDescriptor)
    assert descriptor.get_model() == "gemini-1.5-pro"


def test_google_provider_get_prompter_with_options():
    """Test that the provider accepts generation config options."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_prompter(
        model="gemini-2.5-flash",
        temperature=0.7,
        max_output_tokens=100,
    )

    assert descriptor.get_options() == {"temperature": 0.7, "max_output_tokens": 100}


def test_google_provider_raises_import_error_without_numpy():
    with patch("daft.dependencies.np.module_available", return_value=False):
        with pytest.raises(ImportError, match=r"Please `pip install 'daft\[google\]'` with this provider"):
            GoogleProvider(api_key="test-key")
