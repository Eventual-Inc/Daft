from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from daft.ai.provider import (
    ProviderImportError,
    load_google,
    load_lm_studio,
    load_openai,
    load_provider,
    load_transformers,
    load_vllm_prefix_caching,
)


def test_provider_import_error_message():
<<<<<<< HEAD
    err = ProviderImportError("tag", function="function")
    assert "Please `pip install 'daft[tag]'` to use the function function with this provider." in str(err)
=======
    err = ProviderImportError("google")
    assert "pip install 'daft[google]'" in str(err)
    assert "with this provider" in str(err)
>>>>>>> 4738581ba77f12028979598b8a9786ba86e8bd21


def test_load_provider_invalid():
    with pytest.raises(ValueError, match="Provider 'invalid' is not yet supported"):
        load_provider("invalid")


@patch.dict("daft.ai.provider.PROVIDERS", {"openai": MagicMock()})
def test_load_provider_dispatch():
    from daft.ai.provider import PROVIDERS

    mock_load = PROVIDERS["openai"]
    load_provider("openai", name="my_openai", api_key="key")
    mock_load.assert_called_once_with("my_openai", api_key="key")


def test_load_google_import_error():
    # Simulate daft.ai.google.provider being missing/failing to import
    with patch.dict(sys.modules, {"daft.ai.google.provider": None}):
        with pytest.raises(ProviderImportError) as excinfo:
            load_google()
        assert "daft[google]" in str(excinfo.value)


def test_load_openai_import_error():
    with patch.dict(sys.modules, {"daft.ai.openai.provider": None}):
        with pytest.raises(ProviderImportError) as excinfo:
            load_openai()
        assert "daft[openai]" in str(excinfo.value)


def test_load_transformers_import_error():
    with patch.dict(sys.modules, {"daft.ai.transformers.provider": None}):
        with pytest.raises(ProviderImportError) as excinfo:
            load_transformers()
        assert "daft[transformers]" in str(excinfo.value)


def test_load_lm_studio_import_error():
    with patch.dict(sys.modules, {"daft.ai.lm_studio.provider": None}):
        with pytest.raises(ProviderImportError) as excinfo:
            load_lm_studio()
        assert "daft[openai]" in str(excinfo.value)  # LM Studio uses openai client


def test_load_vllm_prefix_caching_import_error():
    with patch.dict(sys.modules, {"daft.ai.vllm.provider": None}):
        with pytest.raises(ProviderImportError) as excinfo:
            load_vllm_prefix_caching()
        assert "daft[vllm]" in str(excinfo.value)
