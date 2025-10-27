from __future__ import annotations

from unittest.mock import Mock

import pytest

pytest.importorskip("openai")


def test_normalize_model_name_strips_prefix_for_default_base_url():
    """normalize_model_name should strip 'openai/' when base_url is default OpenAI endpoint."""
    from daft.ai.openai.typing import DEFAULT_OPENAI_BASE_URL
    from daft.ai.openai.utils import normalize_model_name

    assert normalize_model_name("openai/gpt-5-nano") == "gpt-5-nano"
    assert normalize_model_name("openai/gpt-5-nano", DEFAULT_OPENAI_BASE_URL) == "gpt-5-nano"


def test_normalize_model_name_preserves_prefix_for_custom_base_url():
    """normalize_model_name should not strip prefix when using a custom server."""
    from daft.ai.openai.utils import normalize_model_name

    assert normalize_model_name("openai/gpt-5-nano", "https://openrouter.ai/api/v1") == "openai/gpt-5-nano"
    assert normalize_model_name("openai/gpt-5-nano", "http://localhost:8000/v1") == "openai/gpt-5-nano"


def test_validate_model_availability_returns_true_when_model_listed():
    """validate_model_availability returns True when the model id is present."""
    from daft.ai.openai.utils import validate_model_availability

    mock_client = Mock()
    mock_client.base_url = "https://api.openai.com/v1"

    present = Mock()
    present.id = "gpt-4o"
    other = Mock()
    other.id = "gpt-4o-mini"
    mock_client.models.list.return_value = [present, other]

    assert validate_model_availability(mock_client, "gpt-4o") is True
    mock_client.models.list.assert_called_once()


def test_validate_model_availability_returns_false_when_model_missing():
    """validate_model_availability returns False when the model id is absent."""
    from daft.ai.openai.utils import validate_model_availability

    mock_client = Mock()
    mock_client.base_url = "https://api.openai.com/v1"

    available = Mock()
    available.id = "gpt-4o"
    mock_client.models.list.return_value = [available]

    assert validate_model_availability(mock_client, "gpt-4.1") is False
    mock_client.models.list.assert_called_once()


def test_validate_model_availability_raises_value_error_on_client_failure():
    """validate_model_availability wraps client errors in a ValueError with context."""
    from daft.ai.openai.utils import validate_model_availability

    mock_client = Mock()
    mock_client.base_url = "https://api.openai.com/v1"
    mock_client.models.list.side_effect = RuntimeError("boom")

    with pytest.raises(ValueError, match="Error listing models for OpenAI client"):
        validate_model_availability(mock_client, "gpt-4o")

    mock_client.models.list.assert_called_once()
