"""Pytest fixtures that register the mock AI provider for doc testing."""

import os

import pytest


@pytest.fixture(autouse=True, scope="session")
def setup_mock_providers():
    import daft
    from docs.audit.mock_provider import MockProvider

    os.environ.setdefault("OPENAI_API_KEY", "mock-key-for-testing")
    os.environ.setdefault("OPENROUTER_API_KEY", "mock-key-for-testing")

    mock = MockProvider()
    daft.attach_provider(mock, alias="mock")
    daft.attach_provider(mock, alias="openai")
    daft.set_provider("mock")

    yield
