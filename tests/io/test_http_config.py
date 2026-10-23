from __future__ import annotations

import pickle

from daft.io import HTTPConfig


def test_http_config_documented_properties():
    config = HTTPConfig(
        bearer_token="test-token", retry_initial_backoff_ms=10, connect_timeout_ms=20, read_timeout_ms=30, num_tries=2
    )
    for actual in (config, pickle.loads(pickle.dumps(config))):
        assert actual.bearer_token == "test-token"
        assert actual.retry_initial_backoff_ms == 10
        assert actual.connect_timeout_ms == 20
        assert actual.read_timeout_ms == 30
        assert actual.num_tries == 2
        assert "test-token" not in repr(actual)


def test_http_config_default_properties():
    config = HTTPConfig()
    assert config.bearer_token is None
    assert config.retry_initial_backoff_ms == 1000
    assert config.connect_timeout_ms == 30000
    assert config.read_timeout_ms == 30000
    assert config.num_tries == 5
