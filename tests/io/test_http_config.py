from __future__ import annotations

import functools
import os
import pickle
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import pytest

import daft
from daft.io import HTTPConfig, IOConfig


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


def test_http_config_user_agent_roundtrip():
    config = HTTPConfig(user_agent="my-app/1.0", bearer_token="test-token")
    for actual in (config, IOConfig(http=config).http, pickle.loads(pickle.dumps(config))):
        assert actual.user_agent == "my-app/1.0"
        assert actual.bearer_token == "test-token"
        assert "my-app/1.0" in repr(actual)
        assert "test-token" not in repr(actual)

    assert HTTPConfig().user_agent == "daft/0.0.1"
    assert HTTPConfig(None, 10, 20, 30, 2).num_tries == 2
    assert HTTPConfig(user_agent="").user_agent == ""


@pytest.mark.skipif(os.getenv("DAFT_RUNNER") == "ray", reason="local HTTP server is unavailable to Ray workers")
def test_http_config_user_agent_sent_to_server(tmp_path):
    (tmp_path / "data.csv").write_text("value\n42\n")
    user_agents = []

    class Handler(SimpleHTTPRequestHandler):
        def do_GET(self):
            user_agents.append(self.headers.get("User-Agent"))
            super().do_GET()

        def do_HEAD(self):
            user_agents.append(self.headers.get("User-Agent"))
            super().do_HEAD()

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(Handler, directory=str(tmp_path)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/data.csv"
        config = IOConfig(http=HTTPConfig(user_agent="my-app/1.0"))
        assert daft.read_csv(url, io_config=config).to_pydict() == {"value": [42]}
        assert user_agents and all(agent == "my-app/1.0" for agent in user_agents)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
