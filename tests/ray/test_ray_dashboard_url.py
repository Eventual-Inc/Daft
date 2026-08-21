from __future__ import annotations

import pytest

from daft.runners import ray_runner


@pytest.fixture(autouse=True)
def clear_dashboard_env(monkeypatch):
    monkeypatch.delenv("DAFT_RAY_DASHBOARD_URL", raising=False)
    monkeypatch.delenv("RAY_DISABLE_DASHBOARD", raising=False)


class _FakeRuntimeContext:
    def __init__(self, job_id: str | None) -> None:
        self._job_id = job_id

    def get_job_id(self) -> str | None:
        return self._job_id


@pytest.fixture
def fake_ray(monkeypatch):
    """Stub out the Ray calls made by `_resolve_ray_dashboard_url`."""

    def configure(*, initialized: bool = True, dashboard_url: str | None = None, job_id: str | None = "job-1"):
        monkeypatch.setattr(ray_runner.ray, "is_initialized", lambda: initialized)
        monkeypatch.setattr(ray_runner.ray.worker, "get_dashboard_url", lambda: dashboard_url)
        monkeypatch.setattr(ray_runner.ray, "get_runtime_context", lambda: _FakeRuntimeContext(job_id))

    return configure


def test_autodetected_url_is_normalized_and_gets_job_id(fake_ray):
    fake_ray(dashboard_url="127.0.0.1:8265")

    assert ray_runner._resolve_ray_dashboard_url() == "http://127.0.0.1:8265/#/jobs/job-1"


def test_autodetection_skipped_when_ray_dashboard_disabled(monkeypatch, fake_ray):
    fake_ray(dashboard_url="127.0.0.1:8265")
    monkeypatch.setenv("RAY_DISABLE_DASHBOARD", "1")

    assert ray_runner._resolve_ray_dashboard_url() is None


def test_autodetection_returns_none_when_ray_not_initialized(fake_ray):
    fake_ray(initialized=False)

    assert ray_runner._resolve_ray_dashboard_url() is None


def test_autodetection_tolerates_ray_errors(monkeypatch):
    def _raise() -> bool:
        raise RuntimeError("boom")

    monkeypatch.setattr(ray_runner.ray, "is_initialized", _raise)

    assert ray_runner._resolve_ray_dashboard_url() is None


def test_env_override_takes_precedence(monkeypatch, fake_ray):
    fake_ray(dashboard_url="127.0.0.1:8265")
    monkeypatch.setenv("DAFT_RAY_DASHBOARD_URL", "https://ray.example.com")

    assert ray_runner._resolve_ray_dashboard_url() == "https://ray.example.com/#/jobs/job-1"


def test_env_override_preserves_https_scheme(monkeypatch, fake_ray):
    fake_ray(job_id=None)
    monkeypatch.setenv("DAFT_RAY_DASHBOARD_URL", "https://ray.example.com/dashboard/")

    assert ray_runner._resolve_ray_dashboard_url() == "https://ray.example.com/dashboard/"


def test_env_override_without_scheme_is_normalized(monkeypatch, fake_ray):
    fake_ray(job_id=None)
    monkeypatch.setenv("DAFT_RAY_DASHBOARD_URL", "my-proxy.example.com")

    assert ray_runner._resolve_ray_dashboard_url() == "http://my-proxy.example.com"


def test_env_override_honored_when_ray_dashboard_disabled(monkeypatch, fake_ray):
    fake_ray(job_id=None)
    monkeypatch.setenv("RAY_DISABLE_DASHBOARD", "1")
    monkeypatch.setenv("DAFT_RAY_DASHBOARD_URL", "https://ray.example.com")

    assert ray_runner._resolve_ray_dashboard_url() == "https://ray.example.com"


def test_env_override_with_explicit_route_is_left_alone(monkeypatch, fake_ray):
    fake_ray(dashboard_url="127.0.0.1:8265")
    monkeypatch.setenv("DAFT_RAY_DASHBOARD_URL", "https://ray.example.com/#/overview")

    assert ray_runner._resolve_ray_dashboard_url() == "https://ray.example.com/#/overview"


def test_empty_env_override_falls_back_to_autodetection(monkeypatch, fake_ray):
    fake_ray(dashboard_url="127.0.0.1:8265")
    monkeypatch.setenv("DAFT_RAY_DASHBOARD_URL", "")

    assert ray_runner._resolve_ray_dashboard_url() == "http://127.0.0.1:8265/#/jobs/job-1"
