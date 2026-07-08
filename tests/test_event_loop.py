from __future__ import annotations

import pytest


@pytest.mark.timeout(5)
def test_background_event_loop_runs_coroutine():
    from daft.event_loop import BackgroundEventLoop

    async def return_value() -> int:
        return 1

    event_loop = BackgroundEventLoop()

    assert event_loop.run(return_value()) == 1


@pytest.mark.timeout(5)
def test_background_event_loop_startup_errors_propagate(monkeypatch):
    import asyncio

    from daft.event_loop import BackgroundEventLoop

    def fail_new_event_loop():
        raise RuntimeError("loop startup failed")

    monkeypatch.setattr(asyncio, "new_event_loop", fail_new_event_loop)

    with pytest.raises(RuntimeError, match="loop startup failed"):
        BackgroundEventLoop()
