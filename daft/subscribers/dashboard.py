from __future__ import annotations

from daft.daft import dashboard as native


def launch(noop_if_initialized: bool = False) -> None:
    """Launches the Daft dashboard server on an available port.

    The server will try to start on port 3238.
    The server serves HTML/CSS/JS bundles, so you are able to point your browser towards the returned URL.

    # Arguments:
        - noop_if_initialized: bool = False
            Will not raise an exception if a Daft dashboard server process is already launched and running.
            Otherwise, an exception will be raised.
    """
    handle = native.launch(noop_if_initialized=noop_if_initialized)

    import atexit

    atexit.register(handle.shutdown, noop_if_shutdown=True)
