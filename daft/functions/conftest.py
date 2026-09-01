"""Pytest configuration for ``daft.functions`` doctests.

The audio doctests in :mod:`daft.functions.audio` glob a HuggingFace dataset
(``hf://datasets/Eventual-Inc/sample-files/audio/*.mp3``) and materialize the
result with ``df.show(3)``. On shared CI egress IPs that endpoint is regularly
rate-limited (HTTP 429), which used to be papered over with an unconditional
``# doctest: +SKIP``. That hid useful documentation when the network was fine.

Instead we run the doctest for real, and if the HuggingFace Hub rate-limits us
(or is otherwise unreachable) we skip just that single doctest item with a
descriptive message, leaving every other doctest unaffected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Sequence

_HF_PROBE_URL = "https://huggingface.co/api/datasets/Eventual-Inc/sample-files/tree/main/audio"
_HF_PROBE_TIMEOUT_SECONDS = 5.0


def _probe_hf_audio_dataset() -> tuple[bool, str]:
    """Return ``(reachable, reason)`` for the audio sample dataset on HF Hub."""
    try:
        import urllib.error
        import urllib.request

        req = urllib.request.Request(_HF_PROBE_URL, method="HEAD")
        with urllib.request.urlopen(req, timeout=_HF_PROBE_TIMEOUT_SECONDS) as resp:
            status = getattr(resp, "status", 200)
            if status == 429:
                return False, f"HuggingFace Hub rate-limited (HTTP 429) at {_HF_PROBE_URL}"
            return True, ""
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            return False, f"HuggingFace Hub rate-limited (HTTP 429) at {_HF_PROBE_URL}"
        # Any other HTTP status (e.g. 5xx) is also a transient infra issue from
        # the doctest's perspective — skip rather than fail the build.
        return False, f"HuggingFace Hub returned HTTP {exc.code} at {_HF_PROBE_URL}"
    except Exception as exc:  # pragma: no cover - network / DNS / timeout
        return False, f"HuggingFace Hub unreachable ({type(exc).__name__}: {exc})"


@pytest.fixture(scope="session")  # type: ignore[misc, untyped-decorator, unused-ignore]
def _hf_audio_dataset_available() -> tuple[bool, str]:
    """Cache one HF probe per test session and reuse for every audio doctest."""
    return _probe_hf_audio_dataset()


def pytest_collection_modifyitems(config: pytest.Config, items: Sequence[pytest.Item]) -> None:
    """Attach a soft-skip marker to ``daft.functions.audio`` doctests.

    Doctests collected by ``pytest --doctest-modules`` show up as
    ``DoctestItem`` instances whose ``nodeid`` looks like
    ``daft/functions/audio.py::daft.functions.audio.resample``. We tag those
    items so the autouse fixture below can skip them when HF Hub is unhealthy.
    """
    for item in items:
        nodeid = getattr(item, "nodeid", "")
        if "daft/functions/audio.py" in nodeid or "daft.functions.audio" in nodeid:
            item.add_marker(pytest.mark.hf_audio_doctest)


@pytest.fixture(autouse=True)  # type: ignore[misc, untyped-decorator, unused-ignore]
def _skip_audio_doctests_when_hf_unhealthy(request: pytest.FixtureRequest) -> None:
    """Skip the audio doctests when HuggingFace Hub is unreachable / 429."""
    if request.node.get_closest_marker("hf_audio_doctest") is None:
        return
    reachable, reason = request.getfixturevalue("_hf_audio_dataset_available")
    if not reachable:
        pytest.skip(reason)


def pytest_configure(config: pytest.Config) -> None:
    """Register the marker so ``--strict-markers`` users do not get warnings."""
    config.addinivalue_line(
        "markers",
        "hf_audio_doctest: doctest that depends on the HuggingFace audio sample dataset",
    )
