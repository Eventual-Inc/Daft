from __future__ import annotations

from daft.ml.model import Model


class _VLLM(Model):
    """VLLM-based implementation of the Model protocol."""

    _model: str

    def __init__(self, model: str | None = None, **properties: str):
        self._model = model

    def __enter__(self) -> None:
        """Performs the VLLM instantiation logic."""
        ...

    def __exit__(self) -> None:
        """Perform any teardown logic for this model resource."""
        ...
