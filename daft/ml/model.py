from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from daft.ml.typing import Provider


@runtime_checkable
class Model(Protocol):
    @classmethod
    def from_provider(cls, provider: Provider, model: str | None = None, **properties: str) -> Model:
        if provider == "vllm":
            return cls._from_vllm(model, **properties)
        elif provider == "sentence_transformers":
            return cls._from_sentence_transformers(model, **properties)
        raise ValueError(f"Provider is not yet supported: {provider}")

    @classmethod
    def _from_vllm(cls, model: str | None = None, **properties: str) -> Model:
        try:
            from daft.ml._vllm import _VLLM

            return _VLLM(model, **properties)
        except ImportError:
            raise ImportError("The vllm import is missing, please install 'vllm' to use this provider.")

    @classmethod
    def _from_sentence_transformers(cls, model: str | None = None, **properties: str) -> Model:
        try:
            from daft.ml._transformers import _SentenceTransformers

            if model is None:
                raise ValueError("The sentence_transformers provider requires a model identifier.")
            return _SentenceTransformers(model, **properties)
        except ImportError as e:
            raise ImportError(
                "The sentence_transformers or torch import is missing, please install 'sentence_transformers' and 'torch' to use this provider."
            ) from e

    def __enter__(self) -> None:
        """Perform any initialization logic for this model resource."""
        ...

    def __exit__(self) -> None:
        """Perform any teardown logic for this model resource."""
        ...
