"""Mock AI provider for testing doc code blocks without real API calls."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from daft.ai.provider import Provider
from daft.ai.protocols import (
    Prompter,
    PrompterDescriptor,
    TextClassifier,
    TextClassifierDescriptor,
    TextEmbedder,
    TextEmbedderDescriptor,
)
from daft.ai.typing import EmbeddingDimensions, Label, Options, UDFOptions
from daft.datatype import DataType


# ---------------------------------------------------------------------------
# Mock protocol implementations
# ---------------------------------------------------------------------------


class MockTextEmbedder(TextEmbedder):
    def __init__(self, size: int):
        self._size = size

    def embed_text(self, text: list[str]) -> list[Any]:
        return [np.full(self._size, 0.1, dtype=np.float32) for _ in text]


class MockTextClassifier(TextClassifier):
    def classify_text(self, text: list[str], labels: Label | list[Label]) -> list[Label]:
        first = labels[0] if isinstance(labels, list) else labels
        return [first] * len(text)


class MockPrompter(Prompter):
    def __init__(self, return_format: Any | None = None):
        self._return_format = return_format

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        if self._return_format is not None:
            # Try to instantiate the pydantic model with default/empty values
            try:
                return self._return_format()
            except Exception:
                return '{"mock": true}'
        return "Mock response"


# ---------------------------------------------------------------------------
# Mock descriptors
# ---------------------------------------------------------------------------


@dataclass
class MockTextEmbedderDescriptor(TextEmbedderDescriptor):
    _provider_name: str = "mock"
    _model_name: str = "mock-embed"
    _dimensions: int = 128

    def get_provider(self) -> str:
        return self._provider_name

    def get_model(self) -> str:
        return self._model_name

    def get_options(self) -> Options:
        return {}

    def get_dimensions(self) -> EmbeddingDimensions:
        return EmbeddingDimensions(size=self._dimensions, dtype=DataType.float32())

    def get_udf_options(self) -> UDFOptions:
        return UDFOptions(batch_size=64)

    def is_async(self) -> bool:
        return False

    def instantiate(self) -> TextEmbedder:
        return MockTextEmbedder(self._dimensions)


@dataclass
class MockTextClassifierDescriptor(TextClassifierDescriptor):
    _provider_name: str = "mock"
    _model_name: str = "mock-classify"

    def get_provider(self) -> str:
        return self._provider_name

    def get_model(self) -> str:
        return self._model_name

    def get_options(self) -> Options:
        return {}

    def get_udf_options(self) -> UDFOptions:
        return UDFOptions(batch_size=64)

    def instantiate(self) -> TextClassifier:
        return MockTextClassifier()


@dataclass
class MockPrompterDescriptor(PrompterDescriptor):
    _provider_name: str = "mock"
    _model_name: str = "mock-prompt"
    _return_format: Any = None

    def get_provider(self) -> str:
        return self._provider_name

    def get_model(self) -> str:
        return self._model_name

    def get_options(self) -> Options:
        return {}

    def get_udf_options(self) -> UDFOptions:
        return UDFOptions(batch_size=1, concurrency=1)

    def instantiate(self) -> Prompter:
        return MockPrompter(self._return_format)


# ---------------------------------------------------------------------------
# Mock provider
# ---------------------------------------------------------------------------


class MockProvider(Provider):
    """A mock provider that returns deterministic results for testing."""

    @property
    def name(self) -> str:
        return "mock"

    def get_text_embedder(self, model=None, dimensions=None, **options: Any) -> TextEmbedderDescriptor:
        return MockTextEmbedderDescriptor(
            _dimensions=dimensions or 128,
            _model_name=model or "mock-embed",
        )

    def get_text_classifier(self, model=None, **options: Any) -> TextClassifierDescriptor:
        return MockTextClassifierDescriptor(
            _model_name=model or "mock-classify",
        )

    def get_prompter(self, model=None, return_format=None, system_message=None, **options: Any) -> PrompterDescriptor:
        return MockPrompterDescriptor(
            _model_name=model or "mock-prompt",
            _return_format=return_format,
        )
