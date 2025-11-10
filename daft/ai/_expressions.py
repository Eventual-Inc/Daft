from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any

from daft.dependencies import pil_image

if TYPE_CHECKING:
    from daft import Series
    from daft.ai.protocols import (
        ImageClassifier,
        ImageClassifierDescriptor,
        ImageEmbedder,
        ImageEmbedderDescriptor,
        Prompter,
        PrompterDescriptor,
        TextClassifier,
        TextClassifierDescriptor,
        TextEmbedder,
        TextEmbedderDescriptor,
    )
    from daft.ai.typing import Embedding, Label


class _TextEmbedderExpression:
    """Function expression implementation for a TextEmbedder protocol."""

    text_embedder: TextEmbedder

    def __init__(self, text_embedder: TextEmbedderDescriptor):
        self.text_embedder = text_embedder.instantiate()

    def _call_sync(self, text_series: Series) -> list[Embedding]:
        text = text_series.to_pylist()
        if not text:
            return []
        result = self.text_embedder.embed_text(text)
        assert isinstance(result, list)
        return result

    async def _call_async(self, text_series: Series) -> list[Embedding]:
        text = text_series.to_pylist()
        if not text:
            return []
        result_awaitable = self.text_embedder.embed_text(text)
        assert isinstance(result_awaitable, Awaitable)
        return await result_awaitable


class _ImageEmbedderExpression:
    """Function expression implementation for an ImageEmbedder protocol."""

    image_embedder: ImageEmbedder

    def __init__(self, image_embedder: ImageEmbedderDescriptor):
        self.image_embedder = image_embedder.instantiate()

    def __call__(self, image_series: Series) -> list[Embedding]:
        image = image_series.to_pylist()
        return self.image_embedder.embed_image(image) if image else []


class _TextClassificationExpression:
    """Function expression implementation for a TextClassifier protocol."""

    text_classifier: TextClassifier
    labels: list[Label]

    def __init__(self, text_classifier: TextClassifierDescriptor, labels: list[Label]):
        self.text_classifier = text_classifier.instantiate()
        self.labels = labels

    def __call__(self, text_series: Series) -> list[Label]:
        text = text_series.to_pylist()
        return self.text_classifier.classify_text(text, labels=self.labels) if text else []


class _ImageClassificationExpression:
    """Function expression implementation for a ImageClassifier protocol."""

    image_classifier: ImageClassifier
    labels: list[Label]

    def __init__(self, image_classifier: ImageClassifierDescriptor, labels: list[Label]):
        self.image_classifier = image_classifier.instantiate()
        self.labels = labels

    def __call__(self, image_series: Series) -> list[Label]:
        if len(image_series) == 0:
            return []

        images = [pil_image.fromarray(image) for image in image_series]
        return self.image_classifier.classify_image(images, labels=self.labels)


class _PrompterExpression:
    """Function expression implementation for a Prompter protocol."""

    prompter: Prompter

    def __init__(self, prompter: PrompterDescriptor):
        self.prompter = prompter.instantiate()

    async def prompt(self, *messages: Any) -> Any:
        return await self.prompter.prompt(messages)
