from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

import transformers
from transformers import pipeline

from daft.ai.protocols import TextClassifier, TextClassifierDescriptor
from daft.ai.utils import get_torch_device

if TYPE_CHECKING:
    from daft.ai.typing import Label, Options


class ZeroShotClassificationResult(TypedDict):
    sequence: str
    labels: list[str]  # labels sorted by likelihood
    scores: list[float]  # probability of each label


@dataclass
class TransformersTextClassifierDescriptor(TextClassifierDescriptor):
    provider_name: str
    model_name: str
    model_options: Options

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def instantiate(self) -> TextClassifier:
        return TransformersTextClassifier(self.model_name, **self.model_options)


class TransformersTextClassifier(TextClassifier):
    """Pipeline based zero-shot text classification.

    Note:
        This could be improved or configurable with allowing a custom hypothesis
        template and other pipeline-related options. This is sufficient for now.
    """

    pipe: transformers.ZeroShotClassificationPipeline

    def __init__(self, model_name_or_path: str, **options: Any):
        self.device = get_torch_device()
        self.pipe = pipeline(
            task="zero-shot-classification",
            model=model_name_or_path,
            device=get_torch_device(),
        )

    def classify_text(self, text: list[str], labels: Label | list[Label]) -> list[Label]:
        results: list[ZeroShotClassificationResult] = self.pipe(text, candidate_labels=labels)
        return [result["labels"][0] for result in results]
