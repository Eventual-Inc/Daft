from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

import transformers
from transformers import pipeline
from typing_extensions import Unpack

from daft.ai.protocols import ImageClassifier, ImageClassifierDescriptor
from daft.ai.utils import get_gpu_udf_options, get_torch_device

if TYPE_CHECKING:
    import numpy as np

    from daft.ai.typing import Label, Options, UDFOptions


class TransformersImageClassifierResult(TypedDict):
    label: str  # labels sorted by likelihood
    score: float  # probability of each label


class TransformersImageClassifierOptions(TypedDict, total=False):
    batch_size: int | None


@dataclass
class TransformersImageClassifierDescriptor(ImageClassifierDescriptor):
    provider_name: str
    model_name: str
    model_options: TransformersImageClassifierOptions

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options  # type: ignore

    def get_udf_options(self) -> UDFOptions:
        return get_gpu_udf_options()

    def instantiate(self) -> ImageClassifier:
        return TransformersImageClassifier(self.model_name, **self.model_options)


class TransformersImageClassifier(ImageClassifier):
    """Pipeline based zero-shot image classification."""

    _model: str
    _options: TransformersImageClassifierOptions
    _pipeline: transformers.ImageClassificationPipeline

    def __init__(self, model_name_or_path: str, **options: Unpack[TransformersImageClassifierOptions]):
        self._model = model_name_or_path
        self._options = options

        self._pipeline = pipeline(
            task="zero-shot-image-classification", model=model_name_or_path, device=get_torch_device()
        )

    def classify_image(self, images: list[np.ndarray], labels: Label | list[Label]) -> list[Label]:
        batch_size = self._options.get("batch_size", None)
        results: list[list[TransformersImageClassifierResult]] = self._pipeline(
            images,
            batch_size=batch_size,
            candidate_labels=labels,
        )
        return [result[0]["label"] for result in results]
