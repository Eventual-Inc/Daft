from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

import transformers
from transformers import pipeline

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.protocols import TextClassifier, TextClassifierDescriptor
from daft.ai.utils import get_gpu_udf_options, get_torch_device

if TYPE_CHECKING:
    from daft.ai.typing import Label, Options, UDFOptions


class TransformersTextClassiferResult(TypedDict):
    sequence: str
    labels: list[str]  # labels sorted by likelihood
    scores: list[float]  # probability of each label


class TransformersTextClassifierOptions(TypedDict, total=False):
    batch_size: int | None


@dataclass
class TransformersTextClassifierDescriptor(TextClassifierDescriptor):
    provider_name: str
    model_name: str
    model_options: TransformersTextClassifierOptions

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options  # type: ignore

    def get_udf_options(self) -> UDFOptions:
        return get_gpu_udf_options()

    def instantiate(self) -> TextClassifier:
        return TransformersTextClassifier(self.model_name, **self.model_options)


class TransformersTextClassifier(TextClassifier):
    """Pipeline based zero-shot text classification.

    Note:
        This could be improved or configurable with allowing a custom hypothesis
        template and other pipeline-related options. This is sufficient for now.
    """

    _model: str
    _options: TransformersTextClassifierOptions
    _pipeline: transformers.ZeroShotClassificationPipeline

    def __init__(self, model_name_or_path: str, **options: Unpack[TransformersTextClassifierOptions]):
        self._model = model_name_or_path
        self._options = options
        self._pipeline = pipeline(
            task="zero-shot-classification",
            model=model_name_or_path,
            device=get_torch_device(),
        )

    def classify_text(self, text: list[str], labels: Label | list[Label]) -> list[Label]:
        batch_size = self._options.get("batch_size", None)
        results: list[TransformersTextClassiferResult] = self._pipeline(
            text,
            batch_size=batch_size,
            candidate_labels=labels,
        )
        return [result["labels"][0] for result in results]
