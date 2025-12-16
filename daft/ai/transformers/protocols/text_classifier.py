from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypedDict

import transformers
from transformers import pipeline

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.protocols import TextClassifier, TextClassifierDescriptor
from daft.ai.typing import ClassifyTextOptions
from daft.ai.utils import get_gpu_udf_options, get_torch_device

if TYPE_CHECKING:
    from daft.ai.typing import Label, Options, UDFOptions


class TransformersTextClassiferResult(TypedDict):
    sequence: str
    labels: list[str]  # labels sorted by likelihood
    scores: list[float]  # probability of each label


@dataclass
class TransformersTextClassifierDescriptor(TextClassifierDescriptor):
    provider_name: str
    model_name: str
    classify_options: ClassifyTextOptions = field(default_factory=lambda: ClassifyTextOptions(batch_size=64))

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.classify_options)

    def get_udf_options(self) -> UDFOptions:
        udf_options = get_gpu_udf_options()
        for key, value in self.classify_options.items():
            if key in udf_options.__annotations__.keys():
                setattr(udf_options, key, value)
        return udf_options

    def instantiate(self) -> TextClassifier:
        return TransformersTextClassifier(self.model_name, **self.classify_options)


class TransformersTextClassifier(TextClassifier):
    """Pipeline based zero-shot text classification.

    Note:
        This could be improved or configurable with allowing a custom hypothesis
        template and other pipeline-related options. This is sufficient for now.
    """

    _model: str
    _options: ClassifyTextOptions
    _pipeline: transformers.ZeroShotClassificationPipeline

    def __init__(self, model_name_or_path: str, **options: Unpack[ClassifyTextOptions]):
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
