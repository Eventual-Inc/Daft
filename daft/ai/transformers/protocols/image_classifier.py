from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

import transformers
from transformers import pipeline

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.protocols import ImageClassifier, ImageClassifierDescriptor
from daft.ai.utils import get_gpu_udf_options, get_torch_device
from daft.dependencies import tf, torch

if TYPE_CHECKING:
    from PIL import Image

    from daft.ai.typing import Label, Options, UDFOptions


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


class TransformersImageClassifierPipeline(transformers.ZeroShotImageClassificationPipeline):  # type: ignore
    """Modified version of the original implementation to only return the top label."""

    # this postprocess was copy/pasted from the original implementation,
    # but the output is slightly modified to only return the top label
    def postprocess(self, model_outputs: Any) -> str:
        candidate_labels = model_outputs.pop("candidate_labels")
        logits = model_outputs["logits"][0]
        if self.framework == "pt" and "siglip" in self.model.config.model_type:
            probs = torch.sigmoid(logits).squeeze(-1)
            scores = probs.tolist()
            if not isinstance(scores, list):
                scores = [scores]
        elif self.framework == "pt":
            probs = logits.softmax(dim=-1).squeeze(-1)
            scores = probs.tolist()
            if not isinstance(scores, list):
                scores = [scores]
        elif self.framework == "tf":
            probs = tf.nn.softmax(logits=logits + 1e-9, axis=-1)
            scores = probs.numpy().tolist()
        else:
            raise ValueError(f"Unsupported framework: {self.framework}")

        sorted_results = sorted(zip(scores, candidate_labels), key=lambda x: -x[0])

        return sorted_results[0][1]


class TransformersImageClassifier(ImageClassifier):
    """Pipeline based zero-shot image classification."""

    _model: str
    _options: TransformersImageClassifierOptions
    _pipeline: transformers.ZeroShotImageClassificationPipeline

    def __init__(self, model_name_or_path: str, **options: Unpack[TransformersImageClassifierOptions]):
        self._model = model_name_or_path
        self._options = options

        self._pipeline = pipeline(
            task="zero-shot-image-classification",
            model=model_name_or_path,
            device=get_torch_device(),
            pipeline_class=TransformersImageClassifierPipeline,
        )

    def classify_image(self, images: list[Image.Image], labels: Label | list[Label]) -> list[Label]:
        batch_size = self._options.get("batch_size", None)
        return self._pipeline(
            images,
            batch_size=batch_size,
            candidate_labels=labels,
        )
