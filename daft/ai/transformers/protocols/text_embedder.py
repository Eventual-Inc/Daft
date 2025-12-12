from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoConfig

from daft import DataType
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, EmbedTextOptions, Options, UDFOptions
from daft.ai.utils import get_gpu_udf_options

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


class TransformersTextEmbedderOptions(EmbedTextOptions, total=False):
    pass


@dataclass
class TransformersTextEmbedderDescriptor(TextEmbedderDescriptor):
    model: str
    options: TransformersTextEmbedderOptions

    def get_provider(self) -> str:
        return "transformers"

    def get_model(self) -> str:
        return self.model

    def get_options(self) -> Options:
        return dict(self.options)

    def get_dimensions(self) -> EmbeddingDimensions:
        dimensions = AutoConfig.from_pretrained(self.model, trust_remote_code=True).hidden_size
        return EmbeddingDimensions(size=dimensions, dtype=DataType.float32())

    def get_udf_options(self) -> UDFOptions:
        udf_options = get_gpu_udf_options()
        udf_options.max_retries = self.options["max_retries"]
        return udf_options

    def instantiate(self) -> TextEmbedder:
        return TransformersTextEmbedder(self.model, **self.options)


class TransformersTextEmbedder(TextEmbedder):
    model: SentenceTransformer
    options: TransformersTextEmbedderOptions

    def __init__(self, model_name_or_path: str, **options: Any):
        # Let SentenceTransformer handle device selection automatically.
        self.model = SentenceTransformer(model_name_or_path, trust_remote_code=True, backend="torch")
        self.model.eval()
        self.options = cast("TransformersTextEmbedderOptions", options)

    def embed_text(self, text: list[str]) -> list[Embedding]:
        with torch.inference_mode():
            batch = self.model.encode(text, convert_to_numpy=True)
            return list(batch)
