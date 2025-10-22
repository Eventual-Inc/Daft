from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoConfig

from daft import DataType
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, Options, UDFOptions
from daft.ai.utils import get_gpu_udf_options

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


@dataclass
class TransformersTextEmbedderDescriptor(TextEmbedderDescriptor):
    model: str
    options: Options

    def get_provider(self) -> str:
        return "transformers"

    def get_model(self) -> str:
        return self.model

    def get_options(self) -> Options:
        return self.options

    def get_dimensions(self) -> EmbeddingDimensions:
        dimensions = AutoConfig.from_pretrained(self.model, trust_remote_code=True).hidden_size
        return EmbeddingDimensions(size=dimensions, dtype=DataType.float32())

    def get_udf_options(self) -> UDFOptions:
        return get_gpu_udf_options()

    def instantiate(self) -> TextEmbedder:
        return TransformersTextEmbedder(self.model, **self.options)


class TransformersTextEmbedder(TextEmbedder):
    model: SentenceTransformer
    options: Options  # not currently used, torch hardcoded

    def __init__(self, model_name_or_path: str, **options: Any):
        # Let SentenceTransformer handle device selection automatically.
        self.model = SentenceTransformer(model_name_or_path, trust_remote_code=True, backend="torch")
        self.model.eval()
        self.options = options

    def embed_text(self, text: list[str]) -> list[Embedding]:
        with torch.inference_mode():
            batch = self.model.encode(text, convert_to_numpy=True)
            return list(batch)
