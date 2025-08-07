from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import torch
from sentence_transformers import SentenceTransformer

from daft import DataType
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, Options

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


@dataclass
class SentenceTransformersTextEmbedderDescriptor(TextEmbedderDescriptor):
    model: str
    options: Options

    def get_provider(self) -> str:
        return "sentence_transformers"

    def get_model(self) -> str:
        return self.model

    def get_options(self) -> Options:
        return self.options

    def get_dimensions(self) -> EmbeddingDimensions:
        # hardcoding all-MiniLM-L6-v2 for now
        return EmbeddingDimensions(size=384, dtype=DataType.float32())

    def instantiate(self) -> TextEmbedder:
        return SentenceTransformersTextEmbedder(self.model, **self.options)


class SentenceTransformersTextEmbedder(TextEmbedder):
    model: SentenceTransformer

    def __init__(self, model_name_or_path: str, **options: str):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(model_name_or_path, trust_remote_code=True, backend="torch")
        self.model.eval()
        self.model.to(self.device)

    def embed_text(self, text: list[str]) -> list[Embedding]:
        with torch.inference_mode():
            batch = self.model.encode(text, convert_to_numpy=True)
            return list(batch)
