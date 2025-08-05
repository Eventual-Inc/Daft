from __future__ import annotations

from typing import TYPE_CHECKING

import torch
from sentence_transformers import SentenceTransformer

from daft import DataType
from daft.ai._util import weak_once
from daft.ai.protocols import TextEmbedder
from daft.ai.typing import EmbeddingDimensions

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


class _SentenceTransformers(TextEmbedder):

    model_name_or_path: str

    def __init__(self, model_name_or_path: str):
        self.model_name_or_path = model_name_or_path

    @weak_once()
    def get_model(self) -> SentenceTransformer:
        """Initializes a single instance of the SentenceTransformer."""
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model = SentenceTransformer(self.model_name_or_path, trust_remote_code=True, backend="torch")
        model.eval() # to eval mode
        model.to(device)
        return model

    def get_dimensions(self) -> EmbeddingDimensions:
        # hardcoding all-MiniLM-L6-v2
        return EmbeddingDimensions(size=384, dtype=DataType.float32())

    def embed_text(self, text: list[str]) -> list[Embedding]:
        """Embeds a single string of text into an embedding vector."""
        with torch.inference_mode():
            model = self.get_model()
            batch = model.encode(text, convert_to_numpy=True)  # type: ignore
            return list(batch)
