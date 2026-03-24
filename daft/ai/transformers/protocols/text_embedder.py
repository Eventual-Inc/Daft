from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoConfig

from daft import DataType
from daft.ai.protocols import TextEmbedder
from daft.ai.typing import EmbeddingDimensions, EmbedTextOptions

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


def resolve_dimensions(model_name: str, dimensions_override: int | None) -> EmbeddingDimensions:
    """Resolves embedding dimensions for a transformers model.

    Uses AutoConfig to determine the model's hidden size, validates the override
    if provided, and returns the resolved EmbeddingDimensions.
    """
    hidden_size = AutoConfig.from_pretrained(model_name, trust_remote_code=True).hidden_size

    if dimensions_override is not None:
        if dimensions_override > hidden_size:
            raise ValueError(
                f"Requested dimensions ({dimensions_override}) exceeds model output size ({hidden_size}) for '{model_name}'."
            )
        return EmbeddingDimensions(size=dimensions_override, dtype=DataType.float32())

    return EmbeddingDimensions(size=hidden_size, dtype=DataType.float32())


class TransformersTextEmbedder(TextEmbedder):
    model: SentenceTransformer
    embed_options: EmbedTextOptions

    def __init__(
        self,
        model_name_or_path: str,
        dimensions: int | None = None,
        **embed_options: Unpack[EmbedTextOptions],
    ):
        # Let SentenceTransformer handle device selection automatically.
        self.model = SentenceTransformer(model_name_or_path, trust_remote_code=True, backend="torch")
        self.model.eval()
        self.embed_options = embed_options
        self.dimensions = dimensions

    def embed_text(self, text: list[str]) -> list[Embedding]:
        with torch.inference_mode():
            batch = self.model.encode(text, convert_to_numpy=True, truncate_dim=self.dimensions)
            return list(batch)
