from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoConfig

from daft import DataType
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, EmbedTextOptions, Options, UDFOptions
from daft.ai.utils import get_gpu_udf_options

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


@dataclass
class TransformersTextEmbedderDescriptor(TextEmbedderDescriptor):
    model: str
    embed_options: EmbedTextOptions = field(default_factory=lambda: EmbedTextOptions(batch_size=64))

    def get_provider(self) -> str:
        return "transformers"

    def get_model(self) -> str:
        return self.model

    def get_options(self) -> Options:
        return dict(self.embed_options)

    def get_dimensions(self) -> EmbeddingDimensions:
        dimensions = AutoConfig.from_pretrained(self.model, trust_remote_code=True).hidden_size
        return EmbeddingDimensions(size=dimensions, dtype=DataType.float32())

    def get_udf_options(self) -> UDFOptions:
        udf_options = get_gpu_udf_options()
        for key, value in self.embed_options.items():
            if key in udf_options.__annotations__.keys():
                setattr(udf_options, key, value)
        return udf_options

    def instantiate(self) -> TextEmbedder:
        return TransformersTextEmbedder(self.model, **self.embed_options)


class TransformersTextEmbedder(TextEmbedder):
    model: SentenceTransformer
    embed_options: EmbedTextOptions

    def __init__(self, model_name_or_path: str, **embed_options: Unpack[EmbedTextOptions]):
        # Let SentenceTransformer handle device selection automatically.
        self.model = SentenceTransformer(model_name_or_path, trust_remote_code=True, backend="torch")
        self.model.eval()
        self.embed_options = embed_options

    def embed_text(self, text: list[str]) -> list[Embedding]:
        with torch.inference_mode():
            batch = self.model.encode(text, convert_to_numpy=True)
            return list(batch)
