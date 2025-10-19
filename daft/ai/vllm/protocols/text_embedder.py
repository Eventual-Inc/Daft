from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import torch
from transformers import AutoConfig
from vllm import LLM

from daft import DataType
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, Options

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


@dataclass
class vLLMTextEmbedderDescriptor(TextEmbedderDescriptor):
    model: str
    options: Options

    def get_provider(self) -> str:
        return "vllm"

    def get_model(self) -> str:
        return self.model

    def get_options(self) -> Options:
        return self.options

    def get_dimensions(self) -> EmbeddingDimensions:
        dimensions = AutoConfig.from_pretrained(self.model, trust_remote_code=True).hidden_size
        return EmbeddingDimensions(size=dimensions, dtype=DataType.float32())

    def instantiate(self) -> TextEmbedder:
        return vLLMTextEmbedder(self.model, **self.options)


class vLLMTextEmbedder(TextEmbedder):
    model: LLM
    options: Options  # not currently used, torch hardcoded

    def __init__(self, model_name_or_path: str, **options: Any):
        config = AutoConfig.from_pretrained(model_name_or_path, trust_remote_code=True)
        max_model_len = getattr(config, "n_ctx", None) or getattr(config, "max_position_embeddings", None)
        # Let vLLM automatically determine the optimal dtype to use based on the model config file.
        self.model = LLM(
            model=model_name_or_path,
            max_num_batched_tokens=max_model_len,
            task="embed",
        )
        self.options = options

    def embed_text(self, text: list[str]) -> list[Embedding]:
        outputs = self.model.embed(text)
        embeddings = torch.tensor([o.outputs.embedding for o in outputs])
        return embeddings.cpu().numpy()
