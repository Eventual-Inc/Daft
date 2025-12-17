from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

import torch
from transformers import AutoConfig, AutoModel, AutoProcessor

from daft import DataType
from daft.ai.protocols import ImageEmbedder, ImageEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, EmbedImageOptions, Options, UDFOptions
from daft.ai.utils import get_gpu_udf_options, get_torch_device
from daft.dependencies import pil_image

if TYPE_CHECKING:
    from daft.ai.typing import Embedding, Image


@dataclass
class TransformersImageEmbedderDescriptor(ImageEmbedderDescriptor):
    model: str
    embed_options: EmbedImageOptions = field(default_factory=lambda: EmbedImageOptions(batch_size=16))

    def get_provider(self) -> str:
        return "transformers"

    def get_model(self) -> str:
        return self.model

    def get_options(self) -> Options:
        return dict(self.embed_options)

    def get_dimensions(self) -> EmbeddingDimensions:
        config = AutoConfig.from_pretrained(self.model, trust_remote_code=True)
        # For CLIP models, the image embedding dimension is typically in projection_dim or hidden_size.
        embedding_size = getattr(config, "projection_dim", getattr(config, "hidden_size", 512))
        return EmbeddingDimensions(size=embedding_size, dtype=DataType.float32())

    def get_udf_options(self) -> UDFOptions:
        udf_options = get_gpu_udf_options()
        for key, value in self.embed_options.items():
            if key in udf_options.__annotations__.keys():
                setattr(udf_options, key, value)
        return udf_options

    def instantiate(self) -> ImageEmbedder:
        return TransformersImageEmbedder(self.model, **self.embed_options)


class TransformersImageEmbedder(ImageEmbedder):
    model: Any
    embed_options: EmbedImageOptions

    def __init__(self, model_name_or_path: str, **embed_options: Unpack[EmbedImageOptions]):
        self.device = get_torch_device()
        self.model = AutoModel.from_pretrained(
            model_name_or_path,
            trust_remote_code=True,
            use_safetensors=True,
        ).to(self.device)
        self.processor = AutoProcessor.from_pretrained(model_name_or_path, trust_remote_code=True, use_fast=True)
        self.embed_options: EmbedImageOptions = embed_options

    def embed_image(self, images: list[Image]) -> list[Embedding]:
        # TODO(desmond): There's potential for image decoding and processing on the GPU with greater
        # performance. Methods differ a little between different models, so let's do it later.
        pil_images = [pil_image.fromarray(image) for image in images]
        processed = self.processor(images=pil_images, return_tensors="pt")
        pixel_values = processed["pixel_values"].to(self.device)

        with torch.inference_mode():
            embeddings = self.model.get_image_features(pixel_values)
        return embeddings.cpu().numpy().tolist()
