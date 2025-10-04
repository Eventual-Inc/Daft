from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import torch
from transformers import AutoModel, AutoProcessor

from daft.ai.protocols import VisualUnderstanding, VisualUnderstandingDescriptor
from daft.ai.utils import get_torch_device
from daft.dependencies import pil_image

if TYPE_CHECKING:
    from daft.ai.typing import Image, Options


@dataclass
class TransformersVisualUnderstandingDescriptor(VisualUnderstandingDescriptor):
    model: str
    options: Options

    def get_provider(self) -> str:
        return "transformers"

    def get_model(self) -> str:
        return self.model

    def get_options(self) -> Options:
        return self.options

    def instantiate(self) -> VisualUnderstanding:
        return TransformersVisualUnderstanding(self.model, **self.options)


class TransformersVisualUnderstanding(VisualUnderstanding):
    model: Any
    options: Options

    def __init__(self, model_name_or_path: str, **options: Any):
        self.device = get_torch_device()
        self.model = AutoModel.from_pretrained(
            model_name_or_path,
            trust_remote_code=True,
            use_safetensors=True,
        ).to(self.device)
        self.processor = AutoProcessor.from_pretrained(model_name_or_path, trust_remote_code=True, use_fast=True)
        self.options = options

    def understand_visual(self, images: list[Image], texts: list[str]) -> list[str]:
        # TODO(desmond): There's potential for image decoding and processing on the GPU with greater
        # performance. Methods differ a little between different models, so let's do it later.
        pil_images = [pil_image.fromarray(image) for image in images]
        
        # Process both images and texts for visual understanding tasks
        if texts and len(texts) == len(images):
            # For models that support text-conditioned visual understanding
            processed = self.processor(images=pil_images, text=texts, return_tensors="pt", padding=True)
        else:
            # For image captioning or general visual understanding
            processed = self.processor(images=pil_images, return_tensors="pt")
        
        # Move tensors to device
        processed = {k: v.to(self.device) if isinstance(v, torch.Tensor) else v for k, v in processed.items()}

        with torch.inference_mode():
            # Generate text outputs for visual understanding
            if hasattr(self.model, 'generate'):
                # For generative models (e.g., image captioning models)
                outputs = self.model.generate(**processed, max_length=50, num_beams=4, early_stopping=True)
                # Decode the generated tokens to text
                results = self.processor.batch_decode(outputs, skip_special_tokens=True)
            elif hasattr(self.model, 'get_image_features'):
                # Fallback: if model only provides embeddings, convert to string representation
                embeddings = self.model.get_image_features(processed["pixel_values"])
                # Convert embeddings to string representation (this is a fallback approach)
                results = [f"Image embedding: {emb.cpu().numpy().tolist()[:5]}..." for emb in embeddings]
            else:
                # Generic fallback
                results = ["Visual understanding not supported for this model" for _ in images]
        
        return results
