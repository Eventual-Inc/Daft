from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from openai import OpenAI

from daft import DataType
from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedder
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, EmbedTextOptions, Options, UDFOptions
from daft.utils import from_dict

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions


@dataclass
class LMStudioTextEmbedderDescriptor(TextEmbedderDescriptor):
    """LM Studio text embedder descriptor that dynamically discovers model dimensions.

    Unlike OpenAI, LM Studio can load different models with varying embedding dimensions.
    This descriptor queries the local server to get the actual model dimensions.
    """

    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    embed_options: EmbedTextOptions = field(
        default_factory=lambda: EmbedTextOptions(batch_size=64, max_retries=3, on_error="raise")
    )

    def get_provider(self) -> str:
        return "lm_studio"

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.embed_options)

    def get_udf_options(self) -> UDFOptions:
        options = from_dict(UDFOptions, dict(self.embed_options))
        options.max_retries = 0  # OpenAI client handles retries internally
        return options

    def is_async(self) -> bool:
        return True

    def get_dimensions(self) -> EmbeddingDimensions:
        try:
            client = OpenAI(**self.provider_options)
            response = client.embeddings.create(
                input="dimension probe",
                model=self.model_name,
                encoding_format="float",
            )
            size = len(response.data[0].embedding)
            return EmbeddingDimensions(size=size, dtype=DataType.float32())
        except Exception as ex:
            raise ValueError("Failed to determine embedding dimensions from LM Studio.") from ex

    def instantiate(self) -> TextEmbedder:
        return OpenAITextEmbedder(
            provider_options=self.provider_options,
            model=self.model_name,
            embed_options=self.embed_options,
            provider_name=self.get_provider(),
        )
