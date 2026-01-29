from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from openai import OpenAI
from openai._types import omit

from daft import DataType
from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedder, _models, get_input_text_token_limit_for_model
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
    dimensions: int | None = None
    embed_options: EmbedTextOptions = field(
        default_factory=lambda: EmbedTextOptions(batch_size=64, max_retries=3, on_error="raise")
    )

    def __post_init__(self) -> None:
        if self.dimensions is None:
            return
        if self.model_name in _models and not _models[self.model_name].supports_overriding_dimensions:
            raise ValueError(f"Embedding model '{self.model_name}' does not support specifying dimensions")
        if "supports_overriding_dimensions" not in self.embed_options:
            self.embed_options["supports_overriding_dimensions"] = True

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
        if self.dimensions is not None:
            return EmbeddingDimensions(size=self.dimensions, dtype=DataType.float32())
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
        # Get batch_token_limit from embed_options, default to 300_000
        batch_token_limit = self.embed_options.get("batch_token_limit", 300_000)

        # Get input_text_token_limit from model profile using helper function
        # This allows LM Studio to use the same model profiles as OpenAI
        input_text_token_limit = get_input_text_token_limit_for_model(self.model_name)

        # Determine if dimensions should be included in the request
        dimensions = (
            self.dimensions
            if (self.dimensions is not None and self.embed_options.get("supports_overriding_dimensions", False))
            else omit
        )

        return OpenAITextEmbedder(
            provider_options=self.provider_options,
            model=self.model_name,
            embed_options=self.embed_options,
            dimensions=dimensions,
            provider_name=self.get_provider(),
            batch_token_limit=batch_token_limit,
            input_text_token_limit=input_text_token_limit,
        )
