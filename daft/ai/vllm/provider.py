from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedderDescriptor
    from daft.ai.typing import Options


class VLLMProvider(Provider):
    _name: str
    _options: Options

    def __init__(self, name: str | None = None, **options: Any):
        self._name = name if name else "vllm"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        from daft.ai.vllm.protocols.text_embedder import vLLMTextEmbedderDescriptor

        return vLLMTextEmbedderDescriptor(model or "sentence-transformers/all-MiniLM-L6-v2", options)
