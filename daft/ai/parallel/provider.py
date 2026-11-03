from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider, ProviderImportError
from daft.ai.typing import ClassifyTextOptions

if TYPE_CHECKING:
    from daft.ai.parallel.typing import ParallelProviderOptions
    from daft.ai.protocols import TextClassifierDescriptor


class ParallelProvider(Provider):
    DEFAULT_MODE = "fast"

    def __init__(self, name: str | None = None, **options: Unpack[ParallelProviderOptions]):
        from daft.dependencies import requests

        if not requests.module_available():
            raise ProviderImportError("parallel")

        self._name = name or "parallel"
        self._api_key = options.get("api_key") or os.environ.get("PARALLEL_API_KEY")
        if not self._api_key:
            raise ValueError("No Parallel API key was provided. Pass api_key or set PARALLEL_API_KEY.")
        self._base_url = (options.get("base_url") or "https://api.parallel.ai").rstrip("/")
        self._timeout = options.get("timeout", 30.0)

    @property
    def name(self) -> str:
        return self._name

    def get_text_classifier(
        self, model: str | None = None, **options: Unpack[ClassifyTextOptions]
    ) -> TextClassifierDescriptor:
        from daft.ai.parallel.protocols.text_classifier import ParallelTextClassifierDescriptor

        mode = model or self.DEFAULT_MODE
        if mode != self.DEFAULT_MODE:
            raise ValueError(f"Unsupported Parallel Decisions mode {mode!r}; expected {self.DEFAULT_MODE!r}")

        classify_options = ClassifyTextOptions(
            batch_size=25,
            concurrency=16,
            max_retries=3,
            on_error="raise",
        )
        classify_options.update(options)
        batch_size = classify_options["batch_size"]
        if not 1 <= batch_size <= 100:
            raise ValueError(f"Parallel Decisions batch_size must be between 1 and 100, got {batch_size}")

        return ParallelTextClassifierDescriptor(
            provider_name=self._name,
            api_key=self._api_key,
            base_url=self._base_url,
            timeout=self._timeout,
            mode=mode,
            classify_options=classify_options,
        )
