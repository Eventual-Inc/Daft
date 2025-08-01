from __future__ import annotations

from typing import Literal

from daft.dependencies import np

Provider = Literal["vllm", "openai", "transformers"]

Embedding = np.ndarray
