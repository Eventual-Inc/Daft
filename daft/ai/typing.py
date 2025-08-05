from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from daft.datatype import DataType
from daft.dependencies import np

Provider = Literal["vllm", "openai", "sentence_transformers"]

Embedding = np.ndarray


@dataclass
class EmbeddingDimensions:
    size: int
    dtype: DataType

    def as_dtype(self) -> DataType:
        return DataType.embedding(dtype=self.dtype, size=self.size)

    def as_numpy(self) -> tuple[int, type]:
        if self.dtype.is_float32():
            return (self.size, np.float32)
        elif self.dtype.is_float64():
            return (self.size, np.float64)
        else:
            raise ValueError("Unsupported embedding size.")
