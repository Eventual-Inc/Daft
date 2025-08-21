from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from daft.datatype import DataType
from daft.dependencies import np

Options = dict[str, str]

T = TypeVar("T")


class Descriptor(ABC, Generic[T]):
    """A descriptor is a serializable factory, with protocol-specific properties, used to instantiate a model."""

    @abstractmethod
    def get_provider(self) -> str:
        """Returns the provider that created this descriptor."""
        ...

    @abstractmethod
    def get_model(self) -> str:
        """Returns the model identifier for this descriptor."""
        ...

    @abstractmethod
    def get_options(self) -> Options:
        """Returns the model instantiation options."""
        ...

    @abstractmethod
    def instantiate(self) -> T:
        """Instantiates and returns a concrete model instance corresponding to this descriptor."""


# temp definition to defer complexity of a more generic embedding type to later PRs
Embedding = np.typing.NDArray[Any]


@dataclass
class EmbeddingDimensions:
    size: int
    dtype: DataType

    def as_dtype(self) -> DataType:
        return DataType.embedding(dtype=self.dtype, size=self.size)
