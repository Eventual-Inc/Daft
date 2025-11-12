from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar

from daft.datatype import DataType

if TYPE_CHECKING:
    from typing import Literal


Options = dict[str, Any]

T = TypeVar("T")

__all__ = [
    "Descriptor",
    "Embedding",
    "EmbeddingDimensions",
    "Image",
    "Label",
    "UDFOptions",
]


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

    @abstractmethod
    def get_udf_options(self) -> UDFOptions:
        """Returns the UDF options for this descriptor."""
        ...


# temp definition to defer complexity of a more generic embedding type to later PRs
if TYPE_CHECKING:
    from daft.dependencies import np

    Embedding: TypeAlias = np.typing.NDArray[Any]
    Image: TypeAlias = np.typing.NDArray[Any]
else:
    Embedding: TypeAlias = Any
    Image: TypeAlias = Any


@dataclass
class EmbeddingDimensions:
    size: int
    dtype: DataType

    def as_dtype(self) -> DataType:
        return DataType.embedding(dtype=self.dtype, size=self.size)


@dataclass
class UDFOptions:
    """Options for configuring UDF execution."""

    concurrency: int | None = None
    num_gpus: int | None = None
    max_retries: int = 3
    on_error: Literal["raise", "log", "ignore"] = "raise"


Label = str
