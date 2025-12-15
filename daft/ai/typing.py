from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypedDict, TypeVar

from daft.datatype import DataType

if TYPE_CHECKING:
    from typing import Literal


Options = dict[str, Any]


class EmbedTextOptions(TypedDict, total=False):
    """Options for text embedding.

    Attributes:
        batch_size (int): Number of texts to process in a single batch.
        max_retries (int): Maximum number of retry attempts for failed requests.
        on_error (Literal["raise", "log", "ignore"]): Behavior when an error occurs.

    Note:
        Any additional arguments defined here will be forwarded directly to
        the provider-specific client when making embedding calls.
    """

    batch_size: int
    max_retries: int
    on_error: Literal["raise", "log", "ignore"]


class EmbedImageOptions(TypedDict, total=False):
    """Options for image embedding.

    Attributes:
        batch_size (int): Number of images to process in a single batch.
        max_retries (int): Maximum number of retry attempts for failed requests.
        on_error (Literal["raise", "log", "ignore"]): Behavior when an error occurs.

    Note:
        Any additional arguments defined here will be forwarded directly to
        the provider-specific client when making embedding calls.
    """

    batch_size: int
    max_retries: int
    on_error: Literal["raise", "log", "ignore"]


class ClassifyTextOptions(TypedDict, total=False):
    """Options for text classification.

    Attributes:
        batch_size (int): Number of texts to process in a single batch.
        max_retries (int): Maximum number of retry attempts for failed requests.
        on_error (Literal["raise", "log", "ignore"]): Behavior when an error occurs.

    Note:
        Any additional arguments defined here will be forwarded directly to
        the provider-specific client when making classification calls.
    """

    batch_size: int
    max_retries: int
    on_error: Literal["raise", "log", "ignore"]


class ClassifyImageOptions(TypedDict, total=False):
    """Options for image classification.

    Attributes:
        batch_size (int): Number of images to process in a single batch.
        max_retries (int): Maximum number of retry attempts for failed requests.
        on_error (Literal["raise", "log", "ignore"]): Behavior when an error occurs.

    Note:
        Any additional arguments defined here will be forwarded directly to
        the provider-specific client when making classification calls.
    """

    batch_size: int
    max_retries: int
    on_error: Literal["raise", "log", "ignore"]


class PromptOptions(TypedDict, total=False):
    """Options for prompting.

    Attributes:
        max_retries (int): Maximum number of retry attempts for failed requests.
        on_error (Literal["raise", "log", "ignore"]): Behavior when an error occurs.

    Note:
        Any additional arguments defined here will be forwarded directly to
        the provider-specific client when making prompt calls.
    """

    max_retries: int
    on_error: Literal["raise", "log", "ignore"]


T = TypeVar("T")

__all__ = [
    "ClassifyImageOptions",
    "ClassifyTextOptions",
    "Descriptor",
    "EmbedImageOptions",
    "EmbedTextOptions",
    "Embedding",
    "EmbeddingDimensions",
    "Image",
    "Label",
    "Options",
    "PromptOptions",
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

    def get_udf_options(self) -> UDFOptions:
        from daft.utils import from_dict

        return from_dict(UDFOptions, self.get_options())


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
    batch_size: int | None = None


Label = str
