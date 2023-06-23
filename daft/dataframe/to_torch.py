from __future__ import annotations

from typing import Any, Iterable, Iterator

from loguru import logger

try:
    import torch
except ImportError:
    logger.error(f"Error when importing Torch. To use PyTorch features, please install torch.")
    raise


class DaftTorchDataset(torch.utils.data.Dataset):
    """A wrapper to create a torch map-style Dataset from a Daft pydict of items."""

    def __init__(self, data: dict[str, list[Any]], length: int):
        self.data = data
        self.length = length

    def __len__(self):
        return self.length

    def __getitem__(self, i):
        return {key: vallist[i] for (key, vallist) in self.data.items()}


class DaftTorchIterableDataset(torch.utils.data.IterableDataset):
    """A thin wrapper to create a torch IterableDataset from an iterable."""

    def __init__(self, iterable: Iterable[dict[str, Any]]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.iterable)
