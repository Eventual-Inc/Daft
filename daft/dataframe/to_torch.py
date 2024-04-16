from __future__ import annotations

import logging
from typing import Any, Iterable, Iterator

logger = logging.getLogger(__name__)

try:
    # When available, subclass from the newer torchdata DataPipes instead of torch Datasets.
    import torchdata

    MAP_DATASET_CLASS = torchdata.datapipes.map.MapDataPipe
    ITER_DATASET_CLASS = torchdata.datapipes.iter.IterDataPipe
except ImportError:
    try:
        import torch

        MAP_DATASET_CLASS = torch.utils.data.Dataset
        ITER_DATASET_CLASS = torch.utils.data.IterableDataset
    except ImportError:
        logger.error("Error when importing Torch. To use PyTorch features, please install torch.")
        raise


class DaftTorchDataset(MAP_DATASET_CLASS):  # type: ignore
    """A wrapper to create a torch map-style Dataset from a Daft pydict of items."""

    def __init__(self, data: dict[str, list[Any]], length: int):
        self.data = data
        self.length = length

    def __len__(self):
        return self.length

    def __getitem__(self, i):
        return {key: vallist[i] for (key, vallist) in self.data.items()}


class DaftTorchIterableDataset(ITER_DATASET_CLASS):  # type: ignore
    """A thin wrapper to create a torch IterableDataset from an iterable."""

    def __init__(self, iterable: Iterable[dict[str, Any]]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.iterable)
