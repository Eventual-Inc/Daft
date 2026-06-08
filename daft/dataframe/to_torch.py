from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from daft.dependencies import torch
from daft.runners.partitioning import MaterializedResult

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from daft.dataframe.dataframe import DataFrame
    from daft.recordbatch.micropartition import MicroPartition

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

    def __len__(self) -> int:
        return self.length

    def __getitem__(self, i: int) -> dict[str, Any]:
        return {key: vallist[i] for (key, vallist) in self.data.items()}


class DaftTorchIterableDataset(ITER_DATASET_CLASS):  # type: ignore
    """A thin wrapper to create a torch IterableDataset from an iterable."""

    def __init__(self, iterable: Iterable[dict[str, Any]]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.iterable)


class DaftTorchDataLoader:
    """Streams batched partitions from a Daft DataFrame and yields PyTorch-ready batch dicts.

    Note:
        This simulates the behavior of a PyTorch DataLoader, but does not use the DataLoader class itself.
        If the underlying DataFrame is already materialized, it will reuse the existing data.
    """

    def __init__(
        self,
        df: DataFrame,
        batch_size: int = 1,
        *,
        drop_last: bool = False,
        pin_memory: bool = False,
        pin_memory_device: str = "",
        prefetch_count: int = 0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        self._batch_size = batch_size
        self._drop_last = drop_last
        self._pin_memory = pin_memory
        self._pin_memory_device = pin_memory_device if pin_memory_device else None
        self._prefetch_count = prefetch_count

        self._batched_df = df.into_batches(batch_size)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        from daft.runners import get_or_create_runner

        results = self._batched_df._result
        if results is not None:
            for _, mat_result in results.items():
                batch = self._partition_to_batch(mat_result.micropartition())
                if batch is not None:
                    yield batch
        else:
            partitions_iter: Iterator[MaterializedResult[Any]] = get_or_create_runner().run_iter(
                self._batched_df._builder, results_buffer_size=self._prefetch_count
            )
            for mat_result in partitions_iter:
                batch = self._partition_to_batch(mat_result.micropartition())
                if batch is not None:
                    yield batch

    def _partition_to_batch(self, partition: MicroPartition) -> dict[str, Any] | None:
        if self._drop_last and len(partition) < self._batch_size:
            return None
        return self._to_torch_batch(partition.to_pydict())

    def _to_torch_batch(self, pydict: dict[str, list[Any]]) -> dict[str, Any]:
        return {key: self._column_to_tensor(values) for key, values in pydict.items()}

    def _column_to_tensor(self, values: list[Any]) -> Any:
        if len(values) == 0:
            return self._pin(torch.tensor([]))

        first = values[0]

        if isinstance(first, torch.Tensor):
            return self._pin(torch.stack(values))
        if hasattr(first, "__array__") and not isinstance(first, (str, bytes)):
            import numpy as np

            if isinstance(first, np.ndarray) and first.ndim > 0:
                return self._pin(torch.stack([torch.as_tensor(v) for v in values]))
            return self._pin(torch.as_tensor(values))
        if isinstance(first, (bool, int, float)):
            return self._pin(torch.as_tensor(values))

        return values

    def _pin(self, tensor: torch.Tensor) -> torch.Tensor:
        if not self._pin_memory:
            return tensor

        # Pinned host memory is only used for async CPU -> CUDA copies.
        if not torch.cuda.is_available():
            return tensor

        # If a specific device is provided, use it. Otherwise, use the default device.
        if self._pin_memory_device:
            return tensor.pin_memory(device=self._pin_memory_device)
        return tensor.pin_memory()
