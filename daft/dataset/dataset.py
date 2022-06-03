from __future__ import annotations

import dataclasses
from typing import Callable, Dict, Generic, List, Literal, TypeVar, Union

import pyarrow as pa
import ray
import ray.data.dataset_pipeline
from ray.data.impl.arrow_block import ArrowRow
from ray.data.row import TableRow

from daft.dataclasses import _patch_class_for_deserialization
from daft.dataset.read_api import read_datarepo

# TODO(jaychia): We should derive these in a smarter way, derived from number of CPUs or GPUs?
DEFAULT_ACTOR_STRATEGY: Callable[[], ray.data.ActorPoolStrategy] = lambda: ray.data.ActorPoolStrategy(
    min_size=1,
    max_size=ray.cluster_resources()["CPU"],
)
DatasetInfo = Dict[str, str]


Item = TypeVar("Item")
OutputItem = TypeVar("OutputItem")

BatchItem = List[Item]
BatchOutputItem = List[OutputItem]

CallableClass = type
MapFunc = Union[CallableClass, Callable[[Item], OutputItem]]
BatchMapFunc = Union[CallableClass, Callable[[Union[BatchItem, Item]], BatchOutputItem]]


class Dataset(Generic[Item]):
    """Implements Datasets, which are repositories of Items of data.

    Datasets are unordered collections of Items. In terms of datastructures, they are roughly
    analogous to Sets, but provide a rich interactive interface for working with large (millions+)
    sets of Items. Datasets are built for interactive computing through a REPL such as a notebook
    environment, utilizing Eventual's compute engine for manipulating these large collections of data.

    Datasets provide methods to manipulate Items, including:

        `.map`:     Runs a function on each Item, and the outputs form a new Dataset
        `.sample`:  Retrieves a subset of Items as a new Dataset

    Additionally, Datasets provide an interactive experience to working with Items to aid rapid visualization:

        `.preview`:    Visualize the top N number of Items in the current notebook
    """

    def __init__(
        self,
        dataset_id: str,
        ray_dataset: ray.data.Dataset[Item],
    ):
        """Creates a new Dataset

        Args:
            dataset_id (str): ID of the dataset
            ray_dataset (ray.data.Dataset): Dataset that backs this Dataset
        """
        self._id = dataset_id
        self._ray_dataset = ray_dataset

    def info(self) -> DatasetInfo:
        """Retrieves information about the Dataset. This method never triggers any
        recomputation, but may return <unknown> values if the Dataset has not been
        materialized yet.

        Returns:
            DatasetInfo: dictionary of information about the Dataset
        """
        return {
            "id": self._id,
            "num_rows": str(self._ray_dataset.count()),
        }

    def __repr__(self) -> str:
        body = "\n".join([f" {k}={v}" for k, v in self.info().items()])
        return f"""<DataRepo:\n{body}>"""

    def map(
        self,
        func: MapFunc[Item, OutputItem],
    ) -> Dataset[OutputItem]:
        """Runs a function on each item in the Dataset, returning a new Dataset

        Args:
            func (MapFunc[Item, OutputItem]): function to run

        Returns:
            Dataset[OutputItem]: Dataset of outputs
        """
        return Dataset(
            dataset_id=f"{self._id}:map[{func.__name__}]",
            ray_dataset=self._ray_dataset.map(
                func,
                # Type failing because Ray mistakenly requests for Optional[str]
                compute=_get_compute_strategy(func),  # type: ignore
            ),
        )

    def map_batches(
        self,
        batched_func: BatchMapFunc,
        batch_size: int,
    ) -> Dataset[OutputItem]:
        """Runs a function on batches of items in the Dataset, returning a new Dataset

        Args:
            batched_func (MapFunc[Iterator[Item], Iterator[OutputItem]]): a function that runs on a batch of input
                data, and outputs a batch of output data

        Returns:
            Dataset[OutputItem]: Dataset of outputs
        """
        ray_dataset: ray.data.Dataset[OutputItem] = self._ray_dataset.map_batches(
            batched_func,
            batch_size=batch_size,
            compute=_get_compute_strategy(batched_func),
            batch_format="native",
        )

        return Dataset(
            dataset_id=f"{self._id}:map_batches[{batched_func.__name__}]",
            ray_dataset=ray_dataset,
        )

    def sample(self, n: int = 5) -> Dataset[Item]:
        """Computes and samples `n` Items from the Dataset

        Args:
            n (int, optional): number of items to sample. Defaults to 5.

        Returns:
            Dataset[Item]: new Dataset with sampled size
        """
        head, _ = self._ray_dataset.split_at_indices([n])
        return Dataset(
            dataset_id=f"{self._id}:sample[{n}]",
            ray_dataset=head,
        )

    def take(self, n: int = 5) -> List[Union[Item, TableRow]]:
        """Takes `n` Items from the Dataset and returns a list

        Args:
            n (int, optional): number of items to take. Defaults to 5.

        Returns:
            List[Item]: list of items
        """
        retrieved: List[Union[Item, TableRow]] = []
        for i, row in enumerate(self._ray_dataset.iter_rows()):
            retrieved.append(row)
            if i >= n - 1:
                break
        return retrieved

    def filter(self, func: MapFunc[Item, bool]) -> Dataset[Item]:
        """Filters the Dataset using a function that returns a boolean indicating whether to keep or discard an item

        Args:
            func (MapFunc[Item, bool]): function to filter with

        Returns:
            Dataset[Item]: filtered Dataset
        """
        return Dataset(
            dataset_id=f"{self._id}:filter[{func.__name__}]]",
            # Type failing because Ray mistakenly requests for Optional[str]
            ray_dataset=self._ray_dataset.filter(func, compute=_get_compute_strategy(func)),  # type: ignore
        )

    def show(self, n: int = 1) -> None:
        """Previews the data in a Dataset"""
        items = self.take(n)

        import PIL.Image
        from IPython.display import display  # type: ignore

        for i, item in enumerate(items):
            if i >= n:
                break
            if isinstance(item, ArrowRow):
                for col_name in item:
                    print(f"{col_name}: {item[col_name]}")
            elif dataclasses.is_dataclass(item):
                # TODO(jaychia): This needs further refinement for rich display according
                # to our schema when @sammy is ready with the schema library, by checking
                # if the item is an instance of a Daft Dataclass.
                for field, val in item.__dict__.items():
                    if isinstance(val, PIL.Image.Image):
                        print(f"{field}:")
                        display(val)
                    else:
                        print(f"{field}: {val}")
            else:
                display(item)


def _get_compute_strategy(func: Callable[[Item], OutputItem]) -> Union[Literal["tasks"], ray.data.ActorPoolStrategy]:
    """Returns the appropriate compute_strategy when given a callable.

    Callables can either be a Function, or a Class which defines an .__init__() and a .__call__()
    We handle both cases by constructing the appropriate compute_strategy and passing that to Ray.

    Args:
        func: Callable to evaluate

    Returns:
        Union[str, ray.data.ActorPoolStrategy]: Either the string "tasks" or a ray.data.ActorPoolStrategy
    """
    return DEFAULT_ACTOR_STRATEGY() if isinstance(func, type) else "tasks"
