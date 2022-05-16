from __future__ import annotations

import dataclasses
import io

from typing import Any, Dict, List, Generic, TypeVar, Callable, Union

import ray
import ray.data.dataset_pipeline
from ray.data.impl.arrow_block import ArrowRow

from daft.datarepo import metadata_service

# TODO(jaychia): We should derive these in a smarter way, derived from number of CPUs or GPUs?
MIN_ACTORS = 8
MAX_ACTORS = 8
DEFAULT_ACTOR_STRATEGY: ray.data.ActorPoolStrategy  = ray.data.ActorPoolStrategy(MIN_ACTORS, MAX_ACTORS)
DatarepoInfo = Dict[str, str]


Item = TypeVar("Item")
OutputItem = TypeVar("OutputItem")

BatchItem = List[Item]
BatchOutputItem = List[OutputItem]

MapFunc = Callable[[Item], OutputItem]

BatchMapFunc = Union[type, Callable[[Any], Any]]

class Datarepo(Generic[Item]):
    """Implements Datarepos, which are repositories of Items of data.

    Datarepos are unordered collections of Items. In terms of datastructures, they are roughly
    analogous to Sets, but provide a rich interactive interface for working with large (millions+)
    sets of Items. Datarepos are built for interactive computing through a REPL such as a notebook
    environment, utilizing Eventual's compute engine for manipulating these large collections of data.
    
    Datarepos provide methods to manipulate Items, including:

        `.map`:     Runs a function on each Item, and the outputs form a new Datarepo
        `.sample`:  Retrieves a subset of Items as a new Datarepo

    Additionally, Datarepos provide an interactive experience to working with Items to aid rapid visualization:

        `.preview`:    Visualize the top N number of Items in the current notebook
    """

    def __init__(
        self,
        datarepo_id: str,
        ray_dataset: ray.data.Dataset,
    ):
        """Creates a new Datarepo

        Args:
            datarepo_id (str): ID of the datarepo
            ray_dataset (ray.data.Dataset): Dataset that backs this Datarepo
        """
        self._id = datarepo_id
        self._ray_dataset = ray_dataset

    def info(self) -> DatarepoInfo:
        """Retrieves information about the Datarepo. This method never triggers any
        recomputation, but may return <unknown> values if the Datarepo has not been
        materialized yet.

        Returns:
            DatarepoInfo: dictionary of information about the Datarepo
        """
        return {
            "id": self._id,
            "num_rows": str(self._ray_dataset.count()),
        }

    def __repr__(self) -> str:
        return f"<DataRepo: {' '.join([f'{k}={v}' for k, v in self.info().items()])}>"

    ###
    # Operation Functions: Functions that add to the graph of operations to perform
    ###

    def map(self, func: MapFunc[Item, OutputItem]) -> "Datarepo[OutputItem]":
        """Runs a function on each item in the Datarepo, returning a new Datarepo

        Args:
            func (MapFunc[Item, OutputItem]): function to run

        Returns:
            Datarepo[OutputItem]: Datarepo of outputs
        """
        return Datarepo(
            datarepo_id=f"{self._id}:map[{func.__name__}]",
            ray_dataset=self._ray_dataset.map(func),
        )

    def map_batches(
        self,
        batched_func: BatchMapFunc,
        batch_size: int,
    ) -> Datarepo[OutputItem]:
        """Runs a function on batches of items in the Datarepo, returning a new Datarepo

        Args:
            batched_func (MapFunc[Iterator[Item], Iterator[OutputItem]]): a function that runs on a batch of input
                data, and outputs a batch of output data

        Returns:
            Datarepo[OutputItem]: Datarepo of outputs
        """
        compute_strategy: Union[str, ray.data.ActorPoolStrategy] = (
            "tasks"
            # HACK(jaychia): hack until we define proper types for our Function decorators
            if str(type(batched_func)) == "<class 'function'>"
            else DEFAULT_ACTOR_STRATEGY
        )


        ray_dataset: ray.data.Dataset[OutputItem] = self._ray_dataset.map_batches(
            # TODO(jaychia): failing typecheck for some reason:
            batched_func,
            batch_size=batch_size,
            compute=compute_strategy,
            batch_format="native",
        )

        return Datarepo(
            datarepo_id=f"{self._id}:map_batches[{batched_func.__name__}]",
            ray_dataset=ray_dataset
        )

    def sample(self, n: int = 5) -> "Datarepo[Item]":
        """Computes and samples `n` Items from the Datarepo

        Args:
            n (int, optional): number of items to sample. Defaults to 5.

        Returns:
            Datarepo[Item]: new Datarepo with sampled size
        """
        # TODO(jaychia): This forces the full execution of the Dataset before splitting, which is not good.
        # We might be able to get around it by converting the Dataset to a pipeline first, with blocks_per_window=1
        # and then only accessing the first window. Unclear, needs to be benchmarked.
        head, _ = self._ray_dataset.split_at_indices([n])
        return Datarepo(
            datarepo_id=f"{self._id}:sample[{n}]",
            ray_dataset=head,
        )

    ###
    # Trigger functions: Functions that trigger computation of the Datarepo
    ###

    def save(self, datarepo_id: str) -> None:
        """Save a datarepo to persistent storage

        Args:
            datarepo_id (str): ID to save datarepo as
        """
        # TODO(jaychia): Serialize dataclasses to arrow-compatible types properly with schema library
        import PIL.Image
        def TODO_serialize(item):
            if dataclasses.is_dataclass(item):
                d = {}
                for field in item.__dataclass_fields__:
                    val = getattr(item, field)
                    if isinstance(val, PIL.Image.Image):
                        bio = io.BytesIO()
                        val.save(bio, format="JPEG")
                        d[field] = bio.getvalue()
                    else:
                        d[field] = val
                return d
            else:
                raise NotImplementedError("Can only save Daft Dataclasses to Datarepos")

        path = metadata_service.get_metadata_service().get_path(datarepo_id)
        return self._ray_dataset.map(TODO_serialize).write_parquet(path)

    def preview(self, n: int = 1) -> None:
        """Previews the data in a Datarepo"""
        sampled_repo = self.sample(n)

        from IPython.display import display  # type: ignore
        import PIL.Image
        for i, item in enumerate(sampled_repo._ray_dataset.iter_rows()):
            if i >= n:
                break
            if isinstance(item, ArrowRow):
                for col_name in item:
                    print(f"{col_name}: {item[col_name]}")
            elif dataclasses.is_dataclass(item):
                # TODO(jaychia): This needs further refinement for rich display according
                # to our schema when @sammy is ready with the schema library, by checking
                # if the item is an instance of a Daft Dataclass.
                for field, val in dataclasses.asdict(item).items():
                    if isinstance(val, PIL.Image.Image):
                        print(f"{field}:")
                        display(val)
                    else:
                        print(f"{field}: {val}")

    ###
    # Static methods: Managing Datarepos
    ###

    @staticmethod
    def list_ids() -> List[str]:
        """List the IDs of all materialized datarepos

        Returns:
            List[str]: IDs of datarepos
        """
        return metadata_service.get_metadata_service().list_ids()

    @classmethod
    def get(cls, datarepo_id: str) -> "Datarepo":
        """Gets a Datarepo by ID

        Args:
            datarepo_id (str): ID of the datarepo

        Returns:
            Datarepo: retrieved Datarepo
        """
        path = metadata_service.get_metadata_service().get_path(datarepo_id)
        ds = ray.data.read_parquet(path)
        # NOTE(jaychia): ds.count() is supposedly O(1) for parquet formats:
        # https://github.com/ray-project/ray/blob/master/python/ray/data/dataset.py#L1640
        # But we should benchmark and verify this.
        return cls(
            datarepo_id=datarepo_id,
            ray_dataset=ds,
        )
