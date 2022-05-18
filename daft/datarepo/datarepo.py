from __future__ import annotations

import dataclasses
from typing import Callable, Dict, Generic, List, Optional, Type, TypeVar, Union

import pyarrow as pa
import ray
import ray.data.dataset_pipeline
from ray.data.impl.arrow_block import ArrowRow

from daft.datarepo import metadata_service

# TODO(jaychia): We should derive these in a smarter way, derived from number of CPUs or GPUs?
MIN_ACTORS = 8
MAX_ACTORS = 8
DEFAULT_ACTOR_STRATEGY: ray.data.ActorPoolStrategy = ray.data.ActorPoolStrategy(MIN_ACTORS, MAX_ACTORS)
DatarepoInfo = Dict[str, str]


Item = TypeVar("Item")
OutputItem = TypeVar("OutputItem")

BatchItem = List[Item]
BatchOutputItem = List[OutputItem]

MapFunc = Callable[[Item], OutputItem]

BatchMapFunc = Union[type, Callable[[Union[BatchItem, Item]], BatchOutputItem]]


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
        ray_dataset: ray.data.Dataset[Item],
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

    def map(self, func: MapFunc[Item, OutputItem]) -> Datarepo[OutputItem]:
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

        return Datarepo(datarepo_id=f"{self._id}:map_batches[{batched_func.__name__}]", ray_dataset=ray_dataset)

    def sample(self, n: int = 5) -> Datarepo[Item]:
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

    def save(
        self,
        datarepo_id: str,
        svc: Optional[metadata_service._DatarepoMetadataService] = None,
    ) -> None:
        """Save a datarepo to persistent storage

        Args:
            datarepo_id (str): ID to save datarepo as
        """
        if svc is None:
            svc = metadata_service.get_metadata_service()

        def serialize(items: List[Item]) -> pa.Table:
            if len(items) == 0:
                return None
            first_type = items[0].__class__
            assert dataclasses.is_dataclass(first_type), "We can only serialize daft dataclasses"
            assert hasattr(first_type, "_daft_schema"), "was not initialized with daft dataclass"
            daft_schema = getattr(first_type, "_daft_schema")
            return daft_schema.serialize(items)

        path = svc.get_path(datarepo_id)
        serialized_ds: ray.data.Dataset[pa.Table] = self._ray_dataset.map_batches(serialize)  # type: ignore
        return serialized_ds.write_parquet(path)

    def preview(self, n: int = 1) -> None:
        """Previews the data in a Datarepo"""
        sampled_repo = self.sample(n)

        import PIL.Image
        from IPython.display import display  # type: ignore

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
    def list_ids(
        svc: Optional[metadata_service._DatarepoMetadataService] = None,
    ) -> List[str]:
        """List the IDs of all materialized datarepos
        Args:
            svc (Optional[metadata_service._DatarepoMetadataService], optional): Defaults to None which will detect
                the appropriate service to use from the current environment.

        Returns:
            List[str]: IDs of datarepos
        """
        if svc is None:
            svc = metadata_service.get_metadata_service()
        return svc.list_ids()

    @classmethod
    def get(
        cls,
        datarepo_id: str,
        data_type: Type[Item],
        svc: Optional[metadata_service._DatarepoMetadataService] = None,
    ) -> Datarepo[Item]:
        """Gets a Datarepo by ID

        Args:
            datarepo_id (str): ID of the datarepo
            svc (Optional[metadata_service._DatarepoMetadataService], optional): Defaults to None which will detect
                the appropriate service to use from the current environment.

        Returns:
            Datarepo: retrieved Datarepo
        """

        assert dataclasses.is_dataclass(data_type) and isinstance(data_type, type)
        assert hasattr(data_type, "_daft_schema"), f"{data_type} was not initialized with daft dataclass"
        daft_schema = getattr(data_type, "_daft_schema")

        if svc is None:
            svc = metadata_service.get_metadata_service()
        path = svc.get_path(datarepo_id)

        ds = ray.data.read_parquet(path)

        def deserialize(items) -> List[Item]:
            block: List[Item] = daft_schema.deserialize_batch(items, data_type)
            return block

        deserialized: ray.data.Dataset[Item] = ds.map_batches(deserialize, batch_format="pyarrow")

        # NOTE(jaychia): ds.count() is supposedly O(1) for parquet formats:
        # https://github.com/ray-project/ray/blob/master/python/ray/data/dataset.py#L1640
        # But we should benchmark and verify this.
        return cls(
            datarepo_id=datarepo_id,
            ray_dataset=deserialized,
        )
