from __future__ import annotations

import dataclasses
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
)

import pyarrow as pa
import ray
import ray.data.dataset_pipeline
from ray.data.impl.arrow_block import ArrowRow

from daft.dataclasses import _patch_class_for_deserialization
from daft.datarepo import metadata_service

# TODO(jaychia): We should derive these in a smarter way, derived from number of CPUs or GPUs?
DEFAULT_ACTOR_STRATEGY: Callable[[], ray.data.ActorPoolStrategy] = lambda: ray.data.ActorPoolStrategy(
    min_size=1,
    max_size=ray.cluster_resources()["CPU"],
)
DatarepoInfo = Dict[str, str]


Item = TypeVar("Item")
OutputItem = TypeVar("OutputItem")

BatchItem = List[Item]
BatchOutputItem = List[OutputItem]

CallableClass = type
MapFunc = Union[CallableClass, Callable[[Item], OutputItem]]
BatchMapFunc = Union[CallableClass, Callable[[Union[BatchItem, Item]], BatchOutputItem]]


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
        body = "\n".join([f"\t{k}={v}" for k, v in self.info().items()])
        return f"""<DataRepo:{body}>"""

    def map(
        self,
        func: MapFunc[Item, OutputItem],
    ) -> Datarepo[OutputItem]:
        """Runs a function on each item in the Datarepo, returning a new Datarepo

        Args:
            func (MapFunc[Item, OutputItem]): function to run

        Returns:
            Datarepo[OutputItem]: Datarepo of outputs
        """
        return Datarepo(
            datarepo_id=f"{self._id}:map[{func.__name__}]",
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
    ) -> Datarepo[OutputItem]:
        """Runs a function on batches of items in the Datarepo, returning a new Datarepo

        Args:
            batched_func (MapFunc[Iterator[Item], Iterator[OutputItem]]): a function that runs on a batch of input
                data, and outputs a batch of output data

        Returns:
            Datarepo[OutputItem]: Datarepo of outputs
        """
        ray_dataset: ray.data.Dataset[OutputItem] = self._ray_dataset.map_batches(
            batched_func,
            batch_size=batch_size,
            compute=_get_compute_strategy(batched_func),
            batch_format="native",
        )

        return Datarepo(
            datarepo_id=f"{self._id}:map_batches[{batched_func.__name__}]",
            ray_dataset=ray_dataset,
        )

    def sample(self, n: int = 5) -> Datarepo[Item]:
        """Computes and samples `n` Items from the Datarepo

        Args:
            n (int, optional): number of items to sample. Defaults to 5.

        Returns:
            Datarepo[Item]: new Datarepo with sampled size
        """
        head, _ = self._ray_dataset.split_at_indices([n])
        return Datarepo(
            datarepo_id=f"{self._id}:sample[{n}]",
            ray_dataset=head,
        )

    def take(self, n: int = 5) -> Union[List[Item], List[ArrowRow]]:
        """Takes `n` Items from the Datarepo and returns a list

        Args:
            n (int, optional): number of items to take. Defaults to 5.

        Returns:
            List[Item]: list of items
        """
        retrieved: List[Item] = []
        for i, row in enumerate(self._ray_dataset.iter_rows()):
            retrieved.append(row)
            if i >= n - 1:
                break
        return retrieved

    def filter(self, func: MapFunc[Item, bool]) -> Datarepo[Item]:
        """Filters the Datarepo using a function that returns a boolean indicating whether to keep or discard an item

        Args:
            func (MapFunc[Item, bool]): function to filter with

        Returns:
            Datarepo[Item]: filtered Datarepo
        """
        return Datarepo(
            datarepo_id=f"{self._id}:filter[{func.__name__}]]",
            # Type failing because Ray mistakenly requests for Optional[str]
            ray_dataset=self._ray_dataset.filter(func, compute=_get_compute_strategy(func)),  # type: ignore
        )

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

        # sample_item = self._ray_dataset.take(1)

        # if len(sample_item) == 0:
        #     print("nothing to save")
        #     return None

        # _patch_class_for_deserialization(sample_item[0].__class__)

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

    def show(self, n: int = 1) -> None:
        """Previews the data in a Datarepo"""
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
    def from_id(
        cls,
        datarepo_id: str,
        data_type: Optional[Type[Item]] = None,
        partitions: Optional[int] = None,
        svc: Optional[metadata_service._DatarepoMetadataService] = None,
    ) -> Datarepo[Item]:
        """Gets a Datarepo by ID

        Args:
            datarepo_id (str): ID of the datarepo
            data_type (Optional[Type[Item]], optional): Dataclass of the type of data. Defaults to None.
            partitions (Optional[int], optional): number of partitions to split data into. Defaults to None.
            svc (Optional[metadata_service._DatarepoMetadataService], optional): Defaults to None which will detect
                the appropriate service to use from the current environment.

        Returns:
            Datarepo: retrieved Datarepo
        """
        if data_type is not None:
            assert dataclasses.is_dataclass(data_type) and isinstance(data_type, type)
            assert hasattr(data_type, "_daft_schema"), f"{data_type} was not initialized with daft dataclass"
            daft_schema = getattr(data_type, "_daft_schema")
            # _patch_class_for_deserialization(data_type)
            # if getattr(data_type, "__daft_patched", None) != id(dataclasses._FIELD):
            #     assert dataclasses.is_dataclass(data_type) and isinstance(data_type, type)
            #     fields = data_type.__dict__["__dataclass_fields__"]
            #     for field in fields.values():
            #         if type(field._field_type) is type(dataclasses._FIELD):
            #             field._field_type = dataclasses._FIELD
            #     setattr(data_type, "__daft_patched", id(dataclasses._FIELD))

            def deserialize(items) -> List[Item]:
                block: List[Item] = daft_schema.deserialize_batch(items, data_type)
                return block

        if svc is None:
            svc = metadata_service.get_metadata_service()
        path = svc.get_path(datarepo_id)

        ds = ray.data.read_parquet(path)

        if partitions is not None:
            ds = ds.repartition(partitions, shuffle=True)

        if data_type is not None:
            ds = ds.map_batches(deserialize, batch_format="pyarrow")

        return cls(
            datarepo_id=datarepo_id,
            ray_dataset=ds,
        )


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
