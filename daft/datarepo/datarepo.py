import dataclasses

from typing import Dict, List, Generic, TypeVar, Optional

import ray
import ray.data.dataset_pipeline
from ray.data.impl.arrow_block import ArrowRow
import pyarrow as pa

from daft.datarepo import metadata_service

# TODO(jaychia): We should derive this in a smarter way, perhaps N_CPUs * 2 or something similar.
BLOCKS_PER_WINDOW = 8
DatarepoInfo = Dict[str, str]


Item = TypeVar("Item")
OutputItem = TypeVar("OutputItem")

class MapFunc(Generic[Item, OutputItem]):
    """Function to be used to map over a Datarepo"""

    def __call__(self, item: Item) -> OutputItem:
        ...

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
        ray_dataset_pipeline: ray.data.dataset_pipeline.DatasetPipeline,
        num_rows: Optional[int] = None,
    ):
        """Creates a new Datarepo

        Args:
            datarepo_id (str): ID of the datarepo
            ray_dataset (ray.data.Dataset): Dataset that backs this Datarepo
        """
        self._id = datarepo_id
        self._ray_dataset_pipeline = ray_dataset_pipeline
        self._num_rows = num_rows

    def map(self, func: MapFunc[Item, OutputItem]) -> "Datarepo[OutputItem]":
        """Runs a function on each item in the Datarepo, returning a new Datarepo

        Args:
            func (MapFunc[Item, OutputItem]): function to run

        Returns:
            Datarepo[OutputItem]: Datarepo of outputs
        """
        return Datarepo(
            datarepo_id=f"{self._id}:{func.__name__}",
            ray_dataset_pipeline=self._ray_dataset_pipeline.map(func),
            num_rows=self._num_rows,
        )

    def save(self, datarepo_id: str) -> None:
        """Save a datarepo to persistent storage

        Args:
            datarepo_id (str): ID to save datarepo as
        """
        path = metadata_service.get_metadata_service().get_path(datarepo_id)
        return self._ray_dataset_pipeline.write_parquet(path)

    def info(self) -> DatarepoInfo:
        """Retrieves information about the Datarepo. This method never triggers any
        recomputation, but may return <unknown> values if the Datarepo has not been
        materialized yet.

        Returns:
            DatarepoInfo: dictionary of information about the Datarepo
        """
        return {
            "id": self._id,
            "num_rows": self._num_rows,
        }

    def sample(self, n: int = 5) -> "Datarepo[Item]":
        """Computes and samples `n` Items from the Datarepo

        Args:
            n (int, optional): number of items to sample. Defaults to 5.

        Returns:
            Datarepo[Item]: new Datarepo with sampled size
        """
        # TODO(jaychia): This triggers computation of datasets, which is not ideal
        count = 0
        datasets = []
        for ds in self._ray_dataset_pipeline.iter_datasets():
            datasets.append(ds)
            count += ds.count()
            if count > n:
                minimum_dataset = datasets[0] if len(datasets) == 1 else datasets[0].union(datasets[1:])
                sampled_dataset, _ = minimum_dataset.split_at_indices([n])
                return Datarepo(
                    datarepo_id=f"{self._id}:sampled({n})",
                    ray_dataset_pipeline=sampled_dataset.window(blocks_per_window=BLOCKS_PER_WINDOW),
                    num_rows=n,
                )
        raise RuntimeError(f"Attempting to sample {n} items but the Datarepo only has {count} items")

    def preview(self, n: int = 1) -> None:
        """Previews the data in a Datarepo.

        TODO(jaychia): This triggers the computation of the first window of the pipeline,
        which will cause errors on subsequent usage of the pipeline. This behavior needs
        to be fixed! Ideally we should cache the results of the first window's execution and
        just continue executing the rest.
        """
        from IPython.display import display
        import PIL.Image
        # .iter_rows() of the Ray DatasetPipeline, processes windows lazily as they are requested.
        # If we request a low number of rows for the preview, then we will process a minimal number
        # of windows - ideally just one!
        for i, item in enumerate(self._ray_dataset_pipeline.iter_rows()):
            if i >= n:
                break
            if isinstance(item, ArrowRow):
                for col_name in item:
                    print(f"{col_name}: {item[col_name]}")
            elif dataclasses.is_dataclass(item):
                # TODO(jaychia): This needs further refinement for rich display according
                # to our schema when @sammy is ready with the schema library, by checking
                # if the item is an instance of a Daft Dataclass.
                for field in item.__dataclass_fields__:
                    val = getattr(item, field)
                    if isinstance(val, PIL.Image.Image):
                        print(f"{field}:")
                        display(val)
                    else:
                        print(f"{field}: {val}")

    def __repr__(self) -> str:
        return str(self.info())

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
        ds_len = ds.count()
        pipeline = ds.window(blocks_per_window=BLOCKS_PER_WINDOW)
        return cls(
            datarepo_id,
            pipeline,
            num_rows=ds_len,
        )
