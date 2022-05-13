from typing import Dict, List, Generic, TypeVar, Optional

import ray
import pyarrow as pa

from daft.datarepo import metadata_service


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
        ray_dataset: ray.data.Dataset,
    ):
        """Creates a new Datarepo

        Args:
            datarepo_id (str): ID of the datarepo
            ray_dataset (ray.data.Dataset): Dataset that backs this Datarepo
        """
        self._id = datarepo_id
        self._ray_dataset = ray_dataset

    def map(self, func: MapFunc[Item, OutputItem]) -> "Datarepo[OutputItem]":
        """Runs a function on each item in the Datarepo, returning a new Datarepo

        Args:
            func (MapFunc[Item, OutputItem]): function to run

        Returns:
            Datarepo[OutputItem]: Datarepo of outputs
        """
        return Datarepo(self._id + "[mapped]", self._ray_dataset.map(func))

    def save(self, datarepo_id: str) -> None:
        """Save a datarepo to persistent storage

        Args:
            datarepo_id (str): ID to save datarepo as
        """
        path = metadata_service.get_metadata_service().get_path(datarepo_id)
        return self._ray_dataset.write_parquet(path)

    def info(self) -> DatarepoInfo:
        """Retrieves information about the Datarepo. This method never triggers any
        recomputation, but may return <unknown> values if the Datarepo has not been
        materialized yet.

        Returns:
            DatarepoInfo: dictionary of information about the Datarepo
        """
        return {
            "id": self._id,
            "num_rows": self._ray_dataset.count(),
        }

    def sample(self, n: int = 5) -> "Datarepo[Item]":
        """Samples a certain number of Items from the Datarepo

        Args:
            n (int, optional): number of items to sample. Defaults to 5.

        Returns:
            Datarepo[Item]: _description_
        """
        # TODO(jaychia): This likely triggers a computation, we might need to find
        # a way to make this lazy, perhaps by building out our own queue of operations?
        head, _ = self._ray_dataset.split_at_indices([n])
        return Datarepo(self._id + "[sampled]", head)

    def preview(self) -> None:
        """Previews the data in a Datarepo, triggering the computation of that data

        TODO(jaychia): This needs further refinement to display according to our schema
        """
        from IPython.display import display
        items = ray.get(self._ray_dataset.get_internal_block_refs()[0])

        # HACK(jaychia): In the absence of schemas, we just detect the types dynamically
        if isinstance(items, pa.Table):
            display(items.to_pandas())
        elif isinstance(items, list):
            for item in items:
                print(item)
        else:
            print(f"Visualization not supported: {type(items)}")

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
        return cls(datarepo_id, ray.data.read_parquet(path))
