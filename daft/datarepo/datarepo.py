from typing import Dict, List, Generic, TypeVar


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

    def __init__(self):
        pass

    def map(self, func: MapFunc[Item, OutputItem]) -> "Datarepo[OutputItem]":
        pass

    def save(self, datarepo_id: str) -> "Datarepo[str]":
        pass

    def info(self) -> DatarepoInfo:
        pass

    def sample(self, n: int = 5) -> "Datarepo[Item]":
        pass

    def preview(self) -> None:
        pass

    ###
    # Static methods: Managing Datarepos
    ###

    @staticmethod
    def list_ids() -> List[str]:
        """List the IDs of all materialized datarepos

        Returns:
            List[str]: IDs of datarepos
        """        
        return ["foo", "bar", "baz"]

    @staticmethod
    def get(datarepo_id: str) -> "Datarepo":
        """Gets a Datarepo by ID

        Args:
            datarepo_id (str): ID of the datarepo

        Returns:
            Datarepo: retrieved Datarepo
        """        
        return Datarepo()
