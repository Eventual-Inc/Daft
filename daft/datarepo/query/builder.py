from __future__ import annotations
import dataclasses
from daft.dataclasses import DataclassBuilder

from daft.datarepo.log import DaftLakeLog
import networkx as NX
import ray

from daft.datarepo.query import stages
from daft.datarepo.query.definitions import NodeId, QueryColumn, Comparator, WriteDatarepoStageOutput
from daft.datarepo.query import functions as F

from typing import Literal, Type, Tuple, List, Union, cast


@dataclasses.dataclass(frozen=True)
class _QueryColumnSchema:
    name: QueryColumn
    type: Type


class DatarepoQueryBuilder:
    def __init__(
        self,
        query_tree: NX.DiGraph,
        root: str,
        current_dataclass: Type,
    ) -> None:
        # The Query is structured as a Query Tree
        # 1. There is one root, which holds the entire query
        # 2. Leaf nodes are stages that read from other queries or Datarepos
        # 3. Other nodes are Operations that process data
        # 4. Edges hold a "key" attribute which defines the name of the input to a given operation
        self._query_tree = query_tree
        self._root = root
        self._current_dataclass = current_dataclass

    @classmethod
    def _from_datarepo_log(cls, daft_lake_log: DaftLakeLog, dtype: Type) -> DatarepoQueryBuilder:
        """Initializes a DatarepoQueryBuilder with the root node being a GetDatarepoStage

        This should not be called by users. Users should access queries through the Datarepo.query() API.

        Args:
            daft_lake_log (DaftLakeLog): log of the Datarepo to query
            dtype (Type): Dataclass to query

        Returns:
            DatarepoQueryBuilder: initialized DatarepoQueryBuilder
        """
        tree = NX.DiGraph()
        stage = stages.GetDatarepoStage(
            daft_lake_log=daft_lake_log,
            dtype=dtype,
            read_limit=None,
            filters=None,
        )
        node_id, tree = stage.add_root(tree, "")
        return cls(
            query_tree=tree,
            root=node_id,
            current_dataclass=dtype,
        )

    def where(self, column: QueryColumn, operation: Comparator, value: Union[str, float, int]):
        """Filters the query with a simple predicate.

        Args:
            column (QueryColumn): column to filter on
            operation (Comparator): operation to compare the column with
            value (Union[str, float, int]): value to compare the column with
        """
        stage = stages.WhereStage(column, operation, value)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id, current_dataclass=self._current_dataclass)

    def with_column(self, new_column: QueryColumn, expr: F.QueryExpression) -> DatarepoQueryBuilder:
        """Creates a new column with value derived from the specified expression"""
        dataclass_builder = DataclassBuilder.from_class(self._current_dataclass)
        dataclass_builder.add_field(name=new_column, dtype=expr.return_type)
        new_dataclass = dataclass_builder.generate()
        stage = stages.WithColumnStage(new_column=new_column, expr=expr, dataclass=new_dataclass)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id, current_dataclass=new_dataclass)

    def limit(self, limit: int) -> DatarepoQueryBuilder:
        """Limits the number of rows in the query"""
        stage = stages.LimitStage(limit=limit)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id, current_dataclass=self._current_dataclass)

    def write_datarepo(
        self,
        datarepo_path: str,
        datarepo_name: str,
        mode: Union[Literal["append"], Literal["overwrite"]] = "append",
        rows_per_partition: int = 1024,
    ) -> DatarepoQueryBuilder:
        """Writes the query to a datarepo, returning the results of the write"""
        stage = stages.WriteDatarepoStage(
            datarepo_name=datarepo_name,
            datarepo_path=datarepo_path,
            mode=mode,
            rows_per_partition=rows_per_partition,
            dtype=self._current_dataclass,
        )
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(
            query_tree=tree,
            root=node_id,
            current_dataclass=WriteDatarepoStageOutput,
        )

    def execute(self) -> ray.data.Dataset:
        """Executes the query and returns it as a Daft dataset"""
        ds = _execute_query_tree(self._query_tree, self._root)
        return ds

    def __repr__(self) -> str:
        fields = "\n".join([f"  {field.name}: {field.type}" for field in dataclasses.fields(self._current_dataclass)])
        stages = [self._query_tree.nodes[node_id]["stage"] for node_id in NX.dfs_tree(self._query_tree)]
        stages_repr = "\n|\n".join([stage.__repr__() for stage in stages])
        return f"""class {self._current_dataclass.__name__}:
{fields}

Query Plan
----------
{stages_repr}
"""


def _execute_query_tree(tree: NX.DiGraph, root: NodeId) -> ray.data.Dataset:
    """Executes the stages in a query tree recursively in a DFS

    Args:
        tree (NX.DiGraph): query tree to execute
        root (NodeId): root of the query tree to execute

    Returns:
        ray.data.Dataset: Dataset obtained from the execution
    """
    stage: stages.QueryStage = tree.nodes[root]["stage"]
    children = tree.out_edges(root)

    # Base case: leaf nodes with no children run with no inputs
    # Recursive case: recursively resolve inputs for each child
    inputs = {tree.edges[root, child]["key"]: _execute_query_tree(tree, child) for _, child in children}
    return stage.run(inputs)
