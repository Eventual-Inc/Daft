from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass

from daft.logical.schema import ExpressionList
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import vPartition
from daft.types import ExpressionType


class MapPartitionOp:
    @abstractmethod
    def resource_request(self) -> ResourceRequest:
        """Returns the resource request of running this MapPartitionOp on one vPartition"""

    @abstractmethod
    def get_output_schema(self, input_schema: ExpressionList) -> ExpressionList:
        """Returns the output schema after running this MapPartitionOp on the supplied input_schema"""

    @abstractmethod
    def run(self, input_partition: vPartition) -> vPartition:
        """Runs this MapPartitionOp on the supplied vPartition"""


@dataclass
class ExplodeOp(MapPartitionOp):

    explode_columns: ExpressionList

    def __post_init__(self) -> None:
        assert all([e.has_id() for e in self.explode_columns]), "Columns in ExplodeOp must be resolved"

        for c in self.explode_columns:
            resolved_type = c.resolved_type()
            # TODO(jay): Will have to change this after introducing nested types
            if ExpressionType.is_primitive(resolved_type):
                raise ValueError(
                    f"Expected expression {c} to resolve to an explodable type such as PY, but received: {resolved_type}"
                )

    def resource_request(self) -> ResourceRequest:
        return self.explode_columns.resource_request()

    def get_output_schema(self, input_schema: ExpressionList) -> ExpressionList:
        return input_schema.union(self.explode_columns, other_override=True)

    def run(self, input_partition: vPartition) -> vPartition:
        return input_partition.explode(self.explode_columns)
