from __future__ import annotations

from abc import abstractmethod

from daft.logical.schema import ExpressionList, Schema
from daft.runners.partitioning import vPartition
from daft.types import ExpressionType


class MapPartitionOp:
    @abstractmethod
    def get_output_schema(self) -> Schema:
        """Returns the output schema after running this MapPartitionOp"""

    @abstractmethod
    def run(self, input_partition: vPartition) -> vPartition:
        """Runs this MapPartitionOp on the supplied vPartition"""


class ExplodeOp(MapPartitionOp):
    input_schema: Schema
    explode_columns: ExpressionList

    def __init__(self, input_schema: Schema, explode_columns: ExpressionList) -> None:
        super().__init__()
        self.input_schema = input_schema
        output_fields = []
        explode_schema = explode_columns.to_schema(input_schema)
        for f in input_schema.fields.values():
            if f.name in explode_schema.column_names():
                output_fields.append(explode_schema[f.name])
            else:
                output_fields.append(f)

        self.output_schema = Schema(output_fields)
        self.explode_columns = explode_columns

        for c in self.explode_columns:
            resolved_type = c.resolve_type(self.input_schema)
            # TODO(jay): Will have to change this after introducing nested types
            if ExpressionType.is_primitive(resolved_type):
                raise ValueError(
                    f"Expected expression {c} to resolve to an explodable type such as PY, but received: {resolved_type}"
                )

    def get_output_schema(self) -> Schema:
        return self.output_schema

    def run(self, input_partition: vPartition) -> vPartition:
        return input_partition.explode(self.explode_columns)
