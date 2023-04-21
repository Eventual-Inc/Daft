from __future__ import annotations

from abc import abstractmethod

from daft.expressions import ExpressionsProjection
from daft.logical.schema import Schema
from daft.table import Table


class MapPartitionOp:
    @abstractmethod
    def get_output_schema(self) -> Schema:
        """Returns the output schema after running this MapPartitionOp"""

    @abstractmethod
    def run(self, input_partition: Table) -> Table:
        """Runs this MapPartitionOp on the supplied vPartition"""


class ExplodeOp(MapPartitionOp):
    input_schema: Schema
    explode_columns: ExpressionsProjection

    def __init__(self, input_schema: Schema, explode_columns: ExpressionsProjection) -> None:
        super().__init__()
        self.input_schema = input_schema
        output_fields = []
        explode_columns = ExpressionsProjection([c._explode() for c in explode_columns])
        explode_schema = explode_columns.resolve_schema(input_schema)
        for f in input_schema:
            if f.name in explode_schema.column_names():
                output_fields.append(explode_schema[f.name])
            else:
                output_fields.append(f)

        self.output_schema = Schema._from_field_name_and_types([(f.name, f.dtype) for f in output_fields])
        self.explode_columns = explode_columns

    def get_output_schema(self) -> Schema:
        return self.output_schema

    def run(self, input_partition: Table) -> Table:
        return input_partition.explode(self.explode_columns)
