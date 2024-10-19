from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar

from daft.expressions import ExpressionsProjection
from daft.logical.schema import Schema
from daft.runners.partitioning import EstimatedPartitionMetadata

if TYPE_CHECKING:
    from daft.table import MicroPartition


class MapPartitionOp:
    @abstractmethod
    def get_output_schema(self) -> Schema:
        """Returns the output schema after running this MapPartitionOp"""

    @abstractmethod
    def run(self, input_partition: MicroPartition) -> MicroPartition:
        """Runs this MapPartitionOp on the supplied vPartition"""

    @abstractmethod
    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        """Estimates the output partition metadata"""


class ExplodeOp(MapPartitionOp):
    CARDINALITY_INCREASE_FACTOR: ClassVar[int] = 5

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

    def run(self, input_partition: MicroPartition) -> MicroPartition:
        return input_partition.explode(self.explode_columns)

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 1
        [input_meta] = input_metadatas
        return [
            EstimatedPartitionMetadata(
                num_rows=input_meta.num_rows * ExplodeOp.CARDINALITY_INCREASE_FACTOR,
                size_bytes=input_meta.size_bytes * ExplodeOp.CARDINALITY_INCREASE_FACTOR,
            )
        ]
