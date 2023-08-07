from __future__ import annotations

from daft.daft import LogicalPlanBuilder, PartitionSpec
from daft.logical.schema import Schema


class RustLogicalPlanBuilder:
    """Wrapper class for the new LogicalPlanBuilder in Rust."""

    def __init__(self, builder: LogicalPlanBuilder) -> None:
        self.builder = builder

    def schema(self) -> Schema:
        pyschema = self.builder.schema()
        return Schema._from_pyschema(pyschema)

    def partition_spec(self) -> PartitionSpec:
        return self.builder.partition_spec()

    def __repr__(self) -> str:
        return self.builder.repr_ascii()
