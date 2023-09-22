# mypy: ignore-errors

from __future__ import annotations

from contextlib import AbstractContextManager

import daft
from daft.daft import PartitionScheme, PartitionSpec
from daft.dataframe import DataFrame
from daft.logical.schema import Field, Schema


class IcebergCatalog:
    # creator functions
    @classmethod
    def from_glue(cls, args) -> IcebergCatalog:
        ...

    def from_storage(cls, args) -> IcebergCatalog:
        ...

    # namespace functions
    def list_namespaces(self) -> DataFrame:
        ...

    def create_namespace(self, namespace: str) -> bool:
        ...

    def drop_namespace(self, namespace: str) -> bool:
        ...

    # table functions
    def list_tables(self) -> DataFrame:
        ...

    def create_table(
        self,
        identifier: str,
        schema: Schema,
        partition_spec: PartitionSpec | None = None,
        sort_columns: list[str] | None = None,
    ) -> Table:
        ...

    def load_table(self, identifier: str) -> Table:
        ...


class Table:
    def history(self) -> DataFrame:
        ...

    def schema(self) -> Schema:
        ...

    def schemas(self) -> dict[int, Schema]:
        ...

    def partition_spec(self) -> PartitionSpec:
        ...

    def partition_specs(self) -> dict[int, PartitionSpec]:
        ...

    def update_schema(self) -> SchemaUpdate:
        ...

    def update_partition_spec(self) -> PartitionSpecUpdate:
        ...


class SchemaUpdate(AbstractContextManager):
    def add_column(self, field: Field) -> SchemaUpdate:
        ...

    def drop_column(self, name: str) -> SchemaUpdate:
        ...

    def rename_column(self, name: str, new_name: str) -> SchemaUpdate:
        ...


class PartitionSpecUpdate(AbstractContextManager):
    def add_partitioning_field(self, name: str, scheme: PartitionScheme) -> PartitionSpecUpdate:
        ...


class DataframeIcebergNamespace:
    def __init__(self, df: DataFrame) -> None:
        self.df = df

    @classmethod
    def read(cls, catalog: IcebergCatalog, table_identifier: str, snapshot_id: int) -> DataFrame:
        ...

    def append(self, catalog: IcebergCatalog, table_identifier: str) -> None:
        ...

    def overwrite(self, catalog: IcebergCatalog, table_identifier: str) -> None:
        ...


def example_deleting_rows() -> None:
    catalog = IcebergCatalog.from_glue("path/to/glue")
    df: DataFrame = daft.read_iceberg(catalog, "my-table")
    df = df.where(df["id"] > 10 & df["id"] < 20)
    df.iceberg.overwrite(catalog, "my-table")


def example_updating_rows() -> None:
    catalog = IcebergCatalog.from_glue("path/to/glue")
    df: DataFrame = daft.read_iceberg(catalog, "my-table")
    df = df.with_column("x", (df["x"] < 10).if_else(0, df["x"]))
    df.iceberg.overwrite(catalog, "my-table")
