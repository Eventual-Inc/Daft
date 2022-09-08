from __future__ import annotations

from typing import Type

from icebridge.client import IcebergCatalog, IcebergSchema, IcebergTable

from daft.experimental.dataclasses import is_daft_dataclass


class DataRepo:
    def __init__(self, table: IcebergTable) -> None:
        self._table = table

    def schema(self):
        return self._table.schema()

    def name(self):
        return self._table.name()

    @classmethod
    def create(cls, catalog: IcebergCatalog, name: str, dtype: Type) -> DataRepo:
        assert dtype is not None and is_daft_dataclass(dtype)
        catalog.client
        new_schema = getattr(dtype, "_daft_schema", None)
        assert new_schema is not None, f"{dtype} is not a daft dataclass"
        arrow_schema = new_schema.arrow_schema()
        iceberg_schema = IcebergSchema.from_arrow_schema(catalog.client, arrow_schema)
        # builder = iceberg_schema.partition_spec_builder() # TODO(sammy) expose partitioning
        table = catalog.create_table(name, iceberg_schema)

        return DataRepo(table)
