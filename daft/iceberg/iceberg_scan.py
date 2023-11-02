from __future__ import annotations

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table

from daft.datatype import DataType
from daft.expressions.expressions import col
from daft.io.scan import PartitionField, ScanOperator, make_partition_field
from daft.logical.schema import Field, Schema


def _iceberg_partition_field_to_daft_partition_field(
    iceberg_schema: IcebergSchema, pfield: IcebergPartitionField
) -> PartitionField:
    name = pfield.name
    source_id = pfield.source_id
    source_field = iceberg_schema.find_field(source_id)
    source_name = source_field.name
    daft_field = Field.create(
        source_name, DataType.from_arrow_type(schema_to_pyarrow(iceberg_schema.find_type(source_name)))
    )
    transform = pfield.transform
    iceberg_result_type = transform.result_type(source_field.field_type)
    arrow_result_type = schema_to_pyarrow(iceberg_result_type)
    daft_result_type = DataType.from_arrow_type(arrow_result_type)
    result_field = Field.create(name, daft_result_type)

    from pyiceberg.transforms import (
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        YearTransform,
    )

    expr = None
    if isinstance(transform, IdentityTransform):
        expr = col(source_name)
        if source_name != name:
            expr = expr.alias(name)
    elif isinstance(transform, YearTransform):
        expr = col(source_name).dt.year().alias(name)
    elif isinstance(transform, MonthTransform):
        expr = col(source_name).dt.month().alias(name)
    elif isinstance(transform, DayTransform):
        expr = col(source_name).dt.day().alias(name)
    elif isinstance(transform, HourTransform):
        raise NotImplementedError("HourTransform not implemented, Please make an issue!")
    else:
        raise NotImplementedError(f"{transform} not implemented, Please make an issue!")

    assert expr is not None
    return make_partition_field(result_field, daft_field, transform=expr)


def iceberg_partition_spec_to_fields(iceberg_schema: IcebergSchema, spec: IcebergPartitionSpec) -> list[PartitionField]:
    return [_iceberg_partition_field_to_daft_partition_field(iceberg_schema, field) for field in spec.fields]


class IcebergScanOperator(ScanOperator):
    def __init__(self, iceberg_table: Table) -> None:
        super().__init__()
        self._table = iceberg_table
        arrow_schema = schema_to_pyarrow(iceberg_table.schema())
        self._schema = Schema.from_pyarrow_schema(arrow_schema)
        self._partition_keys = iceberg_partition_spec_to_fields(self._table.schema(), self._table.spec())

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PartitionField]:
        return self._partition_keys


def catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


cat = catalog()
tab = cat.load_table("default.test_partitioned_by_years")
ice = IcebergScanOperator(tab)
