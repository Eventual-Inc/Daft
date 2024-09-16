import datetime
import uuid
import warnings
from typing import TYPE_CHECKING, Any

from daft import Expression, Series, col
from daft.table import MicroPartition

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.schema import Schema as IcebergSchema


def add_missing_columns(table: MicroPartition, schema: "pa.Schema") -> MicroPartition:
    """Add null values for columns in the schema that are missing from the table."""

    import pyarrow as pa

    existing_columns = set(table.column_names())

    columns = {}
    for name in schema.names:
        if name in existing_columns:
            columns[name] = table.get_column(name)
        else:
            columns[name] = Series.from_arrow(pa.nulls(len(table), type=schema.field(name).type), name=name)

    return MicroPartition.from_pydict(columns)


def coerce_pyarrow_table_to_schema(pa_table: "pa.Table", schema: "pa.Schema") -> "pa.Table":
    """Coerces a PyArrow table to the supplied schema

    1. For each field in `pa_table`, cast it to the field in `input_schema` if one with a matching name
        is available
    2. Reorder the fields in the casted table to the supplied schema, dropping any fields in `pa_table`
        that do not exist in the supplied schema
    3. If any fields in the supplied schema are not present, add a null array of the correct type

    This ensures that we populate field_id for iceberg as well as fill in null values where needed
    This might break for nested fields with large_strings
    we should test that behavior

    Args:
        pa_table (pa.Table): Table to coerce
        schema (pa.Schema): Iceberg schema to coerce to

    Returns:
        pa.Table: Table with schema == `schema`
    """
    import pyarrow as pa

    input_schema_names = set(schema.names)

    # Perform casting of types to provided schema's types
    cast_to_schema = [
        (schema.field(inferred_field.name) if inferred_field.name in input_schema_names else inferred_field)
        for inferred_field in pa_table.schema
    ]
    casted_table = pa_table.cast(pa.schema(cast_to_schema))

    # Reorder and pad columns with a null column where necessary
    pa_table_column_names = set(casted_table.column_names)
    columns = []
    for name in schema.names:
        if name in pa_table_column_names:
            columns.append(casted_table[name])
        else:
            columns.append(pa.nulls(len(casted_table), type=schema.field(name).type))
    return pa.table(columns, schema=schema)


def partition_field_to_expr(field: "IcebergPartitionField", schema: "IcebergSchema") -> Expression:
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    partition_col = col(schema.find_field(field.source_id).name)

    if isinstance(field.transform, IdentityTransform):
        return partition_col
    elif isinstance(field.transform, YearTransform):
        return partition_col.partitioning.years()
    elif isinstance(field.transform, MonthTransform):
        return partition_col.partitioning.months()
    elif isinstance(field.transform, DayTransform):
        return partition_col.partitioning.days()
    elif isinstance(field.transform, HourTransform):
        return partition_col.partitioning.hours()
    elif isinstance(field.transform, BucketTransform):
        return partition_col.partitioning.iceberg_bucket(field.transform.num_buckets)
    elif isinstance(field.transform, TruncateTransform):
        return partition_col.partitioning.iceberg_truncate(field.transform.width)
    else:
        warnings.warn(f"{field.transform} not implemented, Please make an issue!")
        return partition_col


def to_partition_representation(value: Any):
    if value is None:
        return None

    if isinstance(value, datetime.datetime):
        # Convert to microseconds since epoch
        return (value - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)
    elif isinstance(value, datetime.date):
        # Convert to days since epoch
        return (value - datetime.date(1970, 1, 1)) // datetime.timedelta(days=1)
    elif isinstance(value, datetime.time):
        # Convert to microseconds since midnight
        return (value.hour * 60 * 60 + value.minute * 60 + value.second) * 1_000_000 + value.microsecond
    elif isinstance(value, uuid.UUID):
        return str(value)
    else:
        return value
