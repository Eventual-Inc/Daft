import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from daft import Series
from daft.table import MicroPartition

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.io.pyarrow import _TablePartition
    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.typedef import Record


def _coerce_pyarrow_table_to_schema(pa_table: "pa.Table", schema: "IcebergSchema") -> "pa.Table":
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
        schema (IcebergSchema): PyIceberg schema to coerce to

    Returns:
        pa.Table: Table with schema == `input_schema`
    """
    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    input_schema = schema_to_pyarrow(schema)

    input_schema_names = set(input_schema.names)

    # Perform casting of types to provided schema's types
    cast_to_schema = [
        (input_schema.field(inferred_field.name) if inferred_field.name in input_schema_names else inferred_field)
        for inferred_field in pa_table.schema
    ]
    casted_table = pa_table.cast(pa.schema(cast_to_schema))

    # Reorder and pad columns with a null column where necessary
    pa_table_column_names = set(casted_table.column_names)
    columns = []
    for name in input_schema.names:
        if name in pa_table_column_names:
            columns.append(casted_table[name])
        else:
            columns.append(pa.nulls(len(casted_table), type=input_schema.field(name).type))
    return pa.table(columns, schema=input_schema)


def _determine_partitions(
    spec: "IcebergPartitionSpec", schema: "IcebergSchema", arrow_table: "pa.Table"
) -> List["_TablePartition"]:
    """Based on https://github.com/apache/iceberg-python/blob/d8d509ff1bc33040b9f6c90c28ee47ac7437945d/pyiceberg/io/pyarrow.py#L2669"""

    import pyarrow as pa
    from pyiceberg.io.pyarrow import _get_table_partitions
    from pyiceberg.partitioning import PartitionField
    from pyiceberg.transforms import Transform
    from pyiceberg.types import NestedField

    partition_columns: List[Tuple[PartitionField, NestedField]] = [
        (partition_field, schema.find_field(partition_field.source_id)) for partition_field in spec.fields
    ]

    def partition_transform(array: pa.Array, transform: Transform) -> Optional[pa.Array]:
        from pyiceberg.transforms import (
            BucketTransform,
            DayTransform,
            HourTransform,
            IdentityTransform,
            MonthTransform,
            TruncateTransform,
            YearTransform,
        )

        series = Series.from_arrow(array)

        transformed = None
        if isinstance(transform, IdentityTransform):
            transformed = series
        elif isinstance(transform, YearTransform):
            transformed = series.partitioning.years()
        elif isinstance(transform, MonthTransform):
            transformed = series.partitioning.months()
        elif isinstance(transform, DayTransform):
            transformed = series.partitioning.days()
        elif isinstance(transform, HourTransform):
            transformed = series.partitioning.hours()
        elif isinstance(transform, BucketTransform):
            n = transform.num_buckets
            transformed = series.partitioning.iceberg_bucket(n)
        elif isinstance(transform, TruncateTransform):
            w = transform.width
            transformed = series.partitioning.iceberg_truncate(w)
        else:
            warnings.warn(f"{transform} not implemented, Please make an issue!")

        return transformed.to_arrow() if transformed is not None else None

    partition_values_table = pa.table(
        {
            str(partition.field_id): partition_transform(arrow_table[field.name], partition.transform)
            for partition, field in partition_columns
        }
    )

    # Sort by partitions
    sort_indices = pa.compute.sort_indices(
        partition_values_table,
        sort_keys=[(col, "ascending") for col in partition_values_table.column_names],
        null_placement="at_end",
    ).to_pylist()
    arrow_table = arrow_table.take(sort_indices)

    # Get slice_instructions to group by partitions
    partition_values_table = partition_values_table.take(sort_indices)
    reversed_indices = pa.compute.sort_indices(
        partition_values_table,
        sort_keys=[(col, "descending") for col in partition_values_table.column_names],
        null_placement="at_start",
    ).to_pylist()
    slice_instructions: List[Dict[str, Any]] = []
    last = len(reversed_indices)
    reversed_indices_size = len(reversed_indices)
    ptr = 0
    while ptr < reversed_indices_size:
        group_size = last - reversed_indices[ptr]
        offset = reversed_indices[ptr]
        slice_instructions.append({"offset": offset, "length": group_size})
        last = reversed_indices[ptr]
        ptr = ptr + group_size

    table_partitions: List[_TablePartition] = _get_table_partitions(arrow_table, spec, schema, slice_instructions)

    return table_partitions


def micropartition_to_arrow_tables(
    table: MicroPartition, path: str, schema: "IcebergSchema", partition_spec: "IcebergPartitionSpec"
) -> List[Tuple["pa.Table", str, "Record"]]:
    """
    Converts a MicroPartition to a list of Arrow tables with paths, partitioning the data if necessary.

    Args:
        table (MicroPartition): Table to convert
        path (str): Base path to write the table to
        schema (IcebergSchema): Schema of the Iceberg table
        partition_spec (IcebergPartitionSpec): Iceberg partitioning spec

    Returns:
        List[Tuple[pa.Table, str, Record]]: List of Arrow tables with their paths and partition records
    """
    from pyiceberg.typedef import Record

    arrow_table = table.to_arrow()
    arrow_table = _coerce_pyarrow_table_to_schema(arrow_table, schema)

    if partition_spec.is_unpartitioned():
        return [(arrow_table, path, Record())]
    else:
        partitions = _determine_partitions(partition_spec, schema, arrow_table)

        return [
            (
                partition.arrow_table_partition,
                f"{path}/{partition.partition_key.to_path()}",
                partition.partition_key.partition,
            )
            for partition in partitions
        ]
