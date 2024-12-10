import datetime
import uuid
import warnings
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

from daft import Expression, col, lit
from daft.datatype import DataType
from daft.expressions.expressions import ExpressionsProjection
from daft.io.common import _get_schema_from_dict
from daft.table import MicroPartition
from daft.table.partitioning import PartitionedTable, partition_strings_to_path

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.manifest import DataFile
    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties
    from pyiceberg.typedef import Record as IcebergRecord


def get_missing_columns(data_schema: "pa.Schema", iceberg_schema: "IcebergSchema") -> ExpressionsProjection:
    """Add null values for columns in the schema that are missing from the table."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    iceberg_pyarrow_schema = schema_to_pyarrow(iceberg_schema)

    existing_columns = set(data_schema.names)

    to_add = []
    for name in iceberg_pyarrow_schema.names:
        if name not in existing_columns:
            to_add.append(lit(None).alias(name).cast(DataType.from_arrow_type(iceberg_pyarrow_schema.field(name).type)))

    return ExpressionsProjection(to_add)


def coerce_pyarrow_table_to_schema(pa_table: "pa.Table", schema: "pa.Schema") -> "pa.Table":
    """Coerces a PyArrow table to the supplied schema.

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

    part_col = col(schema.find_field(field.source_id).name)

    if isinstance(field.transform, IdentityTransform):
        transform_expr = part_col
    elif isinstance(field.transform, YearTransform):
        transform_expr = part_col.partitioning.years()
    elif isinstance(field.transform, MonthTransform):
        transform_expr = part_col.partitioning.months()
    elif isinstance(field.transform, DayTransform):
        transform_expr = part_col.partitioning.days()
    elif isinstance(field.transform, HourTransform):
        transform_expr = part_col.partitioning.hours()
    elif isinstance(field.transform, BucketTransform):
        transform_expr = part_col.partitioning.iceberg_bucket(field.transform.num_buckets)
    elif isinstance(field.transform, TruncateTransform):
        transform_expr = part_col.partitioning.iceberg_truncate(field.transform.width)
    else:
        warnings.warn(f"{field.transform} not implemented, Please make an issue!")
        transform_expr = part_col

    # currently the partitioning expressions change the name of the column
    # so we need to alias it back to the original column name
    return transform_expr


def to_partition_representation(value: Any):
    """Converts a partition value to the format expected by Iceberg metadata.

    Most transforms already do this, but the identity transforms preserve the original value type so we need to convert it.
    """
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


def make_iceberg_data_file(file_path, size, metadata, partition_record, spec_id, schema, properties):
    import pyiceberg
    from packaging.version import parse
    from pyiceberg.io.pyarrow import (
        compute_statistics_plan,
        parquet_path_to_id_mapping,
    )
    from pyiceberg.manifest import DataFile, DataFileContent
    from pyiceberg.manifest import FileFormat as IcebergFileFormat

    kwargs = {
        "content": DataFileContent.DATA,
        "file_path": file_path,
        "file_format": IcebergFileFormat.PARQUET,
        "partition": partition_record,
        "file_size_in_bytes": size,
        # After this has been fixed:
        # https://github.com/apache/iceberg-python/issues/271
        # "sort_order_id": task.sort_order_id,
        "sort_order_id": None,
        # Just copy these from the table for now
        "spec_id": spec_id,
        "equality_ids": None,
        "key_metadata": None,
    }

    if parse(pyiceberg.__version__) >= parse("0.7.0"):
        from pyiceberg.io.pyarrow import data_file_statistics_from_parquet_metadata

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=metadata,
            stats_columns=compute_statistics_plan(schema, properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )

        data_file = DataFile(
            **{
                **kwargs,
                **statistics.to_serialized_dict(),
            }
        )
    else:
        from pyiceberg.io.pyarrow import fill_parquet_file_metadata

        data_file = DataFile(**kwargs)

        fill_parquet_file_metadata(
            data_file=data_file,
            parquet_metadata=metadata,
            stats_columns=compute_statistics_plan(schema, properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )

    return data_file


class IcebergWriteVisitors:
    class FileVisitor:
        def __init__(self, parent: "IcebergWriteVisitors", partition_record: "IcebergRecord"):
            self.parent = parent
            self.partition_record = partition_record

        def __call__(self, written_file):
            file_path = f"{self.parent.protocol}://{written_file.path}"
            data_file = make_iceberg_data_file(
                file_path,
                written_file.size,
                written_file.metadata,
                self.partition_record,
                self.parent.spec_id,
                self.parent.schema,
                self.parent.properties,
            )

            self.parent.data_files.append(data_file)

    def __init__(
        self,
        protocol: str,
        spec_id: int,
        schema: "IcebergSchema",
        properties: "IcebergTableProperties",
    ):
        self.data_files: List[DataFile] = []
        self.protocol = protocol
        self.spec_id = spec_id
        self.schema = schema
        self.properties = properties

    def visitor(self, partition_record: "IcebergRecord") -> "IcebergWriteVisitors.FileVisitor":
        return self.FileVisitor(self, partition_record)

    def to_metadata(self) -> MicroPartition:
        col_name = "data_file"
        if len(self.data_files) == 0:
            return MicroPartition.empty(_get_schema_from_dict({col_name: DataType.python()}))
        return MicroPartition.from_pydict({col_name: self.data_files})


def make_iceberg_record(partition_values: Optional[Dict[str, Any]]) -> "IcebergRecord":
    from pyiceberg.typedef import Record as IcebergRecord

    if partition_values:
        iceberg_part_vals = {k: to_partition_representation(v) for k, v in partition_values.items()}
        return IcebergRecord(**iceberg_part_vals)
    else:
        return IcebergRecord()


def partitioned_table_to_iceberg_iter(
    partitioned: PartitionedTable, root_path: str, schema: "pa.Schema"
) -> Iterator[Tuple["pa.Table", str, "IcebergRecord"]]:
    partition_values = partitioned.partition_values()

    if partition_values:
        partition_strings = partitioned.partition_values_str()
        assert partition_strings is not None

        for table, part_vals, part_strs in zip(
            partitioned.partitions(),
            partition_values.to_pylist(),
            partition_strings.to_pylist(),
        ):
            part_record = make_iceberg_record(part_vals)
            part_path = partition_strings_to_path(root_path, part_strs, partition_null_fallback="null")

            arrow_table = coerce_pyarrow_table_to_schema(table.to_arrow(), schema)

            yield arrow_table, part_path, part_record
    else:
        arrow_table = coerce_pyarrow_table_to_schema(partitioned.table.to_arrow(), schema)

        yield arrow_table, root_path, make_iceberg_record(None)
