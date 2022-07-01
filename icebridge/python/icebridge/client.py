from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import fsspec
import pyarrow as pa
from py4j.java_collections import ListConverter, MapConverter

from icebridge.gateway import launch_gateway


class IceBridgeClient:
    def __init__(self) -> None:
        self.gateway = launch_gateway()

    def jvm(self):
        return self.gateway.jvm


class IcebergCatalog:
    def __init__(self, client: IceBridgeClient, java_catalog) -> None:
        self.client = client
        self.catalog = java_catalog

    def create_table(
        self,
        name: str,
        schema: IcebergSchema,
        partition_spec: Optional[IcebergPartitionSpec] = None,
        namespace: str = "default",
    ) -> IcebergTable:
        jvm = self.client.jvm()
        gateway = self.client.gateway

        namespace_vargs = gateway.new_array(jvm.java.lang.String, 1)
        namespace_vargs[0] = namespace
        namespace_obj = jvm.org.apache.iceberg.catalog.Namespace.of(namespace_vargs)

        identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.of(namespace_obj, name)

        table = self.catalog.createTable(identifier, schema.schema)
        return IcebergTable(self.client, table)

    def list_tables(self, namespace: str = "default") -> List[str]:
        jvm = self.client.jvm()
        gateway = self.client.gateway

        namespace_vargs = gateway.new_array(jvm.java.lang.String, 1)
        namespace_vargs[0] = namespace
        namespace_obj = jvm.org.apache.iceberg.catalog.Namespace.of(namespace_vargs)

        tables = self.catalog.listTables(namespace_obj)

        return [t.name() for t in tables]

    def load_table(self, name: str, namespace: str = "default") -> IcebergTable:
        jvm = self.client.jvm()
        gateway = self.client.gateway

        namespace_vargs = gateway.new_array(jvm.java.lang.String, 1)
        namespace_vargs[0] = namespace
        namespace_obj = jvm.org.apache.iceberg.catalog.Namespace.of(namespace_vargs)

        identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.of(namespace_obj, name)
        table = self.catalog.loadTable(identifier)
        return IcebergTable(self.client, table)

    def drop_table(self, name: str, purge: bool, namespace: str = "default") -> bool:
        jvm = self.client.jvm()
        gateway = self.client.gateway

        namespace_vargs = gateway.new_array(jvm.java.lang.String, 1)
        namespace_vargs[0] = namespace
        namespace_obj = jvm.org.apache.iceberg.catalog.Namespace.of(namespace_vargs)

        identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.of(namespace_obj, name)
        return self.catalog.dropTable(identifier, purge)

    @classmethod
    def from_hadoop_catalog(cls, client: IceBridgeClient, hdfs_path: str) -> IcebergCatalog:
        jvm = client.jvm()
        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration
        hadoop_catalog = jvm.org.apache.iceberg.hadoop.HadoopCatalog
        hadoop_catalog_instance = hadoop_catalog(hadoop_conf(), hdfs_path)

        namespace_vargs = client.gateway.new_array(jvm.java.lang.String, 1)
        namespace_vargs[0] = "default"
        namespace_obj = jvm.org.apache.iceberg.catalog.Namespace.of(namespace_vargs)

        hadoop_catalog_instance.createNamespace(namespace_obj)

        return cls(client, hadoop_catalog_instance)


class IcebergTable:
    def __init__(self, client: IceBridgeClient, java_table) -> None:
        self.client = client
        self.table = java_table

    def name(self) -> str:
        return self.table.name()

    def schema(self) -> IcebergSchema:
        return IcebergSchema(self.client, self.table.schema())

    def location(self) -> str:
        return self.table.location()

    def data_dir(self) -> str:
        return os.path.join(self.location(), "_data")

    def spec(self) -> IcebergPartitionSpec:
        return IcebergPartitionSpec(self.client, self.table.spec())

    def new_transaction(self) -> IcebergTransaction:
        return IcebergTransaction(self.client, self.table.newTransaction())

    def new_scan(self) -> IcebergTableScan:
        return IcebergTableScan(self.client, self.table.newScan())


class IcebergTableScan:
    def __init__(self, client: IceBridgeClient, java_table_scan) -> None:
        self.client = client
        self.table_scan = java_table_scan

    def filter(self, expr: IcebergExpression) -> IcebergTableScan:
        new_scan = self.table_scan.filter(expr.expression)
        return IcebergTableScan(self.client, new_scan)

    def select(self, column_names: List[str]) -> IcebergTableScan:
        num_cols = len(column_names)
        col_name_vargs = self.client.gateway.new_array(self.client.jvm().java.lang.String, num_cols)
        for i, name in enumerate(column_names):
            col_name_vargs[i] = name
        new_scan = self.table_scan.filter(col_name_vargs)
        return IcebergTableScan(self.client, new_scan)

    def plan_files(self) -> List[str]:
        files_to_scan = []
        plan_file_iterator = self.table_scan.planFiles().iterator()
        for file in plan_file_iterator:
            files_to_scan.append(file.file().path())
        return files_to_scan


class IcebergExpression:
    def __init__(self, client: IceBridgeClient, java_expression) -> None:
        self.client = client
        self.expression = java_expression

    def negate(self) -> IcebergExpression:
        return IcebergExpression(self.client, self.expression.negate())

    def OR(self, other: IcebergExpression) -> IcebergExpression:
        Expressions = self.client.jvm().org.apache.iceberg.expressions.Expressions
        or_func = getattr(Expressions, "or")
        return IcebergExpression(self.client, or_func(self.expression, other.expression))

    def AND(self, other: IcebergExpression) -> IcebergExpression:
        Expressions = self.client.jvm().org.apache.iceberg.expressions.Expressions
        and_func = getattr(Expressions, "and")
        return IcebergExpression(self.client, and_func(self.expression, other.expression))

    @classmethod
    def always_true(cls, client: IceBridgeClient) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.alwaysTrue())

    @classmethod
    def always_false(cls, client: IceBridgeClient) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.alwaysFalse())

    @classmethod
    def is_null(cls, client: IceBridgeClient, col_name: str) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.isNull(col_name))

    @classmethod
    def is_not_null(cls, client: IceBridgeClient, col_name: str) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.notNull(col_name))

    @classmethod
    def equal(cls, client: IceBridgeClient, col_name: str, value: Any) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.equal(col_name, value))

    @classmethod
    def lt(cls, client: IceBridgeClient, col_name: str, value: Any) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.lessThan(col_name, value))

    @classmethod
    def lte(cls, client: IceBridgeClient, col_name: str, value: Any) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.lessThanOrEqual(col_name, value))

    @classmethod
    def gt(cls, client: IceBridgeClient, col_name: str, value: Any) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.greaterThan(col_name, value))

    @classmethod
    def gte(cls, client: IceBridgeClient, col_name: str, value: Any) -> IcebergExpression:
        Expressions = client.jvm().org.apache.iceberg.expressions.Expressions
        return cls(client, Expressions.greaterThanOrEqual(col_name, value))


class IcebergTransaction:
    def __init__(self, client: IceBridgeClient, java_trasaction) -> None:
        self.client = client
        self.transaction = java_trasaction

    def commit(self) -> None:
        self.transaction.commitTransaction()

    def append_files(self) -> IcebergAppendFiles:
        return IcebergAppendFiles(self.client, self.transaction.newAppend())

    def delete_files(self) -> IcebergDeleteFiles:
        return IcebergDeleteFiles(self.client, self.transaction.newDelete())

    def table(self) -> IcebergTable:
        return IcebergTable(self.client, self.transaction.table())


class IcebergAppendFiles:
    def __init__(self, client: IceBridgeClient, java_append_files) -> None:
        self.client = client
        self.append_files_obj = java_append_files

    def append_data_file(self, data_file: IcebergDataFile) -> IcebergAppendFiles:
        java_data_file = data_file.data_file
        return IcebergAppendFiles(self.client, self.append_files_obj.appendFile(java_data_file))

    def commit(self) -> None:
        self.append_files_obj.commit()


class IcebergDeleteFiles:
    def __init__(self, client: IceBridgeClient, java_delete_files) -> None:
        self.client = client
        self.delete_files_obj = java_delete_files

    def delete_file(self, data_file_path: str) -> IcebergDeleteFiles:
        return IcebergDeleteFiles(self.client, self.delete_files_obj.deleteFile(data_file_path))

    def commit(self) -> None:
        self.delete_files_obj.commit()


class IcebergMetrics:
    METRIC_TYPE_ALLOWLIST = {bool, int, str, float}

    def __init__(self, client: IceBridgeClient, java_metrics) -> None:
        self.client = client
        self.metrics = java_metrics

    def record_count(self) -> int:
        return self.metrics.recordCount()

    @classmethod
    def from_parquet_metadata(cls, schema: IcebergSchema, metadata: pa.parquet.FileMetaData):
        assert metadata.num_row_groups == 1
        client = schema.client
        jvm = client.jvm()
        fields = schema.fields()
        row_group = metadata.row_group(0)
        row_count = row_group.num_rows
        column_sizes = dict()
        value_counts = dict()
        null_counts = dict()
        nan_counts = dict()
        lower_bounds = dict()
        upper_bounds = dict()

        to_byte_buffer = jvm.com.eventualcomputing.icebridge.App.toByteBuffer
        for i in range(row_group.num_columns):
            column = row_group.column(i)
            name = metadata.schema[i].name
            field_id = fields[name]
            java_field = schema.schema.findField(name)
            iceberg_type = java_field.type()
            column_sizes[field_id] = column.total_uncompressed_size
            value_counts[field_id] = column.statistics.num_values
            null_counts[field_id] = column.statistics.null_count
            nan_counts[field_id] = 0  # TODO(sammy) figure out how to do this
            if type(column.statistics.min) in IcebergMetrics.METRIC_TYPE_ALLOWLIST:
                lower_bounds[field_id] = to_byte_buffer(iceberg_type, column.statistics.min)
                upper_bounds[field_id] = to_byte_buffer(iceberg_type, column.statistics.max)

        def convert_map_longs(d: Dict):
            int_map = MapConverter().convert(d, client.gateway._gateway_client)
            return jvm.com.eventualcomputing.icebridge.App.longMapConverter(int_map)

        java_metrics = jvm.org.apache.iceberg.Metrics(
            row_count,
            convert_map_longs(column_sizes),
            convert_map_longs(value_counts),
            convert_map_longs(null_counts),
            convert_map_longs(nan_counts),
            MapConverter().convert(lower_bounds, client.gateway._gateway_client),
            MapConverter().convert(upper_bounds, client.gateway._gateway_client),
        )
        return cls(client, java_metrics)


class IcebergDataFile:
    def __init__(self, client: IceBridgeClient, java_data_file) -> None:
        self.client = client
        self.data_file = java_data_file

    @staticmethod
    def get_file_size(path) -> int:
        protocol = "file"
        if ":" in path:
            protocol = path.split(":")[0]
        fs = fsspec.filesystem(protocol)
        size = fs.size(path)
        return size

    @classmethod
    def from_parquet(
        cls,
        path: str,
        metadata: pa.parquet.FileMetaData,
        table: IcebergTable,
    ) -> IcebergDataFile:
        jvm = table.client.jvm()
        partition_spec = table.spec()
        schema = table.schema()
        datafile = jvm.org.apache.iceberg.DataFiles
        builder = datafile.builder(partition_spec.partition_spec)
        builder.withFormat("parquet")
        builder.withPath(path)
        metrics = IcebergMetrics.from_parquet_metadata(schema, metadata)
        builder.withMetrics(metrics.metrics)
        builder.withRecordCount(metrics.record_count())
        builder.withFileSizeInBytes(cls.get_file_size(path))
        data_file = builder.build()
        return IcebergDataFile(table.client, data_file)


class IcebergSchema:
    arrow_to_iceberg = {
        pa.binary(): ("BinaryType", "get"),
        pa.int32(): ("IntegerType", "get"),
        pa.int64(): ("LongType", "get"),
        pa.string(): ("StringType", "get"),
    }

    def __init__(self, client: IceBridgeClient, java_schema, arrow_schema=None) -> None:
        self.client = client
        self.schema = java_schema
        self.arrow_schema = arrow_schema

    def fields(self) -> Dict[str, int]:
        fields = dict()
        for java_column in self.schema.columns():
            name = java_column.name()
            id = java_column.fieldId()
            fields[name] = id
        return fields

    def partition_spec_builder(self) -> IcebergPartitionSpec.Builder:
        return IcebergPartitionSpec.Builder.from_iceberg_schema(self)

    @classmethod
    def from_arrow_schema(cls, client: IceBridgeClient, arrow_schema) -> IcebergSchema:
        jvm = client.jvm()
        iceberg_types = jvm.org.apache.iceberg.types.Types
        names = arrow_schema.names
        iceberg_fields = []
        for i, name in enumerate(names):
            field = arrow_schema.field(i)
            arrow_type = field.type
            nullable = field.nullable
            if arrow_type not in IcebergSchema.arrow_to_iceberg:
                raise NotImplementedError(f"{arrow_type} to iceberg not implemented")

            iceberg_type_name, method = IcebergSchema.arrow_to_iceberg[arrow_type]
            iceberg_class = getattr(iceberg_types, iceberg_type_name)
            iceberg_type = getattr(iceberg_class, method)()
            if nullable:
                iceberg_field = iceberg_types.NestedField.optional(i + 1, name, iceberg_type)
            else:
                iceberg_field = iceberg_types.NestedField.required(i + 1, name, iceberg_type)
            iceberg_fields.append(iceberg_field)
        java_list = ListConverter().convert(iceberg_fields, client.gateway._gateway_client)
        iceberg_schema = jvm.org.apache.iceberg.Schema(java_list)
        return cls(client, iceberg_schema, arrow_schema=arrow_schema)


class IcebergPartitionSpec:
    def __init__(self, client: IceBridgeClient, java_partition_spec):
        self.client = client
        self.partition_spec = java_partition_spec

    class Builder:
        def __init__(self, client: IceBridgeClient, java_partition_spec_builder) -> None:
            self.client = client
            self.partition_spec_builder = java_partition_spec_builder

        def build(self) -> IcebergPartitionSpec:
            return IcebergPartitionSpec(self.client, self.partition_spec_builder.build())

        def bucket(self, field_name: str, num_buckets: int) -> IcebergPartitionSpec.Builder:
            return IcebergPartitionSpec.Builder(
                self.client, self.partition_spec_builder.bucket(field_name, num_buckets)
            )

        def time(self, field_name: str, time_unit: str) -> IcebergPartitionSpec.Builder:
            assert time_unit in {"hour", "day", "month", "year"}
            method = getattr(self.partition_spec_builder, time_unit)
            return IcebergPartitionSpec.Builder(self.client, method(field_name))

        @classmethod
        def from_iceberg_schema(cls, schema: IcebergSchema) -> IcebergPartitionSpec.Builder:
            jvm = schema.client.jvm()
            java_parition_spec_builder = jvm.org.apache.iceberg.PartitionSpec.builderFor(schema.schema)
            return cls(schema.client, java_parition_spec_builder)
