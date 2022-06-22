from __future__ import annotations

from typing import Optional

import os
import sys

import pyarrow as pa
from py4j.java_collections import ListConverter

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
        name:str,
        schema: IcebergSchema,
        partition_spec: Optional[IcebergPartitionSpec]=None,
        namespace: Optional[str] = None
    ) -> IcebergTable:
        jvm = self.client.jvm()
        gateway = self.client.gateway
        name_vargs = gateway.new_array(jvm.java.lang.String, 1)
        name_vargs[0] = name
        if namespace is None:
            identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.of(name_vargs)
        else:
            
            identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.of(namespace, name)
        table = self.catalog.createTable(identifier, schema.schema)
        return IcebergTable(self.client, table)

    @classmethod
    def from_hadoop_catalog(cls, client: IceBridgeClient, hdfs_path: str) -> IcebergCatalog:
        jvm = client.jvm()
        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration
        hadoop_catalog = jvm.org.apache.iceberg.hadoop.HadoopCatalog
        hadoop_catalog_instance = hadoop_catalog(hadoop_conf(), hdfs_path)

        return cls(client, hadoop_catalog_instance)

class IcebergTable:
    def __init__(self, client: IceBridgeClient, java_table) -> None:
        self.client = client
        self.table = java_table
    

class IcebergSchema:
    arrow_to_iceberg = {
        pa.binary(): ('BinaryType', 'get'),
        pa.int32(): ('IntegerType', 'get'),
        pa.int64(): ('LongType', 'get'),
        pa.string(): ('StringType', 'get'),
    }


    def __init__(self, client: IceBridgeClient, java_schema) -> None:
        self.client = client
        self.schema = java_schema
    
    def partition_spec_builder(self) -> IcebergPartitionSpec.Builder:
        return IcebergPartitionSpec.Builder.from_iceberg_schema(self)

    @classmethod
    def from_arrow_schema(cls, client: IceBridgeClient, arrow_schema) -> IcebergSchema:
        jvm = client.jvm()
        iceberg_types = jvm.org.apache.iceberg.types.Types
        names = arrow_schema.names
        iceberg_fields = []
        for i, name  in enumerate(names):
            field = arrow_schema.field(i)
            arrow_type = field.type
            nullable = field.nullable
            if arrow_type not in IcebergSchema.arrow_to_iceberg:
                raise NotImplementedError(f'{arrow_type} to iceberg not implemented')

            iceberg_type_name, method = IcebergSchema.arrow_to_iceberg[arrow_type]
            iceberg_class = getattr(iceberg_types, iceberg_type_name)
            iceberg_type = getattr(iceberg_class, method)()
            if nullable:
                iceberg_field = iceberg_types.NestedField.optional(
                    i + 1,
                    name,
                    iceberg_type
                )
            else:
                iceberg_field = iceberg_types.NestedField.required(
                    i + 1,
                    name,
                    iceberg_type
                )
            iceberg_fields.append(iceberg_field)
        java_list = ListConverter().convert(iceberg_fields, client.gateway._gateway_client)
        iceberg_schema = jvm.org.apache.iceberg.Schema(java_list)
        return cls(client, iceberg_schema)


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
                self.client,
                self.partition_spec_builder.bucket(field_name, num_buckets)
            )

        def time(self, field_name: str, time_unit: str) -> IcebergPartitionSpec.Builder:
            assert time_unit in {'hour', 'day', 'month', 'year'}
            method = getattr(self.partition_spec_builder, time_unit)
            return IcebergPartitionSpec.Builder(
                self.client,
                method(field_name)
            )        

        @classmethod
        def from_iceberg_schema(cls, schema: IcebergSchema) -> IcebergPartitionSpec.Builder:
            jvm = schema.client.jvm()
            java_parition_spec_builder = jvm.org.apache.iceberg.PartitionSpec.builderFor(schema.schema)
            return cls(schema.client, java_parition_spec_builder)
