from __future__ import annotations

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
    
    @classmethod
    def hadoop_catalog(cls, client: IceBridgeClient, hdfs_path: str) -> IcebergCatalog:
        jvm = client.jvm()
        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration
        hadoop_catalog = jvm.org.apache.iceberg.hadoop.HadoopCatalog
        hadoop_catalog_instance = hadoop_catalog(hadoop_conf(), hdfs_path)

        return cls(client, hadoop_catalog_instance)



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
    
    @classmethod
    def from_arrow_schema(cls, client, arrow_schema) -> IcebergSchema:
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
