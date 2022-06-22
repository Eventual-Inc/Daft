import tempfile
import py4j

import pyarrow as pa

from icebridge.client import IceBridgeClient, IcebergCatalog, IcebergSchema



def test_hadoop_catalog() -> None:
    client = IceBridgeClient()
    with tempfile.TemporaryDirectory() as tmpdirname:
        hadoop_catalog = IcebergCatalog.from_hadoop_catalog(client, f"file://{tmpdirname}")



def test_iceberg_schema_from_arrow() -> None:
    client = IceBridgeClient()
    pa_schema = pa.schema([
        ('some_int', pa.int32()),
        ('some_string', pa.string())
    ])
    iceberg_schema = IcebergSchema.from_arrow_schema(client, pa_schema)
    builder = iceberg_schema.partition_spec_builder()
    part_spec = builder.bucket('some_int', 10).build()

    with tempfile.TemporaryDirectory() as tmpdirname:
        hadoop_catalog = IcebergCatalog.from_hadoop_catalog(client, f"file://{tmpdirname}")
        table = hadoop_catalog.create_table("test1", iceberg_schema)
        assert isinstance(hadoop_catalog.catalog, py4j.java_gateway.JavaObject)


