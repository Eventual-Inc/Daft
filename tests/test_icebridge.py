import tempfile
import py4j

import pyarrow as pa
from pyarrow import parquet as pq

from icebridge.client import IceBridgeClient, IcebergCatalog, IcebergSchema, IcebergDataFile


def test_hadoop_catalog() -> None:
    client = IceBridgeClient()
    with tempfile.TemporaryDirectory() as tmpdirname:
        hadoop_catalog = IcebergCatalog.from_hadoop_catalog(client, f"file://{tmpdirname}")


def test_iceberg_schema_from_arrow() -> None:
    client = IceBridgeClient()
    pa_schema = pa.schema([("some_int", pa.int32()), ("some_string", pa.string())])
    pa_table = pa.table({"some_int": [1, 2, 3, 4], "some_string": ["a", "b", "c", "d"]}, schema=pa_schema)

    iceberg_schema = IcebergSchema.from_arrow_schema(client, pa_schema)
    builder = iceberg_schema.partition_spec_builder()
    part_spec = builder.bucket("some_int", 10).build()

    with tempfile.TemporaryDirectory() as tmpdirname:
        hadoop_catalog = IcebergCatalog.from_hadoop_catalog(client, f"file://{tmpdirname}")

        assert isinstance(hadoop_catalog.catalog, py4j.java_gateway.JavaObject)

        table = hadoop_catalog.create_table("test1", iceberg_schema, part_spec)
        list_tables = hadoop_catalog.list_tables()
        assert len(list_tables) == 1
        assert list_tables[0] == "test1"

        path = f"file://{tmpdirname}/test_data.parquet"
        writer = pa.parquet.ParquetWriter(path, pa_schema)
        writer.write_table(pa_table)
        writer.close()
        file_metadata = writer.writer.metadata

        transaction = table.new_transaction()
        append_files = transaction.append_files()
        data_file = IcebergDataFile.from_parquet(path, file_metadata, table)
        append_files.append_data_file(data_file).commit()
        transaction.commit()
        scan = table.new_scan()
        files = scan.plan_files()
        assert len(files) == 1
        assert files[0] == path

        read_back_table = hadoop_catalog.load_table("test1")

        assert table.location() == read_back_table.location()
