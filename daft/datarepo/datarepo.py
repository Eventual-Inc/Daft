from __future__ import annotations

import uuid
from math import ceil
from typing import List, Tuple, Type

import fsspec
import ray
from pyarrow import parquet as pq

from daft.dataclasses import is_daft_dataclass
from daft.datarepo.log import DaftLakeLog
from daft.datarepo.query.builder import DatarepoQueryBuilder

from icebridge.client import IceBridgeClient, IcebergCatalog, IcebergSchema, IcebergDataFile, IcebergTable


class DataRepo:
    def __init__(self, table: IcebergTable) -> None:
        self._table = table

    def query(self, dtype: Type) -> DatarepoQueryBuilder:
        return DatarepoQueryBuilder._from_iceberg_table(self._table, dtype=dtype)

    def to_dataset(self, dtype: Type):
        return self.__read_dataset(dtype)

    def schema(self):
        return self._table.schema()

    def name(self):
        return self._table.name()

    def __read_dataset(self, dtype: Type):
        scan = self._table.new_scan()
        filelist = scan.plan_files()
        daft_schema = getattr(dtype, "_daft_schema", None)
        assert daft_schema is not None

        def _read_block(files: List[str]) -> List[dtype]:
            to_rtn = []
            for filepath in files:
                filepath = filepath.replace("file://", "")
                table = pq.read_table(filepath)
                to_rtn.extend(daft_schema.deserialize_batch(table, dtype))
            return to_rtn

        file_ds = ray.data.from_items(filelist)

        blocks = file_ds.map_batches(_read_block)
        return blocks

    def __write_dataset(self, dataset: ray.data.Dataset, rows_per_partition=1024) -> List[str]:
        data_dir = self._table.data_dir()
        protocol = data_dir.split(":")[0] if ":" in data_dir else "file"
        filesystem = fsspec.filesystem(protocol)
        filesystem.makedirs(data_dir, exist_ok=True)

        data_dir = data_dir.replace("file://", "")

        def _write_block(block: List):
            name = uuid.uuid4()
            filepath = f"{data_dir}/{name}.parquet"
            assert len(block) > 0
            first = block[0]
            daft_schema = getattr(first, "_daft_schema", None)
            assert daft_schema is not None
            arrow_table = daft_schema.serialize(block)

            writer = pq.ParquetWriter(filepath, arrow_table.schema)
            writer.write_table(arrow_table)
            writer.close()
            file_metadata = writer.writer.metadata
            pq.write_table(arrow_table, filepath)
            return [(filepath, file_metadata)]

        num_partitions = ceil(dataset.count() // rows_per_partition)
        dataset = dataset.repartition(num_partitions, shuffle=True)
        filewrite_outputs = dataset.map_batches(_write_block, batch_size=rows_per_partition).take_all()
        return filewrite_outputs

    def append(self, dataset: ray.data.Dataset, rows_per_partition=1024) -> List[str]:
        filewrite_outputs = self.__write_dataset(dataset, rows_per_partition)
        transaction = self._table.new_transaction()
        append_files = transaction.append_files()
        paths_to_rtn = []
        for path, file_metadata in filewrite_outputs:
            data_file = IcebergDataFile.from_parquet(path, file_metadata, self._table)
            append_files.append_data_file(data_file)
            paths_to_rtn.append(path)

        append_files.commit()
        transaction.commit()
        return paths_to_rtn

    def overwrite(self, dataset: ray.data.Dataset, rows_per_partition=1024) -> List[str]:
        filewrite_outputs = self.__write_dataset(dataset, rows_per_partition)
        transaction = self._table.new_transaction()
        scan = self._table.new_scan()
        old_filepaths = scan.plan_files()

        append_files = transaction.append_files()
        paths_to_rtn = []
        for path, file_metadata in filewrite_outputs:
            data_file = IcebergDataFile.from_parquet(path, file_metadata, self._table)
            append_files.append_data_file(data_file)
            paths_to_rtn.append(path)

        append_files.commit()

        delete_files = transaction.delete_files()
        for file in old_filepaths:
            delete_files.delete_file(file)
        delete_files.commit()
        transaction.commit()
        return paths_to_rtn

    def update(self, dataset, func):
        raise NotImplementedError("update not implemented")

    # def history(self):
    #     return self._table.history()

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
