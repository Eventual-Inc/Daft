from __future__ import annotations

import uuid
from math import ceil
from typing import List, Optional, Tuple, Type

import fsspec
import ray
from pyarrow import parquet as pq

from daft.dataclasses import is_daft_dataclass
from daft.datarepo.query import expressions
from daft.datarepo.query.builder import DatarepoQueryBuilder
from icebridge.client import (
    IcebergCatalog,
    IcebergDataFile,
    IcebergSchema,
    IcebergTable,
)


class DataRepo:
    def __init__(self, table: IcebergTable) -> None:
        self._table = table

    def query(self, dtype: Type) -> DatarepoQueryBuilder:
        return DatarepoQueryBuilder._from_datarepo(self, dtype=dtype)

    def to_dataset(
        self,
        dtype: Type,
        filters: Optional[List[List[Tuple]]] = None,
        limit: Optional[int] = None,
    ) -> ray.data.Dataset:
        scan = self._table.new_scan()
        if filters is not None:
            scan = scan.filter(expressions.get_iceberg_filter_expression(filters, self._table.client))
        filelist = scan.plan_files()

        daft_schema = getattr(dtype, "_daft_schema", None)
        assert daft_schema is not None

        # Read 2 * NUM_CPUs number of partitions to take advantage of
        # the amount of parallelism afforded by the cluster
        parallelism = 200
        cluster_cpus = ray.cluster_resources().get("CPU", -1)
        if cluster_cpus != -1:
            parallelism = cluster_cpus * 2

        ds: ray.data.Dataset = ray.data.read_parquet(
            filelist,
            schema=daft_schema.arrow_schema(),
            parallelism=parallelism,
            # Reader kwargs passed to Pyarrow Scanner.from_fragment
            filter=expressions.get_arrow_filter_expression(filters),
        )

        if limit is not None:
            ds = ds.limit(limit)

        return ds.map_batches(
            lambda batch: daft_schema.deserialize_batch(batch, dtype),
            batch_format="pyarrow",
        )

    def schema(self):
        return self._table.schema()

    def name(self):
        return self._table.name()

    def __write_dataset(self, dataset: ray.data.Dataset, rows_per_partition=1024) -> List[str]:
        data_dir = self._table.data_dir()
        protocol = data_dir.split(":")[0] if ":" in data_dir else "file"
        filesystem = fsspec.filesystem(protocol)
        filesystem.makedirs(data_dir, exist_ok=True)

        data_dir = data_dir.replace("file://", "")

        def _write_block(block: List) -> List[Tuple[str, pq.FileMetaData]]:
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
