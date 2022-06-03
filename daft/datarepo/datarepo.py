from __future__ import annotations

import uuid
from math import ceil
from typing import List, Type

import ray
from pyarrow import parquet as pq

from daft.dataclasses import is_daft_dataclass
from daft.datarepo.log import DaftLakeLog


class DataRepo:
    def __init__(self, log: DaftLakeLog) -> None:
        self._log = log

    def _load(self, path) -> None:
        ...

    def to_dataset(self, dtype: Type):
        return self.__read_dataset(dtype)

    def schema(self):
        return self._log.schema()

    def __read_dataset(self, dtype: Type):
        filelist = self._log.file_list()
        data_dir = self._log.data_dir()
        filesystem = self._log._fs
        daft_schema = getattr(dtype, "_daft_schema", None)
        assert daft_schema is not None

        def _read_block(files: List[str]) -> List[dtype]:
            to_rtn = []
            for filepath in files:
                filepath = filepath.replace("file://", "")
                table = pq.read_table(filepath, filesystem=filesystem)
                to_rtn.extend(daft_schema.deserialize_batch(table, dtype))
            return to_rtn

        file_ds = ray.data.from_items(filelist)

        blocks = file_ds.map_batches(_read_block)
        return blocks

    def __write_dataset(self, dataset: ray.data.Dataset, rows_per_partition=1024) -> List[str]:
        data_dir = self._log.data_dir()
        data_dir = data_dir.replace("file://", "")
        filesystem = self._log._fs
        filesystem.makedir(data_dir)

        def _write_block(block: List) -> str:
            name = uuid.uuid4()
            filepath = f"{data_dir}/{name}.parquet"
            assert len(block) > 0
            first = block[0]
            daft_schema = getattr(first, "_daft_schema", None)
            assert daft_schema is not None
            arrow_table = daft_schema.serialize(block)
            pq.write_table(arrow_table, filepath, filesystem=filesystem)
            return [filepath]

        num_partitions = ceil(dataset.count() // rows_per_partition)
        dataset = dataset.repartition(num_partitions, shuffle=True)
        filepaths = dataset.map_batches(_write_block, batch_size=rows_per_partition).take_all()
        return filepaths

    def append(self, dataset: ray.data.Dataset, rows_per_partition=1024):
        filepaths = self.__write_dataset(dataset, rows_per_partition)
        self._log.start_transaction()
        for file in filepaths:
            self._log.add_file(file)
        self._log.commit()

    def overwrite(self, dataset: ray.data.Dataset, rows_per_partition=1024):
        old_filepaths = self._log.file_list()
        filepaths = self.__write_dataset(dataset, rows_per_partition)
        self._log.start_transaction()
        for file in filepaths:
            self._log.add_file(file)
        for file in old_filepaths:
            self._log.remove_file(file)
        self._log.commit()

    def update(self, dataset, func):
        raise NotImplementedError("update not implemented")

    def history(self):
        return self._log.history()

    @classmethod
    def create(cls, path: str, name: str, dtype: Type) -> DataRepo:
        assert dtype is not None and is_daft_dataclass(dtype)
        log = DaftLakeLog(path)
        assert log.is_empty()

        new_schema = getattr(dtype, "_daft_schema", None)
        assert new_schema is not None, f"{dtype} is not a daft dataclass"
        log.create(name, new_schema.arrow_schema())
        return DataRepo(log)
