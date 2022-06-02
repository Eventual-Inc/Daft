from __future__ import annotations
import dataclasses
import logging

import fsspec
import pyarrow as pa
import ray
from ray.data.impl.arrow_block import ArrowRow

from typing import List, Optional, Type, TypeVar

from daft import config
from daft.dataset import Dataset


logger = logging.getLogger(__name__)

Dataclass = TypeVar("Dataclass")


class DatarepoClient:
    def __init__(self, path: str):
        pathsplit = path.split("://")
        if len(pathsplit) != 2:
            raise ValueError(f"Expected path in format <protocol>://<path> but received: {path}")
        self._protocol, self._prefix = pathsplit
        self._fs = fsspec.filesystem(self._protocol)

    def list_ids(self) -> List[str]:
        """List the IDs of all datarepos

        Returns:
            List[str]: IDs of datarepos
        """
        return [path.replace(self._prefix, "").lstrip("/") for path in self._fs.ls(self._prefix)]

    def _get_parquet_filepaths(self, datarepo_id: str) -> List[str]:
        """Returns a sorted list of filepaths to the Parquet files that make up this datarepo

        Args:
            datarepo_id (str): ID of the datarepo

        Returns:
            List[str]: list of paths to Parquet files
        """
        return sorted(
            [
                f"{self._protocol}://{path}"
                for path in self._fs.ls(f"{self._prefix}/{datarepo_id}")
                if path.endswith(".parquet")
            ]
        )

    def get_dataset(
        self,
        datarepo_id: str,
        data_type: Type[Dataclass] = ArrowRow,
        columns: Optional[List[str]] = None,
        partitions: Optional[int] = None,
    ) -> Dataset[Dataclass]:
        """Retrieves a slice of the Datarepo as a Dataset

        Args:
            datarepo_id (str): ID of the datarepo
            data_type (Type[Dataclass], optional): Dataclass of each item - will be deprecated in the future
                when we can autodetect the Dataclass from the Datarepo definition
            columns (Optional[List[str]], optional): columns to retrieve. Defaults to None which retrieves all columns
            partitions (Optional[int], optional): number of partitions to repartition the Dataset into on load. Defaults\
                to None which does not perform any repartitioning.

        Returns:
            Dataset[Dataclass]: Dataset containing the retrieved data from the Datarepo
        """
        if data_type != ArrowRow:
            assert dataclasses.is_dataclass(data_type) and isinstance(data_type, type)
            assert hasattr(data_type, "_daft_schema"), f"{data_type} was not initialized with daft dataclass"
            daft_schema = getattr(data_type, "_daft_schema")
            # _patch_class_for_deserialization(data_type)
            # if getattr(data_type, "__daft_patched", None) != id(dataclasses._FIELD):
            #     assert dataclasses.is_dataclass(data_type) and isinstance(data_type, type)
            #     fields = data_type.__dict__["__dataclass_fields__"]
            #     for field in fields.values():
            #         if type(field._field_type) is type(dataclasses._FIELD):
            #             field._field_type = dataclasses._FIELD
            #     setattr(data_type, "__daft_patched", id(dataclasses._FIELD))

            def deserialize(items) -> List[Dataclass]:
                block: List[Dataclass] = daft_schema.deserialize_batch(items, data_type)
                return block

        parquet_filepaths = self._get_parquet_filepaths(datarepo_id)

        # Ray API does not recognize file:// as a protocol
        if parquet_filepaths and parquet_filepaths[0].startswith("file://"):
            parquet_filepaths = [path.replace("file://", "") for path in parquet_filepaths]

        # TODO(jaychia): Use .read_parquet_bulk instead in Ray 1.13 after its release for various performance improvements
        ds: ray.data.Dataset[ArrowRow] = ray.data.read_parquet(
            parquet_filepaths,
            # Arrow read_table **kwargs
            # See: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
            columns=columns and [f"root.{col}" for col in columns],
            # TODO(jaychia): Need to get schema from the datarepo logs when that is ready
            # This will currently just infer the schema from the Parquet files
            # schema=self.get_schema(datarepo_id),
        )

        if partitions is not None:
            ds = ds.repartition(partitions, shuffle=True)

        if data_type != ArrowRow:
            ds = ds.map_batches(deserialize, batch_format="pyarrow")

        return Dataset(
            dataset_id=datarepo_id,
            ray_dataset=ds,
        )

    def save(self, datarepo_id: str, dataset: Dataset[Dataclass]) -> None:
        """Saves a Dataset as a Datarepo - NOTE: this will be deprecated soon in favor of operations
        that will overwrite, append and update a datarepo using our DatarepoLog.

        Args:
            datarepo_id (str): ID of the datarepo
            dataset (Dataset[Dataclass]): dataset to save
        """
        sample_item = dataset._ray_dataset.take(1)

        if len(sample_item) == 0:
            logger.warning("nothing to save")
            return None

        # _patch_class_for_deserialization(sample_item[0].__class__)

        def serialize(items: List[Dataclass]) -> pa.Table:
            if len(items) == 0:
                return None
            first_type = items[0].__class__
            assert dataclasses.is_dataclass(first_type), "We can only serialize daft dataclasses"
            assert hasattr(first_type, "_daft_schema"), "was not initialized with daft dataclass"
            daft_schema = getattr(first_type, "_daft_schema")
            return daft_schema.serialize(items)

        path = f"{self._protocol}://{self._prefix}/{datarepo_id}"
        serialized_ds: ray.data.Dataset[pa.Table] = dataset._ray_dataset.map_batches(serialize)  # type: ignore
        return serialized_ds.write_parquet(path)


def get_client(datarepo_path: Optional[str] = None) -> DatarepoClient:
    """Return the appropriate DatarepoClient as configured by the environment

    Returns:
        DatarepoClient: DatarepoClient to access Datarepos
    """
    if datarepo_path is None:
        daft_settings = config.DaftSettings()
        datarepo_path = f"s3://{daft_settings.DAFT_DATAREPOS_BUCKET}/{daft_settings.DAFT_DATAREPOS_PREFIX}"
    return DatarepoClient(datarepo_path)
