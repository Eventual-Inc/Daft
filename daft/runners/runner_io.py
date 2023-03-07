from __future__ import annotations

from abc import abstractmethod
from typing import Generic, TypeVar

from daft.datasources import (
    CSVSourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    SourceInfo,
    StorageType,
)
from daft.logical.schema import Schema
from daft.runners.partitioning import (
    PartitionSet,
    vPartition,
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.types import ExpressionType

PartitionT = TypeVar("PartitionT")


class RunnerIO(Generic[PartitionT]):
    """Reading and writing data from the Runner.

    This is an abstract class and each runner must write their own implementation.

    This is a generic class and each runner needs to parametrize their implementation with the appropriate
    PartitionT that it uses for in-memory data representation.
    """

    FS_LISTING_PATH_COLUMN_NAME = "path"
    FS_LISTING_SIZE_COLUMN_NAME = "size"
    FS_LISTING_TYPE_COLUMN_NAME = "type"
    FS_LISTING_ROWS_COLUMN_NAME = "rows"
    FS_LISTING_SCHEMA = Schema._from_field_name_and_types(
        [
            (FS_LISTING_SIZE_COLUMN_NAME, ExpressionType.integer()),
            (FS_LISTING_PATH_COLUMN_NAME, ExpressionType.string()),
            (FS_LISTING_TYPE_COLUMN_NAME, ExpressionType.string()),
            (FS_LISTING_ROWS_COLUMN_NAME, ExpressionType.integer()),
        ]
    )

    @abstractmethod
    def glob_paths_details(
        self,
        source_path: str,
        source_info: SourceInfo | None = None,
    ) -> PartitionSet[PartitionT]:
        """Globs the specified filepath to construct Partitions containing file and dir metadata

        Args:
            source_path (str): path to glob

        Returns:
            PartitionSet[PartitionT]: Partitions containing the listings' metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def get_schema_from_first_filepath(
        self, listing_details_partitions: PartitionSet[PartitionT], source_info: SourceInfo
    ) -> Schema:
        raise NotImplementedError()


def sample_schema(filepath: str, source_info: SourceInfo) -> Schema:
    """Helper method that samples a schema from the specified source"""

    sampled_partition: vPartition
    if source_info.scan_type() == StorageType.CSV:
        assert isinstance(source_info, CSVSourceInfo)
        sampled_partition = vPartition.from_csv(
            path=filepath,
            csv_options=vPartitionParseCSVOptions(
                delimiter=source_info.delimiter,
                has_headers=source_info.has_headers,
                skip_rows_before_header=0,
                skip_rows_after_header=0,
            ),
            schema_options=vPartitionSchemaInferenceOptions(
                schema=None,
                inference_column_names=None,  # TODO: pass in user-provided column names
            ),
            read_options=vPartitionReadOptions(
                num_rows=100,  # sample 100 rows for schema inference
                column_names=None,  # read all columns
            ),
        )
    elif source_info.scan_type() == StorageType.JSON:
        assert isinstance(source_info, JSONSourceInfo)
        sampled_partition = vPartition.from_json(
            path=filepath,
            read_options=vPartitionReadOptions(
                num_rows=100,  # sample 100 rows for schema inference
                column_names=None,  # read all columns
            ),
        )
    elif source_info.scan_type() == StorageType.PARQUET:
        assert isinstance(source_info, ParquetSourceInfo)
        sampled_partition = vPartition.from_parquet(
            path=filepath,
            read_options=vPartitionReadOptions(
                num_rows=100,  # sample 100 rows for schema inference
                column_names=None,  # read all columns
            ),
        )
    else:
        raise NotImplementedError(f"Schema inference for {source_info} not implemented")

    return sampled_partition.schema()
