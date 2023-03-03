from __future__ import annotations

from daft.datasources import (
    CSVSourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    StorageType,
)
from daft.logical.logical_plan import FileWrite, TabularFilesScan
from daft.runners.blocks import DataBlock
from daft.runners.partitioning import (
    PyListTile,
    vPartition,
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
    vPartitionSchemaInferenceOptions,
)


class LogicalPartitionOpRunner:
    # TODO(charles): move to ExecutionStep

    def _handle_tabular_files_scan(
        self, inputs: dict[int, vPartition], scan: TabularFilesScan, index: int | None = None
    ) -> vPartition:
        child_id = scan._children()[0].id()
        prev_partition = inputs[child_id]
        data = prev_partition.to_pydict()
        assert (
            scan._filepaths_column_name in data
        ), f"TabularFilesScan should be ran on vPartitions with '{scan._filepaths_column_name}' column"
        filepaths = data[scan._filepaths_column_name]

        if index is not None:
            filepaths = [filepaths[index]]

        # Common options for reading vPartition
        schema = scan._schema
        schema_options = vPartitionSchemaInferenceOptions(schema=schema)
        read_options = vPartitionReadOptions(
            num_rows=scan._limit_rows,
            column_names=scan._column_names,  # read only specified columns
        )

        if scan._source_info.scan_type() == StorageType.CSV:
            assert isinstance(scan._source_info, CSVSourceInfo)
            return vPartition.concat(
                [
                    vPartition.from_csv(
                        path=fp,
                        csv_options=vPartitionParseCSVOptions(
                            delimiter=scan._source_info.delimiter,
                            has_headers=scan._source_info.has_headers,
                            skip_rows_before_header=0,
                            skip_rows_after_header=0,
                        ),
                        schema_options=schema_options,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        elif scan._source_info.scan_type() == StorageType.JSON:
            assert isinstance(scan._source_info, JSONSourceInfo)
            return vPartition.concat(
                [
                    vPartition.from_json(
                        path=fp,
                        schema_options=schema_options,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        elif scan._source_info.scan_type() == StorageType.PARQUET:
            assert isinstance(scan._source_info, ParquetSourceInfo)
            return vPartition.concat(
                [
                    vPartition.from_parquet(
                        path=fp,
                        schema_options=schema_options,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        else:
            raise NotImplementedError(f"PyRunner has not implemented scan: {scan._source_info.scan_type()}")

    def _handle_file_write(self, inputs: dict[int, vPartition], file_write: FileWrite) -> vPartition:
        child_id = file_write._children()[0].id()
        assert file_write._storage_type == StorageType.PARQUET or file_write._storage_type == StorageType.CSV
        if file_write._storage_type == StorageType.PARQUET:
            file_names = inputs[child_id].to_parquet(
                root_path=file_write._root_dir,
                partition_cols=file_write._partition_cols,
                compression=file_write._compression,
            )
        else:
            file_names = inputs[child_id].to_csv(
                root_path=file_write._root_dir,
                partition_cols=file_write._partition_cols,
                compression=file_write._compression,
            )

        output_schema = file_write.schema()
        assert len(output_schema) == 1
        col_name = output_schema.column_names()[0]
        columns: dict[str, PyListTile] = {}
        assert col_name is not None
        columns[col_name] = PyListTile(
            col_name,
            block=DataBlock.make_block(file_names),
        )
        return vPartition(columns)
