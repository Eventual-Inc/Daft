from __future__ import annotations

from daft.datasources import (
    CSVSourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    StorageType,
)
from daft.logical.logical_plan import FileWrite, TabularFilesScan
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions, TableReadOptions
from daft.table import Table, table_io


class LogicalPartitionOpRunner:
    # TODO(charles): move to ExecutionStep

    def _handle_tabular_files_scan(
        self, inputs: dict[int, Table], scan: TabularFilesScan, index: int | None = None
    ) -> Table:
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
        fs = scan._fs
        schema = scan._schema
        read_options = TableReadOptions(
            num_rows=scan._limit_rows,
            column_names=scan._column_names,  # read only specified columns
        )

        if scan._source_info.scan_type() == StorageType.CSV:
            assert isinstance(scan._source_info, CSVSourceInfo)
            table = Table.concat(
                [
                    table_io.read_csv(
                        file=fp,
                        schema=schema,
                        fs=fs,
                        csv_options=TableParseCSVOptions(
                            delimiter=scan._source_info.delimiter,
                            header_index=0 if scan._source_info.has_headers else None,
                        ),
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        elif scan._source_info.scan_type() == StorageType.JSON:
            assert isinstance(scan._source_info, JSONSourceInfo)
            table = Table.concat(
                [
                    table_io.read_json(
                        file=fp,
                        schema=schema,
                        fs=fs,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        elif scan._source_info.scan_type() == StorageType.PARQUET:
            assert isinstance(scan._source_info, ParquetSourceInfo)
            table = Table.concat(
                [
                    table_io.read_parquet(
                        file=fp,
                        schema=schema,
                        fs=fs,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        else:
            raise NotImplementedError(f"PyRunner has not implemented scan: {scan._source_info.scan_type()}")

        expected_schema = (
            Schema._from_fields([schema[name] for name in read_options.column_names])
            if read_options.column_names is not None
            else schema
        )
        assert (
            table.schema() == expected_schema
        ), f"Expected table to have schema:\n{expected_schema}\n\nReceived instead:\n{table.schema()}"
        return table

    def _handle_file_write(self, inputs: dict[int, Table], file_write: FileWrite) -> Table:
        child_id = file_write._children()[0].id()
        assert file_write._storage_type == StorageType.PARQUET or file_write._storage_type == StorageType.CSV
        if file_write._storage_type == StorageType.PARQUET:
            file_names = table_io.write_parquet(
                inputs[child_id],
                path=file_write._root_dir,
                compression=file_write._compression,
                partition_cols=file_write._partition_cols,
            )
        else:
            file_names = table_io.write_csv(
                inputs[child_id],
                path=file_write._root_dir,
                compression=file_write._compression,
                partition_cols=file_write._partition_cols,
            )

        output_schema = file_write.schema()
        assert len(output_schema) == 1
        return Table.from_pydict(
            {
                output_schema.column_names()[0]: file_names,
            }
        )
