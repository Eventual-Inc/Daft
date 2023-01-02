from __future__ import annotations

import dataclasses
import io
import weakref
from abc import abstractmethod
from dataclasses import dataclass
from functools import partial
from typing import IO, Any, Callable, Generic, List, Sequence, TypeVar, cast
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import csv
from pyarrow import dataset as pada
from pyarrow import json, parquet

from daft.execution.operators import OperatorEnum
from daft.expressions import ColID, ColumnExpression, Expression, ExpressionExecutor
from daft.filesystem import get_filesystem_from_path
from daft.logical.schema import ExpressionList
from daft.runners.blocks import ArrowArrType, ArrowDataBlock, DataBlock, PyListDataBlock
from daft.types import ExpressionType, PythonExpressionType

PartID = int


@dataclass(frozen=True)
class vPartitionReadOptions:
    """Options for reading a vPartition

    Args:
        num_rows: Number of rows to read, or None to read all rows
        column_names: Column names to include when reading, or None to read all columns
    """

    num_rows: int | None = None
    column_names: list[str] | None = None


@dataclass(frozen=True)
class vPartitionSchemaInferenceOptions:
    """Options for schema inference when reading a vPartition

    Args:
        schema: A schema to use when reading the vPartition. If provided, all schema inference should be skipped.
        inference_column_names: Column names to use when performing schema inference
    """

    schema: ExpressionList | None = None
    inference_column_names: list[str] | None = None

    def full_schema_column_names(self) -> list[str] | None:
        """Returns all column names for the schema, or None if not provided."""
        if self.schema is not None:
            return [expr.name() for expr in self.schema]
        return self.inference_column_names


@dataclass(frozen=True)
class vPartitionParseCSVOptions:
    """Options for parsing CSVs

    Args:
        delimiter: The delimiter to use when parsing CSVs, defaults to ","
        has_headers: Whether the CSV has headers, defaults to True
        skip_rows_before_header: Number of rows to skip before the header, defaults to 0
        skip_rows_after_header: Number of rows to skip after the header, defaults to 0
    """

    delimiter: str = ","
    has_headers: bool = True
    skip_rows_before_header: int = 0
    skip_rows_after_header: int = 0


def _limit_num_rows(buf: IO, num_rows: int) -> IO:
    """Limites a buffer to a certain number of rows using an in-memory buffer."""
    sampled_bytes = io.BytesIO()
    for i, line in enumerate(buf):
        if i >= num_rows:
            break
        sampled_bytes.write(line)
    sampled_bytes.seek(0)
    return sampled_bytes


def _get_column_ids(table: pa.Table, schema_options: vPartitionSchemaInferenceOptions) -> list[ColID]:
    # No schema provided, we allocate column IDs arbitrarily
    # NOTE: This is very dangerous if performed remotely in Ray and if IDs are used locally, since they may conflict
    if schema_options.schema is None:
        return cast(List[ColID], list(range(len(table.column_names))))

    col_ids = []
    for colname in table.column_names:
        expr = schema_options.schema.get_expression_by_name(colname)
        if expr is None:
            raise ValueError(f"Column {colname} not found in provided schema: {schema_options.schema}")
        col_ids.append(expr.get_id())

    return col_ids


@dataclass(frozen=True)
class PyListTile:
    column_id: ColID
    column_name: str
    partition_id: PartID
    block: DataBlock

    def __len__(self) -> int:
        return len(self.block)

    def apply(self, func: Callable[[DataBlock], DataBlock]) -> PyListTile:
        return dataclasses.replace(self, block=func(self.block))

    def split_by_index(
        self,
        num_partitions: int,
        pivots: np.ndarray,
        target_partitions: np.ndarray,
        argsorted_target_partition_indices: DataBlock,
    ) -> list[PyListTile]:
        assert len(argsorted_target_partition_indices) == len(self)
        new_blocks = self.block.partition(
            num_partitions,
            pivots=pivots,
            target_partitions=target_partitions,
            argsorted_targets=argsorted_target_partition_indices,
        )
        assert len(new_blocks) == num_partitions
        return [dataclasses.replace(self, block=nb, partition_id=i) for i, nb in enumerate(new_blocks)]

    @classmethod
    def merge_tiles(cls, to_merge: list[PyListTile], verify_partition_id: bool = True) -> PyListTile:
        assert len(to_merge) > 0

        if len(to_merge) == 1:
            return to_merge[0]

        column_id = to_merge[0].column_id
        partition_id = to_merge[0].partition_id
        column_name = to_merge[0].column_name
        # first perform sanity check
        for part in to_merge[1:]:
            assert part.column_id == column_id
            assert not verify_partition_id or part.partition_id == partition_id
            assert part.column_name == column_name

        merged_block = DataBlock.merge_blocks([t.block for t in to_merge])
        return dataclasses.replace(to_merge[0], block=merged_block)

    def replace_block(self, block: DataBlock) -> PyListTile:
        return dataclasses.replace(self, block=block)


@dataclass(frozen=True)
class vPartition:
    columns: dict[ColID, PyListTile]
    partition_id: PartID

    def __post_init__(self) -> None:
        size = None
        for col_id, tile in self.columns.items():
            if tile.partition_id != self.partition_id:
                raise ValueError(f"mismatch of partition id: {tile.partition_id} vs {self.partition_id}")
            if size is None:
                size = len(tile)
            if len(tile) != size:
                raise ValueError(f"mismatch of tile lengths: {len(tile)} vs {size}")
            if col_id != tile.column_id:
                raise ValueError(f"mismatch of column id: {col_id} vs {tile.column_id}")

    def __len__(self) -> int:
        if len(self.columns) == 0:
            return 0
        return len(next(iter(self.columns.values())))

    def get_unresolved_col_expressions(self) -> ExpressionList:
        """Generates column expressions that represent the vPartition's schema"""
        colexprs = []
        for _, tile in self.columns.items():
            col_name = tile.column_name
            col_type: ExpressionType
            if isinstance(tile.block, ArrowDataBlock):
                col_type = ExpressionType.from_arrow_type(tile.block.data.type)
            else:
                py_types = {type(obj) for obj in tile.block.data} - {type(None)}
                if len(py_types) == 0:
                    col_type = PythonExpressionType(type(None))
                elif len(py_types) == 1:
                    col_type = PythonExpressionType(py_types.pop())
                else:
                    col_type = ExpressionType.python_object()
            colexpr = ColumnExpression(name=col_name, expr_type=col_type)
            colexprs.append(colexpr)
        return ExpressionList(colexprs)

    def eval_expression(self, expr: Expression) -> PyListTile:
        expr_col_id = expr.get_id()
        expr_name = expr.name()

        assert expr_col_id is not None
        assert expr_name is not None
        if not expr.has_call():
            return PyListTile(
                column_id=expr_col_id,
                column_name=expr_name,
                partition_id=self.partition_id,
                block=self.columns[expr_col_id].block,
            )

        required_cols = expr.required_columns()
        required_blocks = {}
        for c in required_cols:
            col_id = c.get_id()
            assert col_id is not None
            block = self.columns[col_id].block
            name = c.name()
            assert name is not None
            required_blocks[name] = block
        exec = ExpressionExecutor()
        result = exec.eval(expr, required_blocks)
        expr_col_id = expr.get_id()
        expr_name = expr.name()
        assert expr_col_id is not None
        assert expr_name is not None
        return PyListTile(column_id=expr_col_id, column_name=expr_name, partition_id=self.partition_id, block=result)

    def eval_expression_list(self, exprs: ExpressionList) -> vPartition:
        tile_list = [self.eval_expression(e) for e in exprs]
        new_columns = {t.column_id: t for t in tile_list}
        return vPartition(columns=new_columns, partition_id=self.partition_id)

    @classmethod
    def from_arrow_table(cls, table: pa.Table, column_ids: list[ColID], partition_id: PartID) -> vPartition:
        names = table.column_names
        assert len(names) == len(column_ids)
        tiles = {}
        for i, (col_id, name) in enumerate(zip(column_ids, names)):
            arr = table[i]
            block: DataBlock[ArrowArrType] = DataBlock.make_block(arr)
            tiles[col_id] = PyListTile(column_id=col_id, column_name=name, partition_id=partition_id, block=block)
        return vPartition(columns=tiles, partition_id=partition_id)

    @classmethod
    def from_pydict(cls, data: dict[str, list[Any]], schema: ExpressionList, partition_id: PartID) -> vPartition:
        column_exprs = schema.to_column_expressions()
        tiles = {}
        for col_expr in column_exprs:
            col_id = col_expr.get_id()
            col_name = col_expr.name()
            col_type = col_expr.resolved_type()
            col_data = data[col_name]

            if ExpressionType.is_py(col_type):
                col_data = list(col_data)
            else:
                if not isinstance(col_data, pa.Array) and not isinstance(col_data, pa.ChunkedArray):
                    col_data = pa.array(col_data, type=col_type.to_arrow_type())

            block = DataBlock.make_block(col_data)
            tiles[col_id] = PyListTile(column_id=col_id, column_name=col_name, partition_id=partition_id, block=block)
        return vPartition(columns=tiles, partition_id=partition_id)

    @classmethod
    def from_csv(
        cls,
        path: str,
        partition_id: PartID,
        csv_options: vPartitionParseCSVOptions = vPartitionParseCSVOptions(),
        schema_options: vPartitionSchemaInferenceOptions = vPartitionSchemaInferenceOptions(),
        read_options: vPartitionReadOptions = vPartitionReadOptions(),
    ) -> vPartition:
        """Gets a vPartition from a CSV file.

        Args:
            path: FSSpec compatible path to the CSV file.
            partition_id: Partition ID to assign to the vPartition.
            csv_options: Options for parsing the CSV file.
            schema_options: Options for inferring the schema from the CSV file.
            read_options: Options for building a vPartition.
        """
        # Use provided CSV column names, or None if nothing provided
        full_column_names = schema_options.full_schema_column_names()

        # Have PyArrow generate the column names if the CSV has no header and no column names were provided
        pyarrow_autogenerate_column_names = (not csv_options.has_headers) and (full_column_names is None)

        # Have Pyarrow skip the header row if column names were provided, and a header exists in the CSV
        skip_header_row = full_column_names is not None and csv_options.has_headers
        pyarrow_skip_rows_after_names = (1 if skip_header_row else 0) + csv_options.skip_rows_after_header

        fs = get_filesystem_from_path(path)
        with fs.open(path, compression="infer") as f:

            if read_options.num_rows is not None:
                f = _limit_num_rows(f, read_options.num_rows)

            table = csv.read_csv(
                f,
                parse_options=csv.ParseOptions(
                    delimiter=csv_options.delimiter,
                ),
                # skip_rows applied, header row is read if column_names is not None, skip_rows_after_names is applied
                read_options=csv.ReadOptions(
                    autogenerate_column_names=pyarrow_autogenerate_column_names,
                    column_names=full_column_names,
                    skip_rows_after_names=pyarrow_skip_rows_after_names,
                    skip_rows=csv_options.skip_rows_before_header,
                ),
                convert_options=csv.ConvertOptions(include_columns=read_options.column_names),
            )

        column_ids = _get_column_ids(table, schema_options)
        return vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)

    @classmethod
    def from_json(
        cls,
        path: str,
        partition_id: PartID,
        schema_options: vPartitionSchemaInferenceOptions = vPartitionSchemaInferenceOptions(),
        read_options: vPartitionReadOptions = vPartitionReadOptions(),
    ) -> vPartition:
        """Gets a vPartition from a Line-delimited JSON file

        Args:
            path: FSSpec compatible path to the Line-delimited JSON file.
            partition_id: Partition ID to assign to the vPartition.
            schema_options: Options for inferring the schema from the JSON file.
            read_options: Options for building a vPartition.
        """
        fs = get_filesystem_from_path(path)
        with fs.open(path, compression="infer") as f:
            if read_options.num_rows is not None:
                f = _limit_num_rows(f, read_options.num_rows)
            table = json.read_json(f)

        if read_options.column_names is not None:
            table = table.select(read_options.column_names)

        column_ids = _get_column_ids(table, schema_options)
        return vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)

    @classmethod
    def from_parquet(
        cls,
        path: str,
        partition_id: PartID,
        schema_options: vPartitionSchemaInferenceOptions = vPartitionSchemaInferenceOptions(),
        read_options: vPartitionReadOptions = vPartitionReadOptions(),
    ) -> vPartition:
        """Gets a vPartition from a Parquet file

        Args:
            path: FSSpec compatible path to the Parquet file.
            partition_id: Partition ID to assign to the vPartition.
            schema_options: Options for inferring the schema from the Parquet file.
            read_options: Options for building a vPartition.
        """
        fs = get_filesystem_from_path(path)

        with fs.open(path) as f:
            # If no rows required, we manually construct an empty table with the right schema
            if read_options.num_rows == 0:
                arrow_schema = parquet.ParquetFile(f).metadata.schema.to_arrow_schema()
                table = pa.Table.from_arrays(
                    [pa.array([], type=field.type) for field in arrow_schema], schema=arrow_schema
                )
            else:
                table = parquet.read_table(
                    f,
                    columns=read_options.column_names,
                )
                # PyArrow API does not provide a way to limit the number of rows read from a Parquet file
                if read_options.num_rows is not None:
                    table = table.slice(length=read_options.num_rows)

        column_ids = _get_column_ids(table, schema_options)
        return vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)

    def to_pydict(self) -> dict[str, Sequence]:
        output_schema = [(tile.column_name, id) for id, tile in self.columns.items()]
        return {name: self.columns[id].block.data for name, id in output_schema}

    def to_pandas(self, schema: ExpressionList | None = None) -> pd.DataFrame:
        if schema is not None:
            output_schema = [(expr.name(), expr.get_id()) for expr in schema]
        else:
            output_schema = [(tile.column_name, id) for id, tile in self.columns.items()]
        return pd.DataFrame(
            {
                name: pd.Series(self.columns[id].block.data)
                if isinstance(self.columns[id].block, PyListDataBlock)
                else self.columns[id].block.data.to_pandas()
                for name, id in output_schema
            }
        )

    def for_each_column_block(self, func: Callable[[DataBlock], DataBlock]) -> vPartition:
        return dataclasses.replace(self, columns={col_id: col.apply(func) for col_id, col in self.columns.items()})

    def head(self, num: int) -> vPartition:
        # TODO make optimization for when num=0
        return self.for_each_column_block(partial(DataBlock.head, num=num))

    def sample(self, num: int) -> vPartition:
        if len(self) <= num:
            return self
        sample_idx: DataBlock[ArrowArrType] = DataBlock.make_block(data=np.random.randint(0, len(self), num))
        return self.for_each_column_block(partial(DataBlock.take, indices=sample_idx))

    def filter(self, predicate: ExpressionList) -> vPartition:
        mask_list = self.eval_expression_list(predicate)
        assert len(mask_list.columns) > 0
        mask = next(iter(mask_list.columns.values())).block
        for to_and in mask_list.columns.values():
            mask = mask.run_binary_operator(to_and.block, OperatorEnum.AND)
        return self.for_each_column_block(partial(DataBlock.filter, mask=mask))

    def argsort(self, sort_keys: ExpressionList, descending: list[bool] | None = None) -> DataBlock:
        sorted = self.eval_expression_list(sort_keys)
        keys = list(sorted.columns.keys())

        if descending is None:
            descending = [False for _ in keys]

        idx = DataBlock.argsort([sorted.columns[k].block for k in keys], descending=descending)
        return idx

    def sort(self, sort_keys: ExpressionList, descending: list[bool] | None = None) -> vPartition:
        idx = self.argsort(sort_keys=sort_keys, descending=descending)
        return self.take(idx)

    def search_sorted(self, keys: vPartition, input_reversed: list[bool] | None = None) -> DataBlock:
        assert self.columns.keys() == keys.columns.keys()
        col_ids = list(self.columns.keys())
        idx = DataBlock.search_sorted(
            [self.columns[k].block for k in col_ids],
            [keys.columns[k].block for k in col_ids],
            input_reversed=input_reversed,
        )
        return idx

    def take(self, indices: DataBlock) -> vPartition:
        return self.for_each_column_block(partial(DataBlock.take, indices=indices))

    def agg(self, to_agg: list[tuple[Expression, str]], group_by: ExpressionList | None = None) -> vPartition:
        evaled_expressions = self.eval_expression_list(ExpressionList([e for e, _ in to_agg]))
        ops = [op for _, op in to_agg]
        if group_by is None:
            agged = {}
            for op, (col_id, tile) in zip(ops, evaled_expressions.columns.items()):
                agged[col_id] = tile.apply(func=partial(tile.block.__class__.agg, op=op))
            return vPartition(partition_id=self.partition_id, columns=agged)
        else:
            grouped_blocked = self.eval_expression_list(group_by)
            assert len(evaled_expressions.columns) == len(ops)
            gcols, acols = DataBlock.group_by_agg(
                list(tile.block for tile in grouped_blocked.columns.values()),
                list(tile.block for tile in evaled_expressions.columns.values()),
                agg_ops=ops,
            )
            new_columns = {}

            for block, (col_id, tile) in zip(gcols, grouped_blocked.columns.items()):
                new_columns[col_id] = dataclasses.replace(tile, block=block)

            for block, (col_id, tile) in zip(acols, evaled_expressions.columns.items()):
                new_columns[col_id] = dataclasses.replace(tile, block=block)
            return vPartition(partition_id=self.partition_id, columns=new_columns)

    def split_by_hash(self, exprs: ExpressionList, num_partitions: int) -> list[vPartition]:
        values_to_hash = self.eval_expression_list(exprs)
        keys = list(values_to_hash.columns.keys())
        keys.sort()
        hsf = None
        assert len(keys) > 0
        for k in keys:
            block = values_to_hash.columns[k].block
            hsf = block.array_hash(seed=hsf)
        assert hsf is not None
        target_idx = hsf.run_binary_operator(num_partitions, OperatorEnum.MOD)
        return self.split_by_index(num_partitions, target_partition_indices=target_idx)

    def split_by_index(self, num_partitions: int, target_partition_indices: DataBlock) -> list[vPartition]:
        assert len(target_partition_indices) == len(self)
        new_partition_to_columns: list[dict[ColID, PyListTile]] = [{} for _ in range(num_partitions)]
        argsort_targets = DataBlock.argsort([target_partition_indices])
        sorted_targets = target_partition_indices.take(argsort_targets)
        sorted_targets_np = sorted_targets.to_numpy()
        pivots = np.where(np.diff(sorted_targets_np, prepend=np.nan))[0]
        target_partitions = sorted_targets_np[pivots]

        for col_id, tile in self.columns.items():
            new_tiles = tile.split_by_index(
                num_partitions=num_partitions,
                pivots=pivots,
                target_partitions=target_partitions,
                argsorted_target_partition_indices=argsort_targets,
            )
            for part_id, nt in enumerate(new_tiles):
                new_partition_to_columns[part_id][col_id] = nt

        return [vPartition(partition_id=i, columns=columns) for i, columns in enumerate(new_partition_to_columns)]

    def quantiles(self, num: int) -> vPartition:
        self_size = len(self)
        if self_size == 0:
            return self
        sample_idx_np = (
            np.minimum(np.linspace(self_size / num, self_size, num), self_size - 1).round().astype(np.int32)[:-1]
        )
        return self.take(DataBlock.make_block(sample_idx_np))

    def explode(self, columns: ExpressionList) -> vPartition:
        partition_to_explode = self.eval_expression_list(columns)
        exploded_col_names = {tile.column_name for tile in partition_to_explode.columns.values()}
        partition_to_repeat = vPartition(
            {cid: tile for cid, tile in self.columns.items() if tile.column_name not in exploded_col_names},
            partition_id=self.partition_id,
        )

        exploded_cols = {}
        found_list_lengths = None
        for col_id, tile in partition_to_explode.columns.items():
            exploded_block, list_lengths = tile.block.list_explode()

            # Ensure that each row has the same length as other rows
            if found_list_lengths is None:
                found_list_lengths = list_lengths
            else:
                if list_lengths != found_list_lengths:
                    raise RuntimeError(
                        ".explode expects columns to have the same length in each row, "
                        "but found row(s) with mismatched lengths"
                    )

            exploded_cols[col_id] = PyListTile(
                column_id=tile.column_id,
                column_name=tile.column_name,
                partition_id=tile.partition_id,
                block=exploded_block,
            )
        assert found_list_lengths is not None, "At least one column must be specified to explode"

        if len(found_list_lengths) == 0:
            return vPartition(
                {
                    **exploded_cols,
                    **partition_to_repeat.columns,
                },
                partition_id=self.partition_id,
            )

        # Use the `found_list_lengths` to generate an array of indices to take from other columns (e.g. [0, 0, 1, 1, 1, 2, ...])
        list_length_cumsum = found_list_lengths.to_numpy().cumsum()
        take_indices = np.zeros(list_length_cumsum[-1], dtype="int64")
        take_indices[0] = 1
        take_indices[list_length_cumsum[:-1]] = 1
        take_indices = take_indices.cumsum()
        take_indices = take_indices - 1

        repeated_partition = partition_to_repeat.take(DataBlock.make_block(take_indices))
        return vPartition({**exploded_cols, **repeated_partition.columns}, partition_id=self.partition_id)

    def join(
        self,
        right: vPartition,
        left_on: ExpressionList,
        right_on: ExpressionList,
        output_schema: ExpressionList,
        how: str = "inner",
    ) -> vPartition:
        assert how == "inner"
        left_key_part = self.eval_expression_list(left_on)
        left_key_ids = list(left_key_part.columns.keys())

        right_key_part = right.eval_expression_list(right_on)
        left_key_list = [left_key_part.columns[le.get_id()].block for le in left_on]
        right_key_list = [right_key_part.columns[re.get_id()].block for re in right_on]

        left_nonjoin_ids = [i for i in self.columns.keys() if i not in left_key_part.columns]
        right_nonjoin_ids = [i for i in right.columns.keys() if i not in right_key_part.columns]

        left_nonjoin_blocks = [self.columns[i].block for i in left_nonjoin_ids]
        right_nonjoin_blocks = [right.columns[i].block for i in right_nonjoin_ids]

        joined_blocks = DataBlock.join(left_key_list, right_key_list, left_nonjoin_blocks, right_nonjoin_blocks)

        result_keys = left_key_ids + left_nonjoin_ids + right_nonjoin_ids

        assert len(joined_blocks) == len(result_keys)
        joined_block_idx = 0
        result_columns = {}
        for k in left_key_ids:
            result_columns[k] = left_key_part.columns[k].replace_block(block=joined_blocks[joined_block_idx])
            joined_block_idx += 1

        for k in left_nonjoin_ids:
            result_columns[k] = self.columns[k].replace_block(block=joined_blocks[joined_block_idx])
            joined_block_idx += 1

        for k in right_nonjoin_ids:
            result_columns[k] = right.columns[k].replace_block(block=joined_blocks[joined_block_idx])
            joined_block_idx += 1

        assert joined_block_idx == len(result_keys)

        output = vPartition(columns=result_columns, partition_id=self.partition_id)
        return output.eval_expression_list(output_schema)

    def _to_file(
        self,
        file_format: str,
        root_path: str,
        partition_cols: ExpressionList | None = None,
        compression: str | None = None,
    ) -> list[str]:
        keys = [col_id for col_id in self.columns.keys()]
        names = [self.columns[k].column_name for k in keys]
        data = [self.columns[k].block.to_arrow() for k in keys]
        arrow_table = pa.table(data, names=names)
        partition_col_names = []
        if partition_cols is not None:
            for col in partition_cols:
                assert isinstance(
                    col, ColumnExpression
                ), "we can only support ColumnExpressions for partitioning parquet"
                col_id = col.get_id()
                assert col_id is not None
                assert col_id in keys
                partition_col_names.append(self.columns[col_id].column_name)

        visited_paths = []

        def file_visitor(written_file):
            visited_paths.append(written_file.path)

        format: pada.FileFormat
        opts = None

        if file_format == "parquet":
            format = pada.ParquetFileFormat()
            opts = format.make_write_options(compression=compression)
        elif file_format == "csv":
            format = pada.CsvFileFormat()
            assert compression is None

        pada.write_dataset(
            arrow_table,
            base_dir=root_path,
            basename_template=str(uuid4()) + "-{i}." + format.default_extname,
            format=format,
            partitioning=partition_col_names,
            file_options=opts,
            file_visitor=file_visitor,
            use_threads=False,
            existing_data_behavior="overwrite_or_ignore",
        )
        return visited_paths

    def to_parquet(
        self, root_path: str, partition_cols: ExpressionList | None = None, compression: str | None = None
    ) -> list[str]:
        return self._to_file("parquet", root_path=root_path, partition_cols=partition_cols, compression=compression)

    def to_csv(
        self, root_path: str, partition_cols: ExpressionList | None = None, compression: str | None = None
    ) -> list[str]:
        return self._to_file("csv", root_path=root_path, partition_cols=partition_cols, compression=compression)

    @classmethod
    def merge_partitions(cls, to_merge: list[vPartition], verify_partition_id: bool = True) -> vPartition:
        assert len(to_merge) > 0

        if len(to_merge) == 1:
            return to_merge[0]

        pid = to_merge[0].partition_id
        col_ids = set(to_merge[0].columns.keys())
        # first perform sanity check
        for part in to_merge[1:]:
            assert not verify_partition_id or part.partition_id == pid
            assert set(part.columns.keys()) == col_ids

        new_columns = {}
        for col_id in to_merge[0].columns.keys():
            new_columns[col_id] = PyListTile.merge_tiles(
                [vp.columns[col_id] for vp in to_merge], verify_partition_id=verify_partition_id
            )
        return dataclasses.replace(to_merge[0], columns=new_columns)


PartitionT = TypeVar("PartitionT")


class PartitionSet(Generic[PartitionT]):
    def _get_merged_vpartition(self) -> vPartition:
        raise NotImplementedError()

    def to_pydict(self) -> dict[str, Sequence]:
        """Retrieves all the data in a PartitionSet as a Python dictionary. Values are the raw data from each Block."""
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_pydict()

    def to_pandas(self, schema: ExpressionList | None = None) -> pd.DataFrame:
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_pandas(schema=schema)

    def items(self) -> list[tuple[PartID, PartitionT]]:
        """
        Returns all (partition id, partition) in this PartitionSet,
        ordered by partition ID.
        """
        raise NotImplementedError()

    def values(self) -> list[PartitionT]:
        return [value for _, value in self.items()]

    @abstractmethod
    def get_partition(self, idx: PartID) -> PartitionT:
        raise NotImplementedError()

    @abstractmethod
    def set_partition(self, idx: PartID, part: PartitionT) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete_partition(self, idx: PartID) -> None:
        raise NotImplementedError()

    @abstractmethod
    def has_partition(self, idx: PartID) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    @abstractmethod
    def len_of_partitions(self) -> list[int]:
        raise NotImplementedError()

    @abstractmethod
    def num_partitions(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def wait(self) -> None:
        raise NotImplementedError()


@dataclass(eq=False, repr=False)
class PartitionCacheEntry:
    key: str
    value: PartitionSet | None

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PartitionCacheEntry) and self.key == other.key

    def __repr__(self) -> str:
        return f"PartitionCacheEntry: {self.key}"

    def __getstate__(self):
        return self.key

    def __setstate__(self, key):
        self.key = key
        self.value = None


class PartitionSetCache:
    def __init__(self) -> None:
        self._uuid_to_partition_set: weakref.WeakValueDictionary[
            str, PartitionCacheEntry
        ] = weakref.WeakValueDictionary()

    def get_partition_set(self, pset_id: str) -> PartitionCacheEntry:
        assert pset_id in self._uuid_to_partition_set
        return self._uuid_to_partition_set[pset_id]

    def put_partition_set(self, pset: PartitionSet) -> PartitionCacheEntry:
        pset_id = uuid4().hex
        part_entry = PartitionCacheEntry(pset_id, pset)
        self._uuid_to_partition_set[pset_id] = part_entry
        return part_entry

    def rm(self, pset_id: str) -> None:
        if pset_id in self._uuid_to_partition_set:
            del self._uuid_to_partition_set[pset_id]

    def clear(self) -> None:
        del self._uuid_to_partition_set
        self._uuid_to_partition_set = weakref.WeakValueDictionary()


class PartitionSetFactory(Generic[PartitionT]):
    """Factory class for creating PartitionSets."""

    FILEPATH_COLUMN_NAME = "filepath"

    @abstractmethod
    def glob_filepaths(
        self,
        source_path: str,
    ) -> tuple[PartitionSet[PartitionT], ExpressionList]:
        """Globs the specified filepath to construct a PartitionSet of file metadata

        Args:
            source_path (str): path to glob

        Returns:
            PartitionSet[PartitionT]: PartitionSet containing the files' metadata
            ExpressionList: Schema of the PartitionSet that was constructed
        """
        raise NotImplementedError()
