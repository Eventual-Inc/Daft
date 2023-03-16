from __future__ import annotations

import dataclasses
import io
import pathlib
import weakref
from abc import abstractmethod
from dataclasses import dataclass
from functools import partial
from typing import IO, Any, Callable, Generic, TypeVar
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import csv
from pyarrow import dataset as pada
from pyarrow import json, parquet

from daft.execution.operators import OperatorEnum
from daft.expressions import Expression, ExpressionExecutor, ExpressionList
from daft.filesystem import get_filesystem_from_path
from daft.logical.field import Field
from daft.logical.schema import Schema
from daft.runners.blocks import ArrowArrType, ArrowDataBlock, DataBlock, PyListDataBlock
from daft.types import DatatypeInference, ExpressionType

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

    schema: Schema | None = None
    inference_column_names: list[str] | None = None

    def full_schema_column_names(self) -> list[str] | None:
        """Returns all column names for the schema, or None if not provided."""
        if self.schema is not None:
            return self.schema.column_names()
        return self.inference_column_names


@dataclass(frozen=True)
class vPartitionParseCSVOptions:
    """Options for parsing CSVs

    Args:
        delimiter: The delimiter to use when parsing CSVs, defaults to ","
        has_headers: Whether the CSV has headers, defaults to True
        column_names: Column names to use in place of headers, defaults to None
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


@dataclass(frozen=True)
class PyListTile:
    column_name: str
    block: DataBlock

    def __len__(self) -> int:
        return len(self.block)

    def apply(self, func: Callable[[DataBlock], DataBlock]) -> PyListTile:
        return dataclasses.replace(self, block=func(self.block))

    def size_bytes(self) -> int:
        return self.block.size_bytes()

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
        return [dataclasses.replace(self, block=nb) for i, nb in enumerate(new_blocks)]

    @classmethod
    def merge_tiles(cls, to_merge: list[PyListTile]) -> PyListTile:
        assert len(to_merge) > 0

        if len(to_merge) == 1:
            return to_merge[0]

        column_name = to_merge[0].column_name
        # first perform sanity check
        for part in to_merge[1:]:
            assert part.column_name == column_name

        merged_block = DataBlock.merge_blocks([t.block for t in to_merge])
        return dataclasses.replace(to_merge[0], block=merged_block)

    def replace_block(self, block: DataBlock) -> PyListTile:
        return dataclasses.replace(self, block=block)


@dataclass(frozen=True)
class PartialPartitionMetadata:
    num_rows: None | int
    size_bytes: None | int


@dataclass(frozen=True)
class PartitionMetadata(PartialPartitionMetadata):
    num_rows: int
    size_bytes: int

    @classmethod
    def from_table(cls, table: vPartition) -> PartitionMetadata:
        return PartitionMetadata(
            num_rows=len(table),
            size_bytes=table.size_bytes(),
        )


@dataclass(frozen=True)
class vPartition:
    columns: dict[str, PyListTile]

    # Temporary workaround for empty blocks affecting .schema
    # Users of vPartition can explicitly pass in a schema to override the "schema inferencing" behavior
    # This will be removed once we move to Rust Tables, which are schema-aware.
    override_schema: Schema | None = None

    def __post_init__(self) -> None:
        size = None
        for name, tile in self.columns.items():
            if len(tile) != 0 and size is None:
                size = len(tile)
            if len(tile) != 0 and len(tile) != size:
                raise ValueError(f"mismatch of tile lengths: {len(tile)} vs {size}")
            if name != tile.column_name:
                raise ValueError(f"mismatch of tile name: {name} vs {tile.column_name}")

    def __len__(self) -> int:
        if len(self.columns) == 0:
            return 0
        return len(next(iter(self.columns.values())))

    def size_bytes(self) -> int:
        return sum(tile.size_bytes() for tile in self.columns.values())

    def schema(self) -> Schema:
        """Generates column expressions that represent the vPartition's schema"""
        if self.override_schema is None:
            return self._infer_schema_from_columns()
        return self.override_schema

    def _infer_schema_from_columns(self) -> Schema:
        fields = []
        for _, tile in self.columns.items():
            col_name = tile.column_name
            col_type: ExpressionType
            if isinstance(tile.block, ArrowDataBlock):
                col_type = ExpressionType.from_arrow_type(tile.block.data.type)
            else:
                py_types = {type(obj) for obj in tile.block.data} - {type(None)}
                if len(py_types) == 0:
                    col_type = ExpressionType.python(type(None))
                elif len(py_types) == 1:
                    col_type = ExpressionType.python(py_types.pop())
                else:
                    col_type = ExpressionType.python_object()
            field = Field(col_name, col_type)
            fields.append(field)
        return Schema._from_field_name_and_types([(f.name, f.dtype) for f in fields])

    def eval_expression(self, expr: Expression) -> PyListTile:
        expr_name = expr.name()

        assert expr_name is not None

        required_cols = expr._required_columns()
        required_blocks = {}
        for name in required_cols:
            block = self.columns[name].block
            required_blocks[name] = block
        exec = ExpressionExecutor()
        result = exec.eval(expr, required_blocks)
        expr_name = expr.name()
        assert expr_name is not None
        return PyListTile(column_name=expr_name, block=result)

    def eval_expression_list(self, exprs: ExpressionList) -> vPartition:
        tile_list = [self.eval_expression(e) for e in exprs]
        new_columns = {t.column_name: t for t in tile_list}
        return vPartition(columns=new_columns)

    @classmethod
    def from_arrow_table(cls, table: pa.Table) -> vPartition:
        names = table.column_names
        tiles = {}
        for i, name in enumerate(names):
            arr = table[i]
            block: DataBlock[ArrowArrType] = DataBlock.make_block(arr)
            tiles[name] = PyListTile(column_name=name, block=block)

        # Infer the schema from Arrow and pass that in explicitly as the inferred schema
        inferred_schema = Schema._from_field_name_and_types(
            [(cname, ExpressionType.from_arrow_type(table[cname].type)) for cname in table.column_names]
        )

        return vPartition(columns=tiles, override_schema=inferred_schema)

    @classmethod
    def from_pydict(
        cls,
        data: dict[str, list[Any] | np.ndarray | pa.Array | pa.ChunkedArray],
    ) -> vPartition:
        column_types = {header: DatatypeInference.infer_type(data[header]) for header in data}
        schema = Schema._from_field_name_and_types(list(column_types.items()))
        tiles = {}
        for f in schema:
            # Coerce the column data into a list or PyArrow array depending on the provided schema
            col_name = f.name
            col_type = f.dtype
            col_data: list | pa.Array
            if col_type._is_python_type():
                col_array = data[col_name]
                if isinstance(col_array, list):
                    col_data = col_array
                elif isinstance(col_array, pa.Array) or isinstance(col_array, pa.ChunkedArray):
                    col_data = col_array.to_pylist()
                elif isinstance(col_array, np.ndarray):
                    col_data = list(col_array)
                else:
                    raise NotImplementedError(
                        f"Unable to coerce {type(col_array)} into a Daft DataFrame for column: {col_name}"
                    )
            else:
                if isinstance(data[col_name], pa.Array) or isinstance(data[col_name], pa.ChunkedArray):
                    col_data = data[col_name]
                else:
                    col_data = pa.array(data[col_name], type=col_type.to_arrow_type())

            block = DataBlock.make_block(col_data)
            tiles[col_name] = PyListTile(column_name=col_name, block=block)
        return vPartition(columns=tiles)

    @classmethod
    def from_csv(
        cls,
        path: str,
        csv_options: vPartitionParseCSVOptions = vPartitionParseCSVOptions(),
        schema_options: vPartitionSchemaInferenceOptions = vPartitionSchemaInferenceOptions(),
        read_options: vPartitionReadOptions = vPartitionReadOptions(),
    ) -> vPartition:
        """Gets a vPartition from a CSV file.

        Args:
            path: FSSpec compatible path to the CSV file.
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

        return vPartition.from_arrow_table(table)

    @classmethod
    def from_json(
        cls,
        path: str,
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

        return vPartition.from_arrow_table(table)

    @classmethod
    def from_parquet(
        cls,
        path: str,
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
            pqf = parquet.ParquetFile(f)
            # If no rows required, we manually construct an empty table with the right schema
            if read_options.num_rows == 0:
                arrow_schema = pqf.metadata.schema.to_arrow_schema()
                table = pa.Table.from_arrays(
                    [pa.array([], type=field.type) for field in arrow_schema], schema=arrow_schema
                )
            elif read_options.num_rows is not None:
                # Read the file by rowgroup.
                tables = []
                rows_read = 0
                for i in range(pqf.metadata.num_row_groups):
                    tables.append(pqf.read_row_group(i, columns=read_options.column_names))
                    rows_read += len(tables[i])
                    if rows_read >= read_options.num_rows:
                        break

                table = pa.concat_tables(tables)
                table = table.slice(length=read_options.num_rows)

            else:
                table = parquet.read_table(
                    f,
                    columns=read_options.column_names,
                )

        return vPartition.from_arrow_table(table)

    def to_pydict(self) -> dict[str, list[Any]]:
        output_schema = [(tile.column_name, id) for id, tile in self.columns.items()]

        results = {}
        for name, id in output_schema:
            block = self.columns[id].block
            data = [block.data for _ in range(len(self))] if block.is_scalar() else list(block.iter_py())
            results[name] = data

        return results

    def to_pandas(self, schema: Schema | None = None) -> pd.DataFrame:
        if schema is not None:
            output_schema = [f.name for f in schema]
        else:
            output_schema = [tile for tile in self.columns.keys()]

        data = {}
        for name in output_schema:
            block = self.columns[name].block
            # PyListDataBlocks contain Python objects
            if isinstance(block, PyListDataBlock) and block.is_scalar():
                data[name] = pd.Series([block.data for _ in range(len(self))])
            elif isinstance(block, PyListDataBlock):
                data[name] = pd.Series(block.data)
            # ArrowDataBlocks contain Arrow scalars or arrays
            elif block.is_scalar():
                data[name] = pd.Series([block.data.as_py() for _ in range(len(self))])
            else:
                data[name] = block.data.to_pandas()

        return pd.DataFrame(data)

    def for_each_column_block(self, func: Callable[[DataBlock], DataBlock]) -> vPartition:
        return dataclasses.replace(self, columns={col_name: col.apply(func) for col_name, col in self.columns.items()})

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

    def take(self, indices: DataBlock) -> vPartition:
        return self.for_each_column_block(partial(DataBlock.take, indices=indices))

    def agg(self, to_agg: list[tuple[Expression, str]], group_by: ExpressionList | None = None) -> vPartition:
        evaled_expressions = self.eval_expression_list(ExpressionList([e for e, _ in to_agg]))
        ops = [op for _, op in to_agg]
        if group_by is None:
            agged = {}
            for op, (col_name, tile) in zip(ops, evaled_expressions.columns.items()):
                agged[col_name] = tile.apply(func=partial(tile.block.__class__.agg, op=op))
            return vPartition(columns=agged)
        else:
            grouped_blocked = self.eval_expression_list(group_by)
            assert len(evaled_expressions.columns) == len(ops)
            gcols, acols = DataBlock.group_by_agg(
                list(tile.block for tile in grouped_blocked.columns.values()),
                list(tile.block for tile in evaled_expressions.columns.values()),
                agg_ops=ops,
            )
            new_columns = {}

            for block, (col_name, tile) in zip(gcols, grouped_blocked.columns.items()):
                new_columns[col_name] = dataclasses.replace(tile, block=block)

            for block, (col_name, tile) in zip(acols, evaled_expressions.columns.items()):
                new_columns[col_name] = dataclasses.replace(tile, block=block)
            return vPartition(columns=new_columns)

    def search_sorted(self, sort_keys: vPartition, descending: list[bool]) -> DataBlock:
        assert sort_keys.columns.keys() == self.columns.keys(), "Sort keys must have same columns as boundaries"
        assert len(descending) == len(self.columns.keys()), "Ordering list must have same length as columns"
        return DataBlock.search_sorted(
            [self.columns[k].block for k in self.columns.keys()],
            [sort_keys.columns[k].block for k in self.columns.keys()],
            input_reversed=descending,
        )

    def split_random(self, num_partitions: int, seed: int) -> list[vPartition]:
        rng = np.random.default_rng(seed=seed)
        target_idx = DataBlock.make_block(data=rng.integers(low=0, high=num_partitions, size=len(self)))
        new_parts = self.split_by_index(num_partitions=num_partitions, target_partition_indices=target_idx)
        return new_parts

    def split_by_hash(self, exprs: ExpressionList, num_partitions: int) -> list[vPartition]:
        values_to_hash = self.eval_expression_list(exprs)
        keys = list(values_to_hash.columns.keys())
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
        new_partition_to_columns: list[dict[str, PyListTile]] = [{} for _ in range(num_partitions)]
        argsort_targets = DataBlock.argsort([target_partition_indices])
        sorted_targets = target_partition_indices.take(argsort_targets)
        sorted_targets_np = sorted_targets.to_numpy()
        pivots = np.where(np.diff(sorted_targets_np, prepend=np.nan))[0]
        target_partitions = sorted_targets_np[pivots]

        for name, tile in self.columns.items():
            new_tiles = tile.split_by_index(
                num_partitions=num_partitions,
                pivots=pivots,
                target_partitions=target_partitions,
                argsorted_target_partition_indices=argsort_targets,
            )
            for part_id, nt in enumerate(new_tiles):
                new_partition_to_columns[part_id][name] = nt

        return [vPartition(columns=columns) for columns in new_partition_to_columns]

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
            {name: tile for name, tile in self.columns.items() if name not in exploded_col_names},
        )

        exploded_cols = {}
        found_list_lengths = None
        for name, tile in partition_to_explode.columns.items():
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

            exploded_cols[name] = PyListTile(
                column_name=name,
                block=exploded_block,
            )
        assert found_list_lengths is not None, "At least one column must be specified to explode"

        if len(found_list_lengths) == 0:
            return vPartition(
                {
                    **exploded_cols,
                    **partition_to_repeat.columns,
                },
            )

        # Use the `found_list_lengths` to generate an array of indices to take from other columns (e.g. [0, 0, 1, 1, 1, 2, ...])
        list_length_cumsum = found_list_lengths.to_numpy().cumsum()
        take_indices = np.zeros(list_length_cumsum[-1], dtype="int64")
        take_indices[0] = 1
        take_indices[list_length_cumsum[:-1]] = 1
        take_indices = take_indices.cumsum()
        take_indices = take_indices - 1

        repeated_partition = partition_to_repeat.take(DataBlock.make_block(take_indices))
        return vPartition({**exploded_cols, **repeated_partition.columns})

    def join(
        self,
        right: vPartition,
        left_on: ExpressionList,
        right_on: ExpressionList,
        output_projection: ExpressionList,
        how: str = "inner",
    ) -> vPartition:
        assert how == "inner"
        left_key_part = self.eval_expression_list(left_on)
        left_key_ids = list(left_key_part.columns.keys())

        right_key_part = right.eval_expression_list(right_on)

        right_key_ids = list(right_key_part.columns.keys())

        left_key_list = [left_key_part.columns[le.name()].block for le in left_on]
        right_key_list = [right_key_part.columns[re.name()].block for re in right_on]

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
            assert k not in result_columns
            result_columns[k] = left_key_part.columns[k].replace_block(block=joined_blocks[joined_block_idx])

            joined_block_idx += 1

        for k in left_nonjoin_ids:
            assert k not in result_columns
            result_columns[k] = self.columns[k].replace_block(block=joined_blocks[joined_block_idx])
            joined_block_idx += 1

        for lk, rk in zip(left_key_ids, right_key_ids):
            if lk != rk:
                while rk in result_columns:
                    rk = f"right.{rk}"

                assert rk not in result_columns
                result_columns[rk] = PyListTile(column_name=rk, block=result_columns[lk].block)

        for k in right_nonjoin_ids:

            while k in result_columns:
                k = f"right.{k}"
            assert k not in result_columns
            result_columns[k] = PyListTile(column_name=k, block=joined_blocks[joined_block_idx])
            joined_block_idx += 1

        assert joined_block_idx == len(result_keys)
        output_ordering = output_projection.to_column_expressions()
        return vPartition(columns=result_columns).eval_expression_list(output_ordering)

    def _to_file(
        self,
        file_format: str,
        root_path: str | pathlib.Path,
        partition_cols: ExpressionList | None = None,
        compression: str | None = None,
    ) -> list[str]:
        keys = [col_name for col_name in self.columns.keys()]
        names = [self.columns[k].column_name for k in keys]
        data = [self.columns[k].block.to_arrow() for k in keys]
        arrow_table = pa.table(data, names=names)
        partition_col_names = []
        if partition_cols is not None:
            for col in partition_cols:
                assert col._is_column(), "we can only support Column Expressions for partitioning parquet"
                col_name = col.name()
                assert col_name is not None
                assert col_name in keys
                partition_col_names.append(col_name)

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
        self,
        root_path: str | pathlib.Path,
        partition_cols: ExpressionList | None = None,
        compression: str | None = None,
    ) -> list[str]:
        return self._to_file("parquet", root_path=root_path, partition_cols=partition_cols, compression=compression)

    def to_csv(
        self,
        root_path: str | pathlib.Path,
        partition_cols: ExpressionList | None = None,
        compression: str | None = None,
    ) -> list[str]:
        return self._to_file("csv", root_path=root_path, partition_cols=partition_cols, compression=compression)

    @classmethod
    def concat(cls, to_merge: list[vPartition]):
        assert len(to_merge) > 0

        if len(to_merge) == 1:
            return to_merge[0]

        col_names = set(to_merge[0].columns.keys())
        # first perform sanity check
        for part in to_merge[1:]:
            assert set(part.columns.keys()) == col_names

        new_columns = {}
        for col_name in to_merge[0].columns.keys():
            new_columns[col_name] = PyListTile.merge_tiles([vp.columns[col_name] for vp in to_merge])
        return vPartition(columns=new_columns)


PartitionT = TypeVar("PartitionT")


class PartitionSet(Generic[PartitionT]):
    def _get_merged_vpartition(self) -> vPartition:
        raise NotImplementedError()

    def to_pydict(self) -> dict[str, list[Any]]:
        """Retrieves all the data in a PartitionSet as a Python dictionary. Values are the raw data from each Block."""
        merged_partition = self._get_merged_vpartition()
        return merged_partition.to_pydict()

    def to_pandas(self, schema: Schema | None = None) -> pd.DataFrame:
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
