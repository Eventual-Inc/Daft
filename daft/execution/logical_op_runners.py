from __future__ import annotations

from abc import abstractmethod
from bisect import bisect_right
from itertools import accumulate
from typing import Callable, ClassVar, TypeVar

from daft.datasources import (
    CSVSourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    StorageType,
)
from daft.expressions import ColID
from daft.logical.logical_plan import (
    Coalesce,
    FileWrite,
    Filter,
    GlobalLimit,
    InMemoryScan,
    Join,
    LocalAggregate,
    LocalDistinct,
    LocalLimit,
    LogicalPlan,
    MapPartition,
    PartitionScheme,
    Projection,
    Repartition,
    Sort,
    TabularFilesScan,
)
from daft.logical.schema import ExpressionList
from daft.resource_request import ResourceRequest
from daft.runners.blocks import DataBlock
from daft.runners.partitioning import (
    PartitionSet,
    PyListTile,
    vPartition,
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    Shuffler,
    SortOp,
)


class LogicalPartitionOpRunner:
    @abstractmethod
    def run_node_list(
        self,
        inputs: dict[int, PartitionSet],
        nodes: list[LogicalPlan],
        num_partitions: int,
        resource_request: ResourceRequest,
    ):
        raise NotImplementedError()

    def run_node_list_single_partition(
        self, inputs: dict[int, vPartition], nodes: list[LogicalPlan], partition_id: int
    ) -> vPartition:
        part_set = {nid: part for nid, part in inputs.items()}
        for node in nodes:
            output = self.run_single_node(inputs=part_set, node=node, partition_id=partition_id)
            part_set[node.id()] = output
            for child in node._children():
                del part_set[child.id()]
        return output

    def run_single_node(self, inputs: dict[int, vPartition], node: LogicalPlan, partition_id: int) -> vPartition:
        if isinstance(node, TabularFilesScan):
            return self._handle_tabular_files_scan(inputs, node, partition_id=partition_id)
        elif isinstance(node, Projection):
            return self._handle_projection(inputs, node, partition_id=partition_id)
        elif isinstance(node, Filter):
            return self._handle_filter(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalLimit):
            return self._handle_local_limit(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalAggregate):
            return self._handle_local_aggregate(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalDistinct):
            return self._handle_local_distinct(inputs, node, partition_id=partition_id)
        elif isinstance(node, Join):
            return self._handle_join(inputs, node, partition_id=partition_id)
        elif isinstance(node, FileWrite):
            return self._handle_file_write(inputs, node, partition_id=partition_id)
        elif isinstance(node, MapPartition):
            return self._handle_map_partition(inputs, node, partition_id=partition_id)
        else:
            raise NotImplementedError(f"{type(node)} not implemented")

    def _handle_tabular_files_scan(
        self, inputs: dict[int, vPartition], scan: TabularFilesScan, partition_id: int
    ) -> vPartition:
        child_id = scan._children()[0].id()
        prev_partition = inputs[child_id]
        data = prev_partition.to_pydict()
        assert (
            scan._filepaths_column_name in data
        ), f"TabularFilesScan should be ran on vPartitions with '{scan._filepaths_column_name}' column"
        # HACK: Ignore type here because this is a PyArrow array
        filepaths = data[scan._filepaths_column_name].to_pylist()  # type: ignore

        # Common options for reading vPartition
        schema = scan._schema
        schema_options = vPartitionSchemaInferenceOptions(schema=schema)
        read_options = vPartitionReadOptions(
            num_rows=None,  # read all rows
            column_names=scan._column_names,  # read only specified columns
        )

        if scan._source_info.scan_type() == StorageType.CSV:
            assert isinstance(scan._source_info, CSVSourceInfo)
            return vPartition.merge_partitions(
                [
                    vPartition.from_csv(
                        path=fp,
                        partition_id=partition_id,
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
            return vPartition.merge_partitions(
                [
                    vPartition.from_json(
                        path=fp,
                        partition_id=partition_id,
                        schema_options=schema_options,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        elif scan._source_info.scan_type() == StorageType.PARQUET:
            assert isinstance(scan._source_info, ParquetSourceInfo)
            return vPartition.merge_partitions(
                [
                    vPartition.from_parquet(
                        path=fp,
                        partition_id=partition_id,
                        schema_options=schema_options,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        else:
            raise NotImplementedError(f"PyRunner has not implemented scan: {scan._source_info.scan_type()}")

    def _handle_projection(self, inputs: dict[int, vPartition], proj: Projection, partition_id: int) -> vPartition:
        child_id = proj._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.eval_expression_list(proj._projection)

    def _handle_filter(self, inputs: dict[int, vPartition], filter: Filter, partition_id: int) -> vPartition:
        predicate = filter._predicate
        child_id = filter._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.filter(predicate)

    def _handle_local_limit(self, inputs: dict[int, vPartition], limit: LocalLimit, partition_id: int) -> vPartition:
        num = limit._num
        child_id = limit._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.head(num)

    def _handle_local_aggregate(
        self, inputs: dict[int, vPartition], agg: LocalAggregate, partition_id: int
    ) -> vPartition:
        child_id = agg._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.agg(agg._agg, group_by=agg._group_by)

    def _handle_local_distinct(
        self, inputs: dict[int, vPartition], agg: LocalDistinct, partition_id: int
    ) -> vPartition:
        child_id = agg._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.agg([], group_by=agg._group_by)

    def _handle_join(self, inputs: dict[int, vPartition], join: Join, partition_id: int) -> vPartition:
        left_id = join._children()[0].id()
        right_id = join._children()[1].id()
        left_partition = inputs[left_id]
        right_partition = inputs[right_id]
        return left_partition.join(
            right_partition,
            left_on=join._left_on,
            right_on=join._right_on,
            output_schema=join.schema(),
            how=join._how.value,
        )

    def _handle_file_write(self, inputs: dict[int, vPartition], file_write: FileWrite, partition_id: int) -> vPartition:
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
        file_name_expr = output_schema.exprs[0]
        file_name_col_id = file_name_expr.get_id()
        columns: dict[ColID, PyListTile] = {}
        columns[file_name_col_id] = PyListTile(
            file_name_col_id,
            file_name_expr.name(),
            partition_id=partition_id,
            block=DataBlock.make_block(file_names),
        )
        return vPartition(
            columns,
            partition_id=partition_id,
        )

    def _handle_map_partition(
        self, inputs: dict[int, vPartition], map_partition: MapPartition, partition_id: int
    ) -> vPartition:
        child_id = map_partition._children()[0].id()
        prev_partition = inputs[child_id]
        return map_partition.eval_partition(prev_partition)


ReduceType = TypeVar("ReduceType")


class LogicalGlobalOpRunner:
    shuffle_ops: ClassVar[dict[type[ShuffleOp], type[Shuffler]]]

    def run_node_list(self, inputs: dict[int, PartitionSet], nodes: list[LogicalPlan]) -> PartitionSet:
        part_set = inputs.copy()
        for node in nodes:
            output = self.run_single_node(inputs=part_set, node=node)
            part_set[node.id()] = output
            for child in node._children():
                del part_set[child.id()]
        return output

    def run_single_node(self, inputs: dict[int, PartitionSet], node: LogicalPlan) -> PartitionSet:
        if isinstance(node, GlobalLimit):
            return self._handle_global_limit(inputs, node)
        elif isinstance(node, Repartition):
            return self._handle_repartition(inputs, node)
        elif isinstance(node, Sort):
            return self._handle_sort(inputs, node)
        elif isinstance(node, Coalesce):
            return self._handle_coalesce(inputs, node)
        elif isinstance(node, InMemoryScan):
            return self._handle_in_memory_scan(inputs, node)
        else:
            raise NotImplementedError(f"{type(node)} not implemented")

    @abstractmethod
    def map_partitions(
        self, pset: PartitionSet, func: Callable[[vPartition], vPartition], resource_request: ResourceRequest
    ) -> PartitionSet:
        raise NotImplementedError()

    @abstractmethod
    def reduce_partitions(self, pset: PartitionSet, func: Callable[[list[vPartition]], ReduceType]) -> ReduceType:
        raise NotImplementedError()

    def _get_shuffle_op_klass(self, t: type[ShuffleOp]) -> type[Shuffler]:
        return self.__class__.shuffle_ops[t]

    def _handle_in_memory_scan(self, inputs: dict[int, PartitionSet], im_scan: InMemoryScan) -> PartitionSet:
        from daft.context import get_context

        cache_entry = im_scan._cache_entry
        runner = get_context().runner()
        result: PartitionSet | None = runner.get_partition_set_from_cache(cache_entry.key).value
        assert result is not None
        return result

    def _handle_global_limit(self, inputs: dict[int, PartitionSet], limit: GlobalLimit) -> PartitionSet:
        child_id = limit._children()[0].id()
        prev_part = inputs[child_id]

        num = limit._num
        size_per_partition = prev_part.len_of_partitions()
        total_size = sum(size_per_partition)
        if total_size <= num:
            return prev_part

        cum_sum = list(accumulate(size_per_partition))
        where_to_cut_idx = bisect_right(cum_sum, num)
        count_so_far = cum_sum[where_to_cut_idx - 1]
        remainder = num - count_so_far
        assert remainder >= 0

        def limit_map_func(part: vPartition) -> vPartition:
            if part.partition_id < where_to_cut_idx:
                return part
            elif part.partition_id == where_to_cut_idx:
                return part.head(remainder)
            else:
                return part.head(0)

        return self.map_partitions(prev_part, limit_map_func, limit.resource_request())

    def _handle_repartition(self, inputs: dict[int, PartitionSet], repartition: Repartition) -> PartitionSet:
        child_id = repartition._children()[0].id()
        repartitioner: ShuffleOp
        if repartition._scheme == PartitionScheme.RANDOM:
            repartitioner = self._get_shuffle_op_klass(RepartitionRandomOp)(
                expr_eval_resource_request=ResourceRequest.default()
            )
        elif repartition._scheme == PartitionScheme.HASH:
            repartitioner = self._get_shuffle_op_klass(RepartitionHashOp)(
                expr_eval_resource_request=repartition.resource_request(),
                map_args={"exprs": repartition._partition_by},
            )
        else:
            raise NotImplementedError()
        prev_part = inputs[child_id]
        return repartitioner.run(input=prev_part, num_target_partitions=repartition.num_partitions())

    def _handle_sort(self, inputs: dict[int, PartitionSet], sort: Sort) -> PartitionSet:

        child_id = sort._children()[0].id()

        SAMPLES_PER_PARTITION = 20
        num_partitions = sort.num_partitions()
        exprs: ExpressionList = sort._sort_by
        descending = sort._descending

        def sample_map_func(part: vPartition) -> vPartition:
            return (
                part.sample(SAMPLES_PER_PARTITION)
                .eval_expression_list(exprs)
                .filter(ExpressionList([~e.to_column_expression().is_null() for e in exprs]).resolve(exprs))
            )

        def quantile_reduce_func(to_reduce: list[vPartition]) -> vPartition:
            merged = vPartition.merge_partitions(to_reduce, verify_partition_id=False)
            merged_sorted = merged.sort(exprs, descending=descending)
            return merged_sorted.quantiles(num_partitions)

        prev_part = inputs[child_id]
        sampled_partitions = self.map_partitions(prev_part, sample_map_func, sort.resource_request())
        boundaries = self.reduce_partitions(sampled_partitions, quantile_reduce_func)
        sort_shuffle_op_klass = self._get_shuffle_op_klass(SortOp)
        sort_op = sort_shuffle_op_klass(
            expr_eval_resource_request=sort.resource_request(),
            map_args={"exprs": exprs, "boundaries": boundaries, "descending": descending},
            reduce_args={"exprs": exprs, "descending": descending},
        )

        return sort_op.run(input=prev_part, num_target_partitions=num_partitions)

    def _handle_coalesce(self, inputs: dict[int, PartitionSet], coal: Coalesce) -> PartitionSet:
        child_id = coal._children()[0].id()
        num_partitions = coal.num_partitions()
        prev_part = inputs[child_id]

        coalesce_op_klass = self._get_shuffle_op_klass(CoalesceOp)

        coalesce_op = coalesce_op_klass(
            expr_eval_resource_request=ResourceRequest.default(),
            map_args={"num_input_partitions": prev_part.num_partitions()},
        )
        return coalesce_op.run(input=prev_part, num_target_partitions=num_partitions)
