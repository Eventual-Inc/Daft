from typing import Dict, List

from pyarrow import csv, parquet

from daft.datasources import (
    CSVSourceInfo,
    InMemorySourceInfo,
    ParquetSourceInfo,
    ScanType,
)
from daft.filesystem import get_filesystem_from_path
from daft.logical.logical_plan import (
    Filter,
    Join,
    LocalAggregate,
    LocalLimit,
    LogicalPlan,
    Projection,
    Scan,
)
from daft.runners.partitioning import vPartition


class LogicalPartitionOpRunner:
    def run_node_list(self, inputs: Dict[int, vPartition], nodes: List[LogicalPlan], partition_id: int) -> vPartition:
        part_set = inputs.copy()
        for node in nodes:
            output = self.run_single_node(inputs=part_set, node=node, partition_id=partition_id)
            part_set[node.id()] = output
            for child in node._children():
                del part_set[child.id()]
        return output

    def run_single_node(self, inputs: Dict[int, vPartition], node: LogicalPlan, partition_id: int) -> vPartition:
        if isinstance(node, Scan):
            return self._handle_scan(inputs, node, partition_id=partition_id)
        elif isinstance(node, Projection):
            return self._handle_projection(inputs, node, partition_id=partition_id)
        elif isinstance(node, Filter):
            return self._handle_filter(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalLimit):
            return self._handle_local_limit(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalAggregate):
            return self._handle_local_aggregate(inputs, node, partition_id=partition_id)
        elif isinstance(node, Join):
            return self._handle_join(inputs, node, partition_id=partition_id)
        else:
            raise NotImplementedError(f"{type(node)} not implemented")

    def _handle_scan(self, inputs: Dict[int, vPartition], scan: Scan, partition_id: int) -> vPartition:
        schema = scan.schema()
        column_ids = [col.get_id() for col in schema.to_column_expressions()]
        if scan._source_info.scan_type() == ScanType.IN_MEMORY:
            assert isinstance(scan._source_info, InMemorySourceInfo)
            table_len = [len(scan._source_info.data[key]) for key in scan._source_info.data][0]
            partition_size = table_len // scan._source_info.num_partitions
            start, end = (partition_size * partition_id, partition_size * (partition_id + 1))
            data = {key: scan._source_info.data[key][start:end] for key in scan._source_info.data}
            vpart = vPartition.from_pydict(data, schema=schema, partition_id=partition_id)
            return vpart
        elif scan._source_info.scan_type() == ScanType.CSV:
            assert isinstance(scan._source_info, CSVSourceInfo)
            path = scan._source_info.filepaths[partition_id]
            fs = get_filesystem_from_path(path)
            table = csv.read_csv(
                fs.open(path),
                parse_options=csv.ParseOptions(
                    delimiter=scan._source_info.delimiter,
                ),
                read_options=csv.ReadOptions(
                    column_names=[expr.name() for expr in schema],
                    skip_rows_after_names=1 if scan._source_info.has_headers else 0,
                ),
            )
            vpart = vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)
            return vpart
        elif scan._source_info.scan_type() == ScanType.PARQUET:
            assert isinstance(scan._source_info, ParquetSourceInfo)
            table = parquet.read_table(scan._source_info.filepaths[partition_id])
            vpart = vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)
            return vpart
        else:
            raise NotImplementedError(f"PyRunner has not implemented scan: {scan._source_info.scan_type()}")

    def _handle_projection(self, inputs: Dict[int, vPartition], proj: Projection, partition_id: int) -> vPartition:
        child_id = proj._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.eval_expression_list(proj._projection)

    def _handle_filter(self, inputs: Dict[int, vPartition], filter: Filter, partition_id: int) -> vPartition:
        predicate = filter._predicate
        child_id = filter._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.filter(predicate)

    def _handle_local_limit(self, inputs: Dict[int, vPartition], limit: LocalLimit, partition_id: int) -> vPartition:
        num = limit._num
        child_id = limit._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.head(num)

    def _handle_local_aggregate(
        self, inputs: Dict[int, vPartition], agg: LocalAggregate, partition_id: int
    ) -> vPartition:
        child_id = agg._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.agg(agg._agg, group_by=agg._group_by)

    def _handle_join(self, inputs: Dict[int, vPartition], join: Join, partition_id: int) -> vPartition:
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
