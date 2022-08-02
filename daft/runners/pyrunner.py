from __future__ import annotations

import collections
from abc import abstractmethod
from dataclasses import dataclass
from functools import partialmethod
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

import pyarrow as pa
from pyarrow import compute as pac
from pyarrow import csv

from daft.logical.logical_plan import (
    Filter,
    GlobalLimit,
    LocalLimit,
    LogicalPlan,
    Projection,
    Scan,
    Sort,
)
from daft.runners.runner import Runner


class PyRunnerColumnManager:
    def __init__(self) -> None:
        self._nid_to_node_output: Dict[int, NodeOutput] = {}

    def put(self, node_id: int, partition_id: int, column_id: int, column_name: str, block: DataBlock) -> None:
        if node_id not in self._nid_to_node_output:
            self._nid_to_node_output[node_id] = NodeOutput({})

        node_output = self._nid_to_node_output[node_id]
        if column_id not in node_output.col_id_to_sharded_column:
            node_output.col_id_to_sharded_column[column_id] = PyListShardedColumn({}, column_name=column_name)

        sharded_column = node_output.col_id_to_sharded_column[column_id]

        assert partition_id not in sharded_column.part_idx_to_tile

        sharded_column.part_idx_to_tile[partition_id] = PyListTile(
            node_id=node_id, column_id=column_id, column_name=column_name, partition_id=partition_id, block=block
        )

    def get(self, node_id: int, partition_id: int, column_id: int) -> PyListTile:
        assert node_id in self._nid_to_node_output
        node_output = self._nid_to_node_output[node_id]

        assert (
            column_id in node_output.col_id_to_sharded_column
        ), f"{column_id} not found in {node_output.col_id_to_sharded_column.keys()}"

        sharded_column = node_output.col_id_to_sharded_column[column_id]

        assert partition_id in sharded_column.part_idx_to_tile

        return sharded_column.part_idx_to_tile[partition_id]

    def rm(self, id: int):
        ...


ArrType = TypeVar("ArrType", bound=collections.abc.Sequence)
UnaryFuncType = Callable[[ArrType], ArrType]
BinaryFuncType = Callable[[ArrType, ArrType], ArrType]


@dataclass
class FunctionDispatch:
    # UnaryOps
    # Arithmetic

    neg: UnaryFuncType
    pos: UnaryFuncType
    abs: UnaryFuncType

    # Logical
    invert: UnaryFuncType

    # BinaryOps
    # Arithmetic
    add: BinaryFuncType
    sub: BinaryFuncType
    mul: BinaryFuncType
    truediv: BinaryFuncType
    pow: BinaryFuncType

    # Logical
    and_: BinaryFuncType
    or_: BinaryFuncType
    lt: BinaryFuncType
    le: BinaryFuncType
    eq: BinaryFuncType
    ne: BinaryFuncType
    gt: BinaryFuncType
    ge: BinaryFuncType

    # Dataframe ops
    filter: BinaryFuncType
    take: BinaryFuncType


ArrowFunctionDispatch = FunctionDispatch(
    neg=pac.negate,
    pos=lambda x: x,
    abs=pac.abs,
    invert=pac.invert,
    add=pac.add,
    sub=pac.subtract,
    mul=pac.multiply,
    truediv=pac.divide,
    pow=pac.power,
    and_=pac.and_,
    or_=pac.or_,
    lt=pac.less,
    le=pac.less_equal,
    eq=pac.equal,
    ne=pac.not_equal,
    gt=pac.greater,
    ge=pac.greater_equal,
    filter=pac.array_filter,
    take=pac.take,
)


class DataBlock(Generic[ArrType]):
    data: ArrType
    operators: ClassVar[FunctionDispatch]

    def __init__(self, data: ArrType) -> None:
        self.data = data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n{self.data}"

    @classmethod
    def make_block(cls, data: Any) -> DataBlock:
        # if isinstance(data, list):
        #     return PyListDataBlock(data=data)
        if isinstance(data, pa.ChunkedArray):
            return ArrowDataBlock(data=data)
        else:
            try:
                arrow_type = pa.infer_type([data])
            except pa.lib.ArrowInvalid:
                arrow_type = None
            if arrow_type is None or pa.types.is_nested(arrow_type):
                raise ValueError("Don't know what block {data} should be")
            return ArrowDataBlock(data=pa.scalar(data))

    def _unary_op(self, func) -> DataBlock[ArrType]:
        fn = getattr(self.__class__.operators, func)
        return DataBlock.make_block(data=fn(self.data))

    def _convert_to_block(self, input: Any) -> DataBlock[ArrType]:
        if isinstance(input, DataBlock):
            return input
        else:
            return DataBlock.make_block(input)

    def _binary_op(self, other: Any, func) -> DataBlock[ArrType]:
        other = self._convert_to_block(other)
        fn = getattr(self.__class__.operators, func)
        return DataBlock.make_block(data=fn(self.data, other.data))

    def _reverse_binary_op(self, other: Any, func) -> DataBlock[ArrType]:
        other_block: DataBlock[ArrType] = self._convert_to_block(other)
        return other_block._binary_op(self, func=func)

    # UnaryOps

    # Arithmetic
    __neg__ = partialmethod(_unary_op, func="neg")
    __pos__ = partialmethod(_unary_op, func="pos")
    __abs__ = partialmethod(_unary_op, func="abs")

    # # Logical
    __invert__ = partialmethod(_unary_op, func="invert")

    # # BinaryOps

    # # Arithmetic
    __add__ = partialmethod(_binary_op, func="add")
    __sub__ = partialmethod(_binary_op, func="sub")
    __mul__ = partialmethod(_binary_op, func="mul")
    __truediv__ = partialmethod(_binary_op, func="truediv")
    __pow__ = partialmethod(_binary_op, func="pow")

    # # Reverse Arithmetic
    __radd__ = partialmethod(_reverse_binary_op, func="add")
    __rsub__ = partialmethod(_reverse_binary_op, func="sub")
    __rmul__ = partialmethod(_reverse_binary_op, func="mul")
    __rtruediv__ = partialmethod(_reverse_binary_op, func="truediv")
    __rpow__ = partialmethod(_reverse_binary_op, func="pow")

    # # Logical
    __and__ = partialmethod(_binary_op, func="and_")
    __or__ = partialmethod(_binary_op, func="or_")

    __lt__ = partialmethod(_binary_op, func="lt")
    __le__ = partialmethod(_binary_op, func="le")
    __eq__ = partialmethod(_binary_op, func="eq")  # type: ignore
    __ne__ = partialmethod(_binary_op, func="ne")  # type: ignore
    __gt__ = partialmethod(_binary_op, func="gt")
    __ge__ = partialmethod(_binary_op, func="ge")

    # # Reverse Logical
    __rand__ = partialmethod(_reverse_binary_op, func="and_")
    __ror__ = partialmethod(_reverse_binary_op, func="or_")

    # DataFrame ops
    filter = partialmethod(_binary_op, func="filter")
    take = partialmethod(_binary_op, func="take")

    def head(self, num: int) -> DataBlock[ArrType]:
        return DataBlock.make_block(self.data[:num])

    @abstractmethod
    def _argsort(self, others: Optional[List[DataBlock[ArrType]]] = None, desc=False) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @classmethod
    def argsort(cls, blocks: List[DataBlock[ArrType]], desc=False) -> DataBlock[ArrType]:
        assert len(blocks) > 0, "no blocks to sort"
        first_type = type(blocks[0])
        assert all(type(b) == first_type for b in blocks), "all block types must match"
        return blocks[0]._argsort(blocks[1:], desc=desc)


class PyListDataBlock(DataBlock[List]):
    ...


class ArrowDataBlock(DataBlock[Union[pa.ChunkedArray, pa.Scalar]]):
    operators: ClassVar[FunctionDispatch] = ArrowFunctionDispatch

    def _argsort(self, others: Optional[List[DataBlock]] = None, desc=False) -> DataBlock:
        to_convert = {"0": self.data}
        order = "descending" if desc else "ascending"
        cols = [("0", order)]
        if others is not None:
            to_convert.update({str(i + 1): o.data for i, o in enumerate(others)})
            cols.extend((str(i + 1), order) for i, _ in enumerate(others))
        table = pa.table(to_convert)
        sort_indices = pac.sort_indices(table, sort_keys=cols)
        return ArrowDataBlock(data=sort_indices)


@dataclass(frozen=True)
class PyListTile:
    node_id: int
    column_id: int
    column_name: str
    partition_id: int
    block: DataBlock


@dataclass
class PyListShardedColumn:
    part_idx_to_tile: Dict[int, PyListTile]
    column_name: str


@dataclass
class NodeOutput:
    col_id_to_sharded_column: Dict[int, PyListShardedColumn]


class PyRunner(Runner):
    def __init__(self, plan: LogicalPlan) -> None:
        self._plan = plan
        self._run_order = self._plan.post_order()
        self._col_manager = PyRunnerColumnManager()

    def run(self) -> None:
        for node in self._run_order:
            print(node)
            if isinstance(node, Scan):
                self._handle_scan(node)
            elif isinstance(node, Projection):
                self._handle_projection(node)
            elif isinstance(node, Filter):
                self._handle_filter(node)
            elif isinstance(node, LocalLimit):
                self._handle_local_limit(node)
            elif isinstance(node, GlobalLimit):
                # TODO update this to global partition
                self._handle_local_limit(node)
            elif isinstance(node, Sort):
                self._handle_sort(node)
            else:
                raise NotImplementedError(f"{node} not implemented")
        print(self._col_manager._nid_to_node_output[node.id()])

    def _handle_scan(self, scan: Scan) -> None:
        n_partitions = scan.num_partitions()
        node_id = scan.id()
        if scan._source_info.scan_type == Scan.ScanType.in_memory:
            assert n_partitions == 1
            raise NotImplementedError()
        elif scan._source_info.scan_type == Scan.ScanType.csv:
            assert isinstance(scan._source_info.source, str)
            schema = scan.schema()
            table = csv.read_csv(scan._source_info.source)
            for expr in schema:
                col_id = expr.get_id()
                col_name = expr = expr.name()
                assert col_name in table.column_names
                col_array = table[col_name]
                self._col_manager.put(
                    node_id=node_id,
                    partition_id=0,
                    column_id=col_id,
                    column_name=col_name,
                    block=DataBlock.make_block(col_array),
                )

    def _handle_projection(self, proj: Projection) -> None:
        assert proj.num_partitions() == 1
        output = proj.schema()
        node_id = proj.id()
        child_id = proj._children()[0].id()
        for expr in output:
            col_id = expr.get_id()
            output_name = expr.name()
            if not expr.has_call():
                assert col_id is not None
                prev_node_value = self._col_manager.get(node_id=child_id, partition_id=0, column_id=col_id)
                self._col_manager.put(
                    node_id=node_id,
                    partition_id=0,
                    column_id=col_id,
                    column_name=output_name,
                    block=prev_node_value.block,
                )
            else:
                required_cols = expr.required_columns()
                required_blocks = {}
                for c in required_cols:
                    block = self._col_manager.get(node_id=child_id, partition_id=0, column_id=c.get_id()).block
                    required_blocks[c.name()] = block
                result = expr.eval(**required_blocks)

                self._col_manager.put(
                    node_id=node_id, partition_id=0, column_id=col_id, column_name=output_name, block=result
                )

    def _handle_filter(self, filter: Filter) -> None:
        assert filter.num_partitions() == 1
        predicate = filter._predicate
        mask_so_far = None
        node_id = filter.id()
        child_id = filter._children()[0].id()
        for expr in predicate:
            required_cols = expr.required_columns()
            required_blocks = {}
            for c in required_cols:
                block = self._col_manager.get(node_id=child_id, partition_id=0, column_id=c.get_id()).block
                required_blocks[c.name()] = block
            mask = expr.eval(**required_blocks)

            if mask_so_far is None:
                mask_so_far = mask
            else:
                mask_so_far = mask_so_far & mask
        for expr in filter.schema():
            assert not expr.has_call()
            col_id = expr.get_id()
            output_name = expr.name()
            unfiltered_column = self._col_manager.get(node_id=child_id, partition_id=0, column_id=col_id).block
            filtered_column = unfiltered_column.filter(mask_so_far)
            self._col_manager.put(
                node_id=node_id, partition_id=0, column_id=col_id, column_name=output_name, block=filtered_column
            )

    def _handle_local_limit(self, limit: Union[LocalLimit, GlobalLimit]) -> None:
        assert limit.num_partitions() == 1
        num = limit._num
        child_id = limit._children()[0].id()
        node_id = limit.id()
        for expr in limit.schema():
            col_id = expr.get_id()
            output_name = expr.name()
            unlimited_column = self._col_manager.get(node_id=child_id, partition_id=0, column_id=col_id).block
            limited_column = unlimited_column.head(num)
            self._col_manager.put(
                node_id=node_id, partition_id=0, column_id=col_id, column_name=output_name, block=limited_column
            )

    def _handle_sort(self, sort: Sort) -> None:
        assert sort.num_partitions() == 1
        desc = sort._desc
        child_id = sort._children()[0].id()
        values_to_sort = []
        node_id = sort.id()

        for expr in sort._sort_by:
            col_id = expr.get_id()
            required_cols = expr.required_columns()
            required_blocks = {}
            for c in required_cols:
                block = self._col_manager.get(node_id=child_id, partition_id=0, column_id=c.get_id()).block
                required_blocks[c.name()] = block
            values_to_sort.append(expr.eval(**required_blocks))
        sort_indices = DataBlock.argsort(values_to_sort, desc=desc)

        for expr in sort.schema():
            assert not expr.has_call()
            col_id = expr.get_id()
            output_name = expr.name()
            unsorted_column = self._col_manager.get(node_id=child_id, partition_id=0, column_id=col_id).block
            sorted_column = unsorted_column.take(sort_indices)
            self._col_manager.put(
                node_id=node_id, partition_id=0, column_id=col_id, column_name=output_name, block=sorted_column
            )
