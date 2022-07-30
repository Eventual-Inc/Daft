from __future__ import annotations

from dataclasses import dataclass
from functools import partialmethod
from typing import Any, Callable, ClassVar, Dict, Generic, List, TypeVar, Union

import pyarrow as pa
from pyarrow import compute as pac
from pyarrow import csv

from daft.logical.logical_plan import LogicalPlan, Projection, Scan
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


ArrType = TypeVar("ArrType")
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
        if isinstance(data, list):
            return PyListDataBlock(data=data)
        elif isinstance(data, pa.ChunkedArray):
            return ArrowDataBlock(data=data)
        else:
            arrow_type = pa.infer_type([data])
            is_prim = pa.types.is_primitive(arrow_type)
            if not is_prim:
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


class PyListDataBlock(DataBlock[List]):
    ...


class ArrowDataBlock(DataBlock[Union[pa.ChunkedArray, pa.Scalar]]):
    operators: ClassVar[FunctionDispatch] = ArrowFunctionDispatch


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
        print(self._col_manager._nid_to_node_output[node.id()])

    def _handle_scan(self, scan: Scan) -> None:
        n_partitions = scan.num_partitions()
        id = scan.id()
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
                    node_id=id,
                    partition_id=0,
                    column_id=col_id,
                    column_name=col_name,
                    block=DataBlock.make_block(col_array),
                )

    def _handle_projection(self, proj: Projection) -> None:
        assert proj.num_partitions() == 1
        output = proj.schema()
        id = proj.id()
        child_id = proj._children()[0].id()
        for expr in output:
            col_id = expr.get_id()
            output_name = expr.name()
            if not expr.has_call():
                assert col_id is not None
                prev_node_value = self._col_manager.get(node_id=child_id, partition_id=0, column_id=col_id)
                self._col_manager.put(
                    node_id=id, partition_id=0, column_id=col_id, column_name=output_name, block=prev_node_value.block
                )
            else:
                required_cols = expr.required_columns()
                required_blocks = {}
                for c in required_cols:
                    block = self._col_manager.get(node_id=child_id, partition_id=0, column_id=c.get_id()).block
                    required_blocks[c.name()] = block
                result = expr.eval(**required_blocks)

                self._col_manager.put(
                    node_id=id, partition_id=0, column_id=col_id, column_name=output_name, block=result
                )

    def logical_node_dispatcher(self, op: LogicalPlan) -> None:
        {
            Scan: self._handle_scan,
        }
