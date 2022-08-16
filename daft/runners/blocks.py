from __future__ import annotations

import collections
from abc import abstractmethod
from functools import partial
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import numpy as np
import pyarrow as pa
import pyarrow.compute as pac

from daft.execution.operators import OperatorEnum, OperatorEvaluator
from daft.internal.hashing import hash_chunked_array

ArrType = TypeVar("ArrType", bound=collections.abc.Sequence)
UnaryFuncType = Callable[[ArrType], ArrType]
BinaryFuncType = Callable[[ArrType, ArrType], ArrType]


class DataBlock(Generic[ArrType]):
    data: ArrType
    evaluator: ClassVar[Type[OperatorEvaluator]]

    def __init__(self, data: ArrType) -> None:
        self.data = data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n{self.data}"

    def __len__(self) -> int:
        return len(self.data)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DataBlock) and self.data == o.data

    @abstractmethod
    def to_pylist(self) -> List:
        raise NotImplementedError()

    @classmethod
    def make_block(cls, data: Any) -> DataBlock[ArrType]:
        # if isinstance(data, list):
        #     return PyListDataBlock(data=data)
        if isinstance(data, pa.Scalar):
            return ArrowDataBlock(data=data)
        elif isinstance(data, pa.ChunkedArray):
            return ArrowDataBlock(data=data)
        elif isinstance(data, np.ndarray):
            return ArrowDataBlock(data=pa.chunked_array([data]))
        elif isinstance(data, pa.Array):
            return ArrowDataBlock(data=pa.chunked_array([data]))
        else:
            try:
                arrow_type = pa.infer_type([data])
            except pa.lib.ArrowInvalid:
                arrow_type = None
            if arrow_type is None or pa.types.is_nested(arrow_type):
                raise ValueError(f"Don't know what block {data} should be")
            return ArrowDataBlock(data=pa.scalar(data))

    def _unary_op(self, fn: Callable[[ArrType], ArrType]) -> DataBlock[ArrType]:
        return DataBlock.make_block(data=fn(self.data))

    @classmethod
    def _convert_to_block(cls, input: Any) -> DataBlock[ArrType]:
        if isinstance(input, DataBlock):
            return input
        else:
            return DataBlock.make_block(input)

    def _binary_op(self, other: Any, fn: Callable[[ArrType, ArrType], ArrType]) -> DataBlock[ArrType]:
        self = DataBlock._convert_to_block(self)
        other = DataBlock._convert_to_block(other)
        return DataBlock.make_block(data=fn(self.data, other.data))

    def _reverse_binary_op(self, other: Any, fn) -> DataBlock[ArrType]:
        other_block: DataBlock[ArrType] = DataBlock._convert_to_block(other)
        return other_block._binary_op(self, fn=fn)

    def run_unary_operator(self, op: OperatorEnum) -> DataBlock[ArrType]:
        op_name = op.name
        fn: Callable[[DataBlock[ArrType]], DataBlock[ArrType]] = getattr(self.evaluator, op_name)
        return fn(self)

    def run_binary_operator(self, other: Any, op: OperatorEnum) -> DataBlock[ArrType]:
        op_name = op.name
        fn: Callable[[DataBlock[ArrType], DataBlock[ArrType]], DataBlock[ArrType]] = getattr(self.evaluator, op_name)
        return fn(self, other)

    def filter(self, mask: DataBlock) -> DataBlock:
        return self._binary_op(mask, fn=pac.array_filter)

    def take(self, indices: DataBlock) -> DataBlock:
        return self._binary_op(indices, fn=pac.take)

    def head(self, num: int) -> DataBlock[ArrType]:
        return DataBlock.make_block(self.data[:num])

    @abstractmethod
    def sample(self, num: int) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @abstractmethod
    def _argsort(self, desc: bool = False) -> DataBlock:
        raise NotImplementedError()

    def argsort(self, desc: bool = False) -> DataBlock:
        return self._argsort(desc=desc)

    @abstractmethod
    def partition(self, num: int, targets: DataBlock[ArrowArrType]) -> List[DataBlock[ArrType]]:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _merge_blocks(blocks: List[DataBlock[ArrType]]) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @classmethod
    def merge_blocks(cls, blocks: List[DataBlock[ArrType]]) -> DataBlock[ArrType]:
        assert len(blocks) > 0, "no blocks"
        first_type = type(blocks[0])
        assert all(type(b) == first_type for b in blocks), "all block types must match"
        return first_type._merge_blocks(blocks)

    @abstractmethod
    def search_sorted(self, pivots: DataBlock[ArrType], reverse: bool = False) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @abstractmethod
    def quantiles(self, num: int) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @abstractmethod
    def array_hash(self, seed: Optional[DataBlock[ArrType]] = None) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @abstractmethod
    def agg(self, op: str) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _group_by_agg(
        group_by: List[DataBlock[ArrType]], to_agg: List[DataBlock[ArrType]], agg_ops: List[str]
    ) -> Tuple[List[DataBlock[ArrType]], List[DataBlock[ArrType]]]:
        raise NotImplementedError()

    @classmethod
    def group_by_agg(
        cls, group_by: List[DataBlock[ArrType]], to_agg: List[DataBlock[ArrType]], agg_ops: List[str]
    ) -> Tuple[List[DataBlock[ArrType]], List[DataBlock[ArrType]]]:
        assert len(group_by) > 0, "no blocks"
        assert len(to_agg) > 0, "no blocks"
        assert len(to_agg) == len(agg_ops)
        first_type = type(to_agg[0])
        assert all(type(b) == first_type for b in group_by), "all block types must match"
        assert all(type(b) == first_type for b in to_agg), "all block types must match"

        return first_type._group_by_agg(group_by, to_agg, agg_ops)

    def identity(self) -> DataBlock[ArrType]:
        return self


class PyListDataBlock(DataBlock[List]):
    ...


ArrowArrType = Union[pa.ChunkedArray, pa.Scalar]


class ArrowDataBlock(DataBlock[ArrowArrType]):
    def to_pylist(self) -> List:
        pylist: List = self.data.to_pylist()
        return pylist

    def _argsort(self, desc: bool = False) -> DataBlock[ArrowArrType]:
        order = "descending" if desc else "ascending"
        sort_indices = pac.array_sort_indices(self.data, order=order)
        return ArrowDataBlock(data=sort_indices)

    @staticmethod
    def _merge_blocks(blocks: List[DataBlock[ArrowArrType]]) -> DataBlock[ArrowArrType]:
        all_chunks = []
        for block in blocks:
            all_chunks.extend(block.data.chunks)
        return ArrowDataBlock(data=pa.chunked_array(all_chunks))

    def partition(self, num: int, targets: DataBlock[ArrowArrType]) -> List[DataBlock[ArrowArrType]]:
        new_partitions: List[DataBlock] = [
            ArrowDataBlock(data=pa.chunked_array([[]], type=self.data.type)) for _ in range(num)
        ]
        # We first argsort the targets to group the same partitions together
        argsort_indices = targets.argsort()
        # We now perform a gather to make items targeting the same partition together
        reordered = self.take(argsort_indices)
        sorted_targets = targets.take(argsort_indices)

        pivots = np.where(np.diff(sorted_targets.data, prepend=np.nan))[0]

        # We now split in the num partitions
        unmatched_partitions = np.split(reordered.data, pivots)[1:]
        target_partitions = sorted_targets.data.to_numpy()[pivots]
        for i, target_idx in enumerate(target_partitions):
            new_partitions[target_idx] = ArrowDataBlock(data=pa.chunked_array([unmatched_partitions[i]]))
        return new_partitions

    def sample(self, num: int, replace=False) -> DataBlock[ArrowArrType]:
        sampled = np.random.choice(self.data, num, replace=replace)
        return ArrowDataBlock(data=pa.chunked_array([sampled]))

    def search_sorted(self, pivots: DataBlock, reverse: bool = False) -> DataBlock[ArrowArrType]:
        arr = self.data.to_numpy()
        indices = np.searchsorted(pivots.data.to_numpy(), arr)
        if reverse:
            indices = len(pivots) - indices
        return ArrowDataBlock(data=pa.chunked_array([indices]))

    def quantiles(self, num: int) -> DataBlock[ArrowArrType]:
        quantiles = np.linspace(1.0 / num, 1.0, num)[:-1]
        pivots = np.quantile(self.data.to_numpy(), quantiles, method="closest_observation")
        return DataBlock.make_block(data=pivots)

    def array_hash(self, seed: Optional[DataBlock[ArrowArrType]] = None) -> DataBlock[ArrowArrType]:
        assert isinstance(self.data, pa.ChunkedArray)
        assert seed is None or isinstance(seed.data, pa.ChunkedArray)

        pa_type = self.data.type
        if not (pa.types.is_integer(pa_type) or pa.types.is_string(pa_type)):
            raise TypeError(f"can only hash ints or strings not {pa_type}")
        hashed = hash_chunked_array(self.data)
        if seed is None:
            return ArrowDataBlock(data=hashed)
        else:
            seed_pa_type = seed.data.type
            if not (pa.types.is_uint64(seed_pa_type)):
                raise TypeError(f"can only seed hash uint64 not {seed_pa_type}")

            seed_arr = seed.data.to_numpy()
            seed_arr = seed_arr ^ (hashed.to_numpy() + 0x9E3779B9 + (seed_arr << 6) + (seed_arr >> 2))
            return ArrowDataBlock(data=seed_arr)

    def agg(self, op: str) -> DataBlock[ArrowArrType]:

        if op == "sum":
            if len(self) == 0:
                return ArrowDataBlock(data=pa.chunked_array([[]], type=self.data.type))
            return ArrowDataBlock(data=pa.chunked_array([[pac.sum(self.data).as_py()]]))
        elif op == "mean":
            if len(self) == 0:
                return ArrowDataBlock(data=pa.chunked_array([[]], type=pa.float64()))
            return ArrowDataBlock(data=pa.chunked_array([[pac.mean(self.data).as_py()]]))
        elif op == "count":
            if len(self) == 0:
                return ArrowDataBlock(data=pa.chunked_array([[]], type=pa.int64()))
            return ArrowDataBlock(data=pa.chunked_array([[pac.count(self.data).as_py()]]))

        else:
            raise NotImplementedError(op)

    @staticmethod
    def _group_by_agg(
        group_by: List[DataBlock[ArrowArrType]], to_agg: List[DataBlock[ArrowArrType]], agg_ops: List[str]
    ) -> Tuple[List[DataBlock[ArrowArrType]], List[DataBlock[ArrowArrType]]]:
        arrs = [a.data for a in group_by]
        arrs.extend([a.data for a in to_agg])
        group_names = [f"g_{i}" for i in range(len(group_by))]
        agg_names = [f"a_{i}" for i in range(len(to_agg))]
        table = pa.table(arrs, names=group_names + agg_names)
        agged = table.group_by(group_names).aggregate([(a_name, op) for a_name, op in zip(agg_names, agg_ops)])
        gcols: List[DataBlock] = [ArrowDataBlock(agged[g_name]) for g_name in group_names]
        acols: List[DataBlock] = [ArrowDataBlock(agged[f"{a_name}_{op}"]) for a_name, op in zip(agg_names, agg_ops)]
        return gcols, acols


def arrow_mod(arr, m):
    return np.mod(arr, m.as_py())


def arrow_floordiv(arr, m):
    return pac.floor(pac.divide(arr, m))


def _arr_unary_op(
    fn: Callable[[pa.ChunkedArray], pa.ChunkedArray]
) -> Callable[[DataBlock[ArrowArrType]], DataBlock[ArrowArrType]]:
    return partial(ArrowDataBlock._unary_op, fn=fn)  # type: ignore


def _arr_bin_op(
    fn: Callable[[pa.ChunkedArray, pa.ChunkedArray], pa.ChunkedArray]
) -> Callable[[DataBlock[ArrowArrType], DataBlock[ArrowArrType]], DataBlock[ArrowArrType]]:
    return partial(ArrowDataBlock._binary_op, fn=fn)  # type: ignore


class ArrowEvaluator(OperatorEvaluator["ArrowDataBlock"]):
    NEGATE = _arr_unary_op(pac.negate)
    POSITIVE = ArrowDataBlock.identity
    ABS = _arr_unary_op(pac.abs)
    SUM = ArrowDataBlock.identity
    MEAN = ArrowDataBlock.identity
    MIN = ArrowDataBlock.identity
    MAX = ArrowDataBlock.identity
    COUNT = ArrowDataBlock.identity
    INVERT = _arr_unary_op(pac.invert)
    ADD = _arr_bin_op(pac.add)
    SUB = _arr_bin_op(pac.subtract)
    MUL = _arr_bin_op(pac.multiply)
    FLOORDIV = _arr_bin_op(arrow_floordiv)
    TRUEDIV = _arr_bin_op(pac.divide)
    POW = _arr_bin_op(pac.power)
    MOD = _arr_bin_op(arrow_mod)
    AND = _arr_bin_op(pac.and_)
    OR = _arr_bin_op(pac.or_)
    LT = _arr_bin_op(pac.less)
    LE = _arr_bin_op(pac.less_equal)
    EQ = _arr_bin_op(pac.equal)
    NEQ = _arr_bin_op(pac.not_equal)
    GT = _arr_bin_op(pac.greater)
    GE = _arr_bin_op(pac.greater_equal)


ArrowDataBlock.evaluator = ArrowEvaluator
