from __future__ import annotations

import collections
import operator
import random
from abc import abstractmethod
from functools import partial
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pac
from pandas.core.reshape.merge import get_join_indexers

from daft.execution.operators import OperatorEnum, OperatorEvaluator
from daft.internal.hashing import hash_chunked_array

ArrType = TypeVar("ArrType", bound=collections.abc.Sequence)
UnaryFuncType = Callable[[ArrType], ArrType]
BinaryFuncType = Callable[[ArrType, ArrType], ArrType]

NUMPY_MINOR_VERSION = int(np.version.version.split(".")[1])


def zip_blocks_as_py(*blocks: DataBlock) -> Iterator[Tuple[Any, ...]]:
    """Utility to zip the data of blocks together, returning a row-based iterator. This utility
    accounts for special-cases such as blocks that contain a single value to be broadcasted,
    differences in the underlying data representation etc.

    Note that this is invokes data conversion to Python types and will be slow. This should be used
    only in UDFs.

    1. If all blocks contain scalars, this will return one tuple of all the scalars
    2. If all blocks are scalars or have 0 length, this returns no elements
    3. Else return the same number of elements as the shortest block
    """
    iterators = [b.iter_py() for b in blocks]

    # If all blocks are scalar blocks we can just return one row
    scalar_blocks = [b.is_scalar() for b in blocks]
    if all(scalar_blocks):
        yield tuple(next(gen) for gen in iterators)
        return

    block_lengths = tuple(len(b) for b in blocks)
    nonzero_block_lengths = tuple(b_len for b_len in block_lengths if b_len > 0)

    if not nonzero_block_lengths:
        return

    min_block_len = min(nonzero_block_lengths)
    for _ in range(min_block_len):
        yield tuple(next(gen) for gen in iterators)


class DataBlock(Generic[ArrType]):
    data: ArrType
    evaluator: ClassVar[Type[OperatorEvaluator]]

    def __init__(self, data: ArrType) -> None:
        self.data = data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n{self.data}"

    def __len__(self) -> int:
        if self.is_scalar():
            return 0
        return len(self.data)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DataBlock) and self.data == o.data

    @abstractmethod
    def is_scalar(self) -> bool:
        """Returns True if this DataBlock holds a scalar value"""
        raise NotImplementedError()

    @abstractmethod
    def iter_py(self) -> Iterator:
        """Iterate through the DataBlock and yield Python objects (e.g. ints, floats etc)
        Note that this is SLOW and should be used sparingly, mostly only in user-facing APIs
        such as UDFs.
        """
        raise NotImplementedError()

    @abstractmethod
    def to_numpy(self):
        raise NotImplementedError()

    @classmethod
    def make_block(cls, data: Any) -> DataBlock:
        # Data is a sequence of data
        if isinstance(data, pa.ChunkedArray):
            return ArrowDataBlock(data=data)
        elif isinstance(data, pa.Array):
            return ArrowDataBlock(data=pa.chunked_array([data]))
        elif isinstance(data, np.ndarray):
            if data.dtype == np.object_ or len(data.shape) > 1:
                try:
                    arrow_type = pa.infer_type(data)
                except pa.lib.ArrowInvalid:
                    arrow_type = None
                if arrow_type is None or pa.types.is_nested(arrow_type):
                    return PyListDataBlock(data=list(data))
                return ArrowDataBlock(data=pa.chunked_array([pa.array(data, type=arrow_type)]))
            arrow_type = pa.from_numpy_dtype(data.dtype)
            return ArrowDataBlock(data=pa.chunked_array([pa.array(data, type=arrow_type)]))
        elif isinstance(data, pd.Series):
            if data.dtype == np.object_:
                try:
                    arrow_type = pa.infer_type(data)
                except pa.lib.ArrowInvalid:
                    arrow_type = None
                if arrow_type is None or pa.types.is_nested(arrow_type):
                    return PyListDataBlock(data=data.to_list())
                return ArrowDataBlock(data=pa.chunked_array([pa.array(data, type=arrow_type)]))
            arrow_type = pa.Schema.from_pandas(pd.DataFrame({"0": data}))[0].type
            return ArrowDataBlock(data=pa.chunked_array([pa.Array.from_pandas(data, type=arrow_type)]))
        elif isinstance(data, list):
            try:
                arrow_type = pa.infer_type(data)
            except pa.lib.ArrowInvalid:
                arrow_type = None
            if arrow_type is None or pa.types.is_nested(arrow_type):
                return PyListDataBlock(data=data)
            return ArrowDataBlock(data=pa.chunked_array([pa.array(data, type=arrow_type)]))
        # Data is a scalar
        elif isinstance(data, pa.Scalar):
            return ArrowDataBlock(data=data)
        else:
            try:
                arrow_type = pa.infer_type([data])
            except pa.lib.ArrowInvalid:
                arrow_type = None
            if arrow_type is None or pa.types.is_nested(arrow_type):
                return PyListDataBlock(data=data)
            return ArrowDataBlock(data=pa.scalar(data, type=arrow_type))

    def _unary_op(self, fn: Callable[[ArrType], ArrType], **kwargs) -> DataBlock[ArrType]:
        return DataBlock.make_block(data=fn(self.data, **kwargs))

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

    def _ternary_op(
        self, other1: Any, other2: Any, fn: Callable[[ArrType, ArrType, ArrType], ArrType]
    ) -> DataBlock[ArrType]:
        self = DataBlock._convert_to_block(self)
        other1 = DataBlock._convert_to_block(other1)
        other2 = DataBlock._convert_to_block(other2)
        return DataBlock.make_block(data=fn(self.data, other1.data, other2.data))

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

    def partition(self, num: int, targets: DataBlock[ArrowArrType]) -> List[DataBlock[ArrType]]:
        assert not self.is_scalar(), "Cannot partition scalar DataBlock"

        # We first argsort the targets to group the same partitions together
        argsort_indices = targets.argsort()

        # We now perform a gather to make items targeting the same partition together
        reordered = self.take(argsort_indices)
        sorted_targets = targets.take(argsort_indices)

        sorted_targets_np = sorted_targets.data.to_numpy()
        pivots = np.where(np.diff(sorted_targets_np, prepend=np.nan))[0]

        # We now split in the num partitions
        unmatched_partitions = reordered._split(pivots)
        target_partitions = sorted_targets_np[pivots]

        target_partition_idx_to_match_idx = {target_idx: idx for idx, target_idx in enumerate(target_partitions)}

        return [
            DataBlock.make_block(unmatched_partitions[target_partition_idx_to_match_idx[i]])
            if i in target_partition_idx_to_match_idx
            else self._make_empty()
            for i in range(num)
        ]

    def head(self, num: int) -> DataBlock[ArrType]:
        assert not self.is_scalar(), "Cannot get head of scalar DataBlock"
        return DataBlock.make_block(self.data[:num])

    def filter(self, mask: DataBlock[ArrowArrType]) -> DataBlock[ArrType]:
        """Filters elements of the Datablock using the provided mask"""
        assert not self.is_scalar(), "Cannot filter scalar DataBlock"
        return self._filter(mask)

    def take(self, indices: DataBlock[ArrowArrType]) -> DataBlock[ArrType]:
        """Takes elements of the DataBlock in the order specified by `indices`"""
        assert not self.is_scalar(), "Cannot get take from scalar DataBlock"
        return self._take(indices)

    @abstractmethod
    def _split(self, pivots: np.ndarray) -> Sequence[ArrType]:
        """Splits the data at the given pivots. First element of the pivot should be a NaN"""
        raise NotImplementedError()

    @abstractmethod
    def _make_empty(self) -> DataBlock[ArrType]:
        """Makes an empty DataBlock of the same type as the current one"""
        raise NotImplementedError()

    @abstractmethod
    def _filter(self, mask: DataBlock[ArrowArrType]) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @abstractmethod
    def _take(self, indices: DataBlock[ArrowArrType]) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @abstractmethod
    def sample(self, num: int) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @abstractmethod
    def _argsort(self, desc: bool = False) -> DataBlock[ArrowArrType]:
        raise NotImplementedError()

    def argsort(self, desc: bool = False) -> DataBlock[ArrowArrType]:
        assert not self.is_scalar(), "Cannot sort scalar DataBlock"
        return self._argsort(desc=desc)

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
    def array_hash(self, seed: Optional[DataBlock[ArrType]] = None) -> DataBlock[ArrowArrType]:
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

    @staticmethod
    @abstractmethod
    def _join_keys(
        left_keys: List[DataBlock[ArrType]], right_keys: List[DataBlock[ArrType]]
    ) -> Tuple[DataBlock[ArrType], DataBlock[ArrType]]:
        raise NotImplementedError()

    @classmethod
    def join(
        cls,
        left_keys: List[DataBlock],
        right_keys: List[DataBlock],
        left_columns: List[DataBlock],
        right_columns: List[DataBlock],
    ) -> List[DataBlock]:
        assert len(left_keys) > 0
        assert len(left_keys) == len(right_keys)
        last_type = None
        for l, r in zip(left_keys, right_keys):
            assert type(l) == type(r), f"type mismatch in keys, {type(l)} vs {type(r)}"
            if last_type is not None:
                assert type(l) == last_type
            last_type = type(l)
        first_type = type(left_keys[0])
        left_indices, right_indices = first_type._join_keys(left_keys=left_keys, right_keys=right_keys)
        to_rtn = []
        for blockset in (left_keys, left_columns):
            for b in blockset:
                to_rtn.append(b.take(left_indices))

        for b in right_columns:
            to_rtn.append(b.take(right_indices))

        return to_rtn


T = TypeVar("T")


class PyListDataBlock(DataBlock[List[T]]):
    def is_scalar(self) -> bool:
        # TODO(jay): This is dangerous, as we can't handle cases such as lit([1, 2, 3]). We might need to
        # make this more explicit and passed in from the DataBlock.make_block() level
        return not isinstance(self.data, list)

    def iter_py(self) -> Iterator:
        if self.is_scalar():
            while True:
                yield self.data
        yield from self.data

    def to_numpy(self):
        if self.is_scalar():
            return self.data
        res = np.empty((len(self),), dtype="object")
        for i, obj in enumerate(self.data):
            res[i] = obj
        return res

    def _filter(self, mask: DataBlock[ArrowArrType]) -> DataBlock[List[T]]:
        return PyListDataBlock(data=[item for keep, item in zip(mask.iter_py(), self.data) if keep])

    def _take(self, indices: DataBlock[ArrowArrType]) -> DataBlock[List[T]]:
        return PyListDataBlock(data=[self.data[i] for i in indices.iter_py()])

    def sample(self, num: int) -> DataBlock[List[T]]:
        return PyListDataBlock(data=random.sample(self.data, num))

    def _argsort(self, desc: bool = False) -> DataBlock[ArrowArrType]:
        raise NotImplementedError("Sorting by Python objects is not implemented")

    def _make_empty(self) -> DataBlock[List[T]]:
        return PyListDataBlock(data=[])

    def _split(self, pivots: np.ndarray) -> Sequence[List[T]]:
        return [list(chunk) for chunk in np.split(self.data, pivots)[1:]]

    @staticmethod
    def _merge_blocks(blocks: List[DataBlock[List[T]]]) -> DataBlock[List[T]]:
        concatted_data = []
        for block in blocks:
            concatted_data.extend(block.data)
        return PyListDataBlock(data=concatted_data)

    def search_sorted(self, pivots: DataBlock[List[T]], reverse: bool = False) -> DataBlock[ArrowArrType]:
        raise NotImplementedError("Sorting by Python objects is not implemented")

    def quantiles(self, num: int) -> DataBlock[List[T]]:
        raise NotImplementedError("Sorting by Python objects is not implemented")

    def array_hash(self, seed: Optional[DataBlock[ArrType]] = None) -> DataBlock[ArrowArrType]:
        # TODO(jay): seed is ignored here, but perhaps we need to set it in PYTHONHASHSEED?
        hashes = [hash(x) for x in self.data]
        return DataBlock.make_block(np.array(hashes))

    def agg(self, op: str) -> DataBlock[ArrowArrType]:
        raise NotImplementedError("Aggregations on Python objects is not implemented yet")

    @staticmethod
    def _group_by_agg(
        group_by: List[DataBlock[List[T]]], to_agg: List[DataBlock[List[T]]], agg_ops: List[str]
    ) -> Tuple[List[DataBlock[List[T]]], List[DataBlock[List[T]]]]:
        raise NotImplementedError("Aggregations on Python objects is not implemented yet")

    @staticmethod
    def _join_keys(
        left_keys: List[DataBlock[List[T]]], right_keys: List[DataBlock[List[T]]]
    ) -> Tuple[DataBlock[List[T]], DataBlock[List[T]]]:
        raise NotImplementedError()


ArrowArrType = Union[pa.ChunkedArray, pa.Scalar]


class ArrowDataBlock(DataBlock[ArrowArrType]):
    def __reduce__(self) -> Tuple:
        if len(self.data) == 0:
            return ArrowDataBlock, (self._make_empty().data,)
        else:
            return ArrowDataBlock, (self.data,)

    def to_numpy(self):
        if isinstance(self.data, pa.Scalar):
            return self.data.as_py()
        return self.data.to_numpy()

    def is_scalar(self) -> bool:
        return isinstance(self.data, pa.Scalar)

    def iter_py(self) -> Iterator:
        if isinstance(self.data, pa.Scalar):
            py_scalar = self.data.as_py()
            while True:
                yield py_scalar
        yield from self.data.to_numpy()

    def _argsort(self, desc: bool = False) -> DataBlock[ArrowArrType]:
        order = "descending" if desc else "ascending"
        sort_indices = pac.array_sort_indices(self.data, order=order)
        return ArrowDataBlock(data=sort_indices)

    def _filter(self, mask: DataBlock[ArrowArrType]) -> DataBlock[ArrowArrType]:
        return self._binary_op(mask, fn=pac.array_filter)

    def _take(self, indices: DataBlock[ArrowArrType]) -> DataBlock[ArrowArrType]:
        return self._binary_op(indices, fn=partial(pac.take, boundscheck=False))

    def _split(self, pivots: np.ndarray) -> Sequence[ArrowArrType]:
        splitted: Sequence[np.ndarray] = np.split(self.data, pivots)[1:]
        return splitted

    @staticmethod
    def _merge_blocks(blocks: List[DataBlock[ArrowArrType]]) -> DataBlock[ArrowArrType]:
        all_chunks = []
        for block in blocks:
            all_chunks.extend(block.data.chunks)
        return ArrowDataBlock(data=pa.chunked_array(all_chunks))

    def _make_empty(self) -> DataBlock[ArrowArrType]:
        return ArrowDataBlock(data=pa.chunked_array([[]], type=self.data.type))

    def sample(self, num: int) -> DataBlock[ArrowArrType]:
        if num >= len(self):
            return self
        sampled = np.random.choice(self.data, num, replace=False)
        return ArrowDataBlock(data=pa.chunked_array([sampled]))

    def search_sorted(self, pivots: DataBlock, reverse: bool = False) -> DataBlock[ArrowArrType]:
        arr = self.data.to_numpy()
        indices = np.searchsorted(pivots.data.to_numpy(), arr)
        if reverse:
            indices = len(pivots) - indices
        return ArrowDataBlock(data=pa.chunked_array([indices]))

    def quantiles(self, num: int) -> DataBlock[ArrowArrType]:
        quantiles = np.linspace(1.0 / num, 1.0, num)[:-1]
        if NUMPY_MINOR_VERSION < 22:
            pivots = np.quantile(self.data.to_numpy(), quantiles, interpolation="nearest")
        else:
            pivots = np.quantile(self.data.to_numpy(), quantiles, method="closest_observation")
        return DataBlock.make_block(data=pivots)

    def array_hash(self, seed: Optional[DataBlock[ArrowArrType]] = None) -> DataBlock[ArrowArrType]:
        assert isinstance(self.data, pa.ChunkedArray)
        assert seed is None or isinstance(seed.data, pa.ChunkedArray)

        pa_type = self.data.type
        data_to_hash = self.data
        if pa.types.is_date32(pa_type):
            data_to_hash = data_to_hash.cast(pa.int32())
        elif pa.types.is_date64(pa_type):
            data_to_hash = data_to_hash.cast(pa.int64())
        elif pa.types.is_floating(pa_type):
            data_to_hash = data_to_hash.cast(pa.string())
        elif not (pa.types.is_integer(pa_type) or pa.types.is_string(pa_type)):
            raise TypeError(f"cannot hash {pa_type}")
        hashed = hash_chunked_array(data_to_hash)
        if seed is None:
            return ArrowDataBlock(data=hashed)
        else:
            seed_pa_type = seed.data.type
            if not (pa.types.is_uint64(seed_pa_type)):
                raise TypeError(f"can only seed hash uint64 not {seed_pa_type}")

            seed_arr = seed.data.to_numpy()
            seed_arr = seed_arr ^ (hashed.to_numpy() + 0x9E3779B9 + (seed_arr << 6) + (seed_arr >> 2))
            return ArrowDataBlock(data=pa.chunked_array([seed_arr]))

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
        table = pa.table(arrs, names=group_names + agg_names).combine_chunks()
        agged = table.group_by(group_names).aggregate([(a_name, op) for a_name, op in zip(agg_names, agg_ops)])
        gcols: List[DataBlock] = [ArrowDataBlock(agged[g_name]) for g_name in group_names]
        acols: List[DataBlock] = [ArrowDataBlock(agged[f"{a_name}_{op}"]) for a_name, op in zip(agg_names, agg_ops)]
        return gcols, acols

    @staticmethod
    def _join_keys(
        left_keys: List[DataBlock[ArrowArrType]], right_keys: List[DataBlock[ArrowArrType]]
    ) -> Tuple[DataBlock[ArrowArrType], DataBlock[ArrowArrType]]:
        assert len(left_keys) == len(right_keys)

        pd_left_keys = [k.data.to_pandas() for k in left_keys]

        pd_right_keys = [k.data.to_pandas() for k in right_keys]
        left_index, right_index = get_join_indexers(pd_left_keys, pd_right_keys, how="inner")

        return DataBlock.make_block(left_index), DataBlock.make_block(right_index)


def arrow_mod(arr, m):
    return np.mod(arr, m.as_py())


def arrow_floordiv(arr, m):
    return pac.floor(pac.divide(arr, m))


def arrow_str_contains(arr: pa.ChunkedArray, pattern: pa.StringScalar):
    substring_counts = pac.count_substring(arr, pattern=pattern.as_py())
    return pac.not_equal(substring_counts, pa.scalar(0))


def arrow_str_endswith(arr: pa.ChunkedArray, pattern: pa.StringScalar):
    return pac.ends_with(arr, pattern=pattern.as_py())


def arrow_str_startswith(arr: pa.ChunkedArray, pattern: pa.StringScalar):
    return pac.starts_with(arr, pattern=pattern.as_py())


def _arr_unary_op(
    fn: Callable[..., pa.ChunkedArray],
) -> Callable[[DataBlock[ArrowArrType]], DataBlock[ArrowArrType]]:
    return partial(ArrowDataBlock._unary_op, fn=fn)  # type: ignore


def _arr_bin_op(
    fn: Callable[[pa.ChunkedArray, pa.ChunkedArray], pa.ChunkedArray]
) -> Callable[[DataBlock[ArrowArrType], DataBlock[ArrowArrType]], DataBlock[ArrowArrType]]:
    return partial(ArrowDataBlock._binary_op, fn=fn)  # type: ignore


def _arr_ternary_op(
    fn: Callable[[pa.ChunkedArray, pa.ChunkedArray, pa.ChunkedArray], pa.ChunkedArray]
) -> Callable[[DataBlock[ArrowArrType], DataBlock[ArrowArrType], DataBlock[ArrowArrType]], DataBlock[ArrowArrType]]:
    return partial(ArrowDataBlock._ternary_op, fn=fn)  # type: ignore


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
    STR_CONTAINS = _arr_bin_op(arrow_str_contains)
    STR_ENDSWITH = _arr_bin_op(arrow_str_endswith)
    STR_STARTSWITH = _arr_bin_op(arrow_str_startswith)
    STR_LENGTH = _arr_unary_op(pac.utf8_length)
    DT_DAY = _arr_unary_op(pac.day)
    DT_MONTH = _arr_unary_op(pac.month)
    DT_YEAR = _arr_unary_op(pac.year)
    DT_DAY_OF_WEEK = _arr_unary_op(pac.day_of_week)
    IS_NULL = _arr_unary_op(pac.is_null)
    IS_NAN = _arr_unary_op(pac.is_nan)
    IF_ELSE = _arr_ternary_op(pac.if_else)


ArrowDataBlock.evaluator = ArrowEvaluator

IN_1 = TypeVar("IN_1")
IN_2 = TypeVar("IN_2")
OUT = TypeVar("OUT")


def make_map_unary(f: Callable[[IN_1], OUT]) -> Callable[[DataBlock[Sequence[IN_1]]], DataBlock[Sequence[OUT]]]:
    def map_f(values: DataBlock[Sequence[IN_1]]) -> DataBlock[Sequence[OUT]]:
        return DataBlock.make_block(list(map(f, (tup[0] for tup in zip_blocks_as_py(values)))))

    return map_f


def make_map_binary(
    f: Callable[[IN_1, IN_2], OUT]
) -> Callable[[DataBlock[Sequence[IN_1]], DataBlock[Sequence[IN_2]]], DataBlock[Sequence[OUT]]]:
    def map_f(values: DataBlock[Sequence[IN_1]], others: DataBlock[Sequence[IN_2]]) -> DataBlock[Sequence[OUT]]:
        return DataBlock.make_block(list(f(v, o) for v, o in zip_blocks_as_py(values, others)))

    return map_f


def assert_invalid_pylist_operation(*args, **kwargs):
    raise AssertionError(f"This is an invalid operation on a PyListDataBlock and should never be executing")


def pylist_is_none(obj: Any):
    return obj is None


def pylist_if_else(
    cond: DataBlock[Sequence[IN_1]], x: DataBlock[Sequence[IN_2]], y: DataBlock[Sequence[IN_2]]
) -> DataBlock[Sequence[IN_2]]:
    return DataBlock.make_block([xitem if c else yitem for c, xitem, yitem in zip_blocks_as_py(cond, x, y)])


class PyListEvaluator(OperatorEvaluator["PyListDataBlock"]):
    NEGATE = make_map_unary(operator.neg)
    POSITIVE = PyListDataBlock.identity
    ABS = make_map_unary(operator.abs)
    SUM = PyListDataBlock.identity
    MEAN = PyListDataBlock.identity
    MIN = PyListDataBlock.identity
    MAX = PyListDataBlock.identity
    COUNT = PyListDataBlock.identity
    INVERT = make_map_unary(operator.invert)
    ADD = make_map_binary(operator.add)
    SUB = make_map_binary(operator.sub)
    MUL = make_map_binary(operator.mul)
    FLOORDIV = make_map_binary(operator.floordiv)
    TRUEDIV = make_map_binary(operator.truediv)
    POW = make_map_binary(operator.pow)
    MOD = make_map_binary(operator.pow)
    AND = make_map_binary(operator.and_)
    OR = make_map_binary(operator.or_)
    LT = make_map_binary(operator.lt)
    LE = make_map_binary(operator.le)
    EQ = make_map_binary(operator.eq)
    NEQ = make_map_binary(operator.ne)
    GT = make_map_binary(operator.gt)
    GE = make_map_binary(operator.ge)

    IS_NULL = make_map_unary(pylist_is_none)
    IF_ELSE = pylist_if_else

    # Unary operations that should never run on a PyListDataBlock because they are represented by
    # Arrow primitives and should always be housed in an ArrowDataBlock
    STR_CONTAINS = assert_invalid_pylist_operation
    STR_ENDSWITH = assert_invalid_pylist_operation
    STR_STARTSWITH = assert_invalid_pylist_operation
    STR_LENGTH = assert_invalid_pylist_operation
    DT_DAY = assert_invalid_pylist_operation
    DT_MONTH = assert_invalid_pylist_operation
    DT_YEAR = assert_invalid_pylist_operation
    DT_DAY_OF_WEEK = assert_invalid_pylist_operation
    IS_NAN = assert_invalid_pylist_operation


PyListDataBlock.evaluator = PyListEvaluator
