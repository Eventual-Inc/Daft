from __future__ import annotations

import collections
import math
import operator
import random
from abc import abstractmethod
from functools import partial
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pac
from pandas.core.reshape.merge import get_join_indexers

from daft.execution.operators import OperatorEnum, OperatorEvaluator
from daft.internal.hashing import hash_chunked_array
from daft.internal.kernels.search_sorted import search_sorted

# A type representing some Python scalar (non-series/array like object)
PyScalar = Any

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
    def to_numpy(self) -> np.ndarray:
        raise NotImplementedError()

    @abstractmethod
    def to_polars(self) -> Union[PyScalar, pl.Series]:
        raise NotImplementedError()

    @abstractmethod
    def to_arrow(self) -> pa.ChunkedArray:
        raise NotImplementedError()

    @classmethod
    def make_block(cls, data: Any) -> DataBlock:
        # Data is a sequence of data
        # Arrow data: nested types get casted to a list and stored as PyListDataBlock
        if isinstance(data, pa.ChunkedArray):
            if pa.types.is_nested(data.type):
                return PyListDataBlock(data=data.to_pylist())
            return ArrowDataBlock(data=data)
        elif isinstance(data, pa.Array):
            if pa.types.is_nested(data.type):
                return PyListDataBlock(data=data.to_pylist())
            return ArrowDataBlock(data=pa.chunked_array([data]))
        # Lists: always stored as a PyListDataBlock
        elif isinstance(data, list):
            return PyListDataBlock(data=data)
        # Numpy data: attempt to cast to Arrow, nested types get casted to a list and stored as PyListDataBlock
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
        # Data is a scalar
        elif isinstance(data, pa.Scalar):
            if pa.types.is_nested(data.type):
                return PyListDataBlock(data=data.as_py())
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

    def partition(
        self, num: int, pivots: np.ndarray, target_partitions: np.ndarray, argsorted_targets: DataBlock[ArrowArrType]
    ) -> List[DataBlock[ArrType]]:
        assert not self.is_scalar(), "Cannot partition scalar DataBlock"

        # We first argsort the targets to group the same partitions together
        argsort_indices = argsorted_targets

        # We now perform a gather to make items targeting the same partition together
        reordered = self.take(argsort_indices)

        # We now split in the num partitions
        unmatched_partitions = reordered._split(pivots)
        target_partition_idx_to_match_idx = {target_idx: idx for idx, target_idx in enumerate(target_partitions)}
        empty = self._make_empty()

        return [
            DataBlock.make_block(unmatched_partitions[target_partition_idx_to_match_idx[i]])
            if i in target_partition_idx_to_match_idx
            else empty
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

    @staticmethod
    @abstractmethod
    def _argsort(blocks: List[DataBlock[ArrType]], descending: Optional[List[bool]] = None) -> DataBlock[ArrowArrType]:
        raise NotImplementedError()

    @classmethod
    def argsort(cls, blocks: List[DataBlock], descending: Optional[List[bool]] = None) -> DataBlock[ArrowArrType]:
        first_type = type(blocks[0])
        assert all(type(b) == first_type for b in blocks), "all block types must match"
        size = len(blocks)
        if descending is None:
            descending = [False for _ in range(size)]
        else:
            assert size == len(descending), f"mismatch of size of descending and blocks {size} vs {len(descending)}"
        return first_type._argsort(blocks, descending=descending)

    @staticmethod
    @abstractmethod
    def _search_sorted(
        sorted_blocks: List[DataBlock], keys: List[DataBlock], input_reversed: Optional[List[bool]] = None
    ) -> DataBlock[ArrowArrType]:
        raise NotImplementedError()

    @classmethod
    def search_sorted(
        cls, sorted_blocks: List[DataBlock], keys: List[DataBlock], input_reversed: Optional[List[bool]] = None
    ) -> DataBlock[ArrowArrType]:
        first_type = type(sorted_blocks[0])
        assert all(type(b) == first_type for b in sorted_blocks), "all block types must match"
        assert all(type(b) == first_type for b in keys), "all block types must match"

        size = len(sorted_blocks)
        if input_reversed is None:
            input_reversed = [False for _ in range(size)]
        assert len(sorted_blocks) == len(keys)
        assert size == len(input_reversed), f"mismatch of size of descending and blocks {size} vs {len(input_reversed)}"

        return first_type._search_sorted(sorted_blocks=sorted_blocks, keys=keys, input_reversed=input_reversed)

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

    @abstractmethod
    def list_explode(self) -> Tuple[DataBlock[ArrType], DataBlock[ArrowArrType]]:
        """Treats each row as a list and flattens the nested lists. Will throw an error if
        elements in the block cannot be treated as a list.

        Note: empty lists are flattened into a single `Null` element!

        Returns a tuple `(exploded_block, list_lengths)` where `exploded_block` is the new block with the
        topmost level of lists flattened, and `list_lengths` is the length of each list element before flattening.
        """
        raise NotImplementedError()


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

    def to_polars(self) -> Union[PyScalar, pl.Series]:
        if self.is_scalar():
            return self.data
        return pl.Series(self.data, dtype=pl.datatypes.Object)

    def to_arrow(self) -> pa.ChunkedArray:
        raise NotImplementedError("can not convert pylist block to arrow")

    def _filter(self, mask: DataBlock[ArrowArrType]) -> DataBlock[List[T]]:
        return PyListDataBlock(data=[item for keep, item in zip(mask.iter_py(), self.data) if keep])

    def _take(self, indices: DataBlock[ArrowArrType]) -> DataBlock[List[T]]:
        return PyListDataBlock(data=[self.data[i] for i in indices.iter_py()])

    def sample(self, num: int) -> DataBlock[List[T]]:
        return PyListDataBlock(data=random.sample(self.data, num))

    @staticmethod
    def _search_sorted(
        sorted_blocks: List[DataBlock], keys: List[DataBlock], input_reversed: Optional[List[bool]] = None
    ) -> DataBlock[ArrowArrType]:
        raise NotImplementedError()

    @staticmethod
    def _argsort(blocks: List[DataBlock[ArrType]], descending: Optional[List[bool]] = None) -> DataBlock[ArrowArrType]:
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

    def list_explode(self) -> Tuple[PyListDataBlock, DataBlock[ArrowArrType]]:
        lengths = []
        exploded = []
        for item in self.data:
            length = 0
            if item is not None:
                for nested_element in cast(Iterable, item):
                    length += 1
                    exploded.append(nested_element)
            # empty lists or Null elements explode into a `Null`
            if length == 0:
                exploded.append(None)
                lengths.append(1)
            else:
                lengths.append(length)
        return (
            PyListDataBlock(exploded),
            DataBlock.make_block(pa.array(lengths)),
        )


ArrowArrType = Union[pa.ChunkedArray, pa.Scalar]


class ArrowDataBlock(DataBlock[ArrowArrType]):
    def __init__(self, data: ArrowArrType) -> None:
        assert not pa.types.is_nested(
            data.type
        ), f"Cannot create ArrowDataBlock with nested type: {data.type}, please report this to Daft developers."
        super().__init__(data)

    def __reduce__(self) -> Tuple:
        if len(self.data) == 0:
            return ArrowDataBlock, (self._make_empty().data,)
        else:
            return ArrowDataBlock, (self.data,)

    def to_numpy(self):
        if isinstance(self.data, pa.Scalar):
            return self.data.as_py()
        return self.data.to_numpy()

    def to_polars(self) -> Union[PyScalar, pl.Series]:
        if isinstance(self.data, pa.Scalar):
            return self.data.as_py()
        return pl.Series(self.data)

    def to_arrow(self) -> pa.ChunkedArray:
        return self.data

    def is_scalar(self) -> bool:
        return isinstance(self.data, pa.Scalar)

    def iter_py(self) -> Iterator:
        if isinstance(self.data, pa.Scalar):
            py_scalar = self.data.as_py()
            while True:
                yield py_scalar
        yield from self.data.to_pylist()

    # def _argsort(self, desc: bool = False) -> DataBlock[ArrowArrType]:
    #     order = "descending" if desc else "ascending"
    #     sort_indices = pac.array_sort_indices(self.data, order=order)
    #     return ArrowDataBlock(data=sort_indices)

    @staticmethod
    def _argsort(blocks: List[DataBlock[ArrType]], descending: Optional[List[bool]] = None) -> DataBlock[ArrowArrType]:
        arrs = [a.data for a in blocks]
        arr_names = [f"a_{i}" for i in range(len(blocks))]
        if descending is None:
            descending = [False for _ in range(len(blocks))]
        order = ["descending" if desc else "ascending" for desc in descending]
        table = pa.table(arrs, names=arr_names)
        indices = pac.sort_indices(table, sort_keys=list(zip(arr_names, order)))
        return DataBlock.make_block(indices)

    @staticmethod
    def _search_sorted(
        sorted_blocks: List[DataBlock], keys: List[DataBlock], input_reversed: Optional[List[bool]] = None
    ) -> DataBlock[ArrowArrType]:

        arr_names = [f"a_{i}" for i in range(len(sorted_blocks))]
        if input_reversed is None:
            input_reversed = [False for _ in range(len(sorted_blocks))]
        sorted_table = pa.table([a.data for a in sorted_blocks], names=arr_names)
        key_table = pa.table([a.data for a in keys], names=arr_names)
        assert sorted_table.schema == key_table.schema
        result = search_sorted(sorted_table, key_table, input_reversed=input_reversed)
        return DataBlock.make_block(result)

    def _filter(self, mask: DataBlock[ArrowArrType]) -> DataBlock[ArrowArrType]:
        return self._binary_op(mask, fn=pac.array_filter)

    def _take(self, indices: DataBlock[ArrowArrType]) -> DataBlock[ArrowArrType]:

        if pa.types.is_binary(self.data.type) or pa.types.is_string(self.data.type):
            bytes_per_row = self.data.nbytes / max(len(self.data), 1)
            expected_size = bytes_per_row * len(indices) * 2  # with safety factor
            if expected_size >= (2**31):
                # if we have a binary or string array thats result will be is larger than 2GB
                # then we need to chunk the indice array so the result array will also be chunk
                # this is a hack since arrow's kernels don't correctly chunk with the arraybuilder
                # for take.
                num_chunks = math.ceil(expected_size / (2**31))
                chunk_size = len(indices) // num_chunks
                new_indices_subarray = []
                for i in range(num_chunks):
                    new_indices_subarray.extend(indices.data.slice(i * chunk_size, chunk_size).chunks)
                indices = DataBlock.make_block(pa.chunked_array(new_indices_subarray))

        return self._binary_op(indices, fn=partial(pac.take, boundscheck=False))

    def _split(self, pivots: np.ndarray) -> Sequence[ArrowArrType]:
        to_return = []
        for i in range(len(pivots) - 1):
            offset = pivots[i]
            size = pivots[i + 1] - offset
            # Combine chunks otherwise arrow has a serialization issue with giant memory
            to_return.append(self.data.slice(offset, size).combine_chunks())
        if len(pivots) > 0:
            to_return.append(self.data.slice(pivots[-1]).combine_chunks())
        return to_return

    @staticmethod
    def _merge_blocks(blocks: List[DataBlock[ArrowArrType]]) -> DataBlock[ArrowArrType]:
        all_chunks = []
        for block in blocks:
            all_chunks.extend(block.data.chunks)
        return DataBlock.make_block(pa.chunked_array(all_chunks))

    def _make_empty(self) -> DataBlock[ArrowArrType]:
        return DataBlock.make_block(data=pa.chunked_array([[]], type=self.data.type))

    def sample(self, num: int) -> DataBlock[ArrowArrType]:
        if num >= len(self):
            return self
        sampled = np.random.choice(self.data, num, replace=False)
        return DataBlock.make_block(data=pa.chunked_array([sampled]))

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
            return DataBlock.make_block(data=hashed)
        else:
            seed_pa_type = seed.data.type
            if not (pa.types.is_uint64(seed_pa_type)):
                raise TypeError(f"can only seed hash uint64 not {seed_pa_type}")

            seed_arr = seed.data.to_numpy()
            seed_arr = seed_arr ^ (hashed.to_numpy() + 0x9E3779B9 + (seed_arr << 6) + (seed_arr >> 2))
            return DataBlock.make_block(data=pa.chunked_array([seed_arr]))

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
        group_arrs = [a.data for a in group_by]
        agg_arrs = [a.data for a in to_agg]
        arrs = group_arrs + agg_arrs
        group_names = [f"g_{i}" for i in range(len(group_by))]
        agg_names = [f"a_{i}" for i in range(len(to_agg))]
        table = pa.table(arrs, names=group_names + agg_names)
        pl_table = pl.from_arrow(table, rechunk=True)

        exprs = []
        grouped_expected_arrow_type = [a.type for a in group_arrs]
        agg_expected_arrow_type = []
        for an, op, arr in zip(agg_names, agg_ops, agg_arrs):
            if op == "sum":
                exprs.append(pl.sum(an))
                agg_expected_arrow_type.append(arr.type)
            elif op == "mean":
                exprs.append(pl.mean(an))
                agg_expected_arrow_type.append(pa.float64())
            elif op == "min":
                exprs.append(pl.min(an))
                agg_expected_arrow_type.append(arr.type)
            elif op == "max":
                exprs.append(pl.max(an))
                agg_expected_arrow_type.append(arr.type)
            elif op == "count":
                exprs.append(pl.count(an))
                agg_expected_arrow_type.append(pa.int64())
            else:
                raise NotImplementedError()

        pl_agged = pl_table.groupby(group_names).agg(exprs)
        agged = pl_agged.to_arrow()
        gcols: List[DataBlock] = [
            DataBlock.make_block(agged[g_name].cast(t)) for g_name, t in zip(group_names, grouped_expected_arrow_type)
        ]
        acols: List[DataBlock] = [
            DataBlock.make_block(agged[f"{a_name}"].cast(t)) for a_name, t in zip(agg_names, agg_expected_arrow_type)
        ]
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

    def list_explode(self) -> Tuple[DataBlock[ArrType], DataBlock[ArrowArrType]]:
        """Treats each row as a list and flattens the nested lists. Will throw an error if
        elements in the block cannot be treated as a list.
        """
        data = self.data
        data = pac.if_else(pac.equal(pac.list_value_length(data), pa.scalar(0)), pa.scalar([None], data.type), data)
        data = pac.if_else(data.is_null(), pa.scalar([None], data.type), data)
        exploded = pac.list_flatten(data)
        list_lengths = pac.list_value_length(data)
        return (
            DataBlock.make_block(exploded),
            DataBlock.make_block(list_lengths),
        )


def arrow_mod(arr, m):
    if isinstance(m, pa.Scalar):
        return np.mod(arr, m.as_py())
    return np.mod(arr, m)


def arrow_floordiv(arr, m):
    floordiv = pac.floor(pac.divide(arr, m))
    if pa.types.is_integer(arr.type) and pa.types.is_integer(m.type):
        floordiv = floordiv.cast(pa.int64())
    return floordiv


def arrow_str_contains(arr: pa.ChunkedArray, pattern: pa.StringScalar):
    if pa.types.is_null(arr.type):
        return arr
    substring_counts = pac.count_substring(arr, pattern=pattern.as_py())
    return pac.not_equal(substring_counts, pa.scalar(0))


def arrow_str_endswith(arr: pa.ChunkedArray, pattern: pa.StringScalar):
    if pa.types.is_null(arr.type):
        return arr
    return pac.ends_with(arr, pattern=pattern.as_py())


def arrow_str_startswith(arr: pa.ChunkedArray, pattern: pa.StringScalar):
    if pa.types.is_null(arr.type):
        return arr
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
    EXPLODE = _arr_unary_op(pac.list_flatten)
    IF_ELSE = _arr_ternary_op(pac.if_else)

    # Placeholders: these change the cardinality of the block and should never be evaluated from the Evaluator
    # They exist on the Evaluator only to provide correct typing information
    SUM = ArrowDataBlock.identity
    MEAN = ArrowDataBlock.identity
    MIN = ArrowDataBlock.identity
    MAX = ArrowDataBlock.identity
    COUNT = ArrowDataBlock.identity
    EXPLODE = ArrowDataBlock.identity


ArrowDataBlock.evaluator = ArrowEvaluator

IN_1 = TypeVar("IN_1")
IN_2 = TypeVar("IN_2")
OUT = TypeVar("OUT")


def make_map_unary(f: Callable[[IN_1], OUT]) -> Callable[[DataBlock[Sequence[IN_1]]], PyListDataBlock[OUT]]:
    def map_f(values: DataBlock[Sequence[IN_1]]) -> PyListDataBlock[OUT]:
        return PyListDataBlock(list(map(f, (tup[0] for tup in zip_blocks_as_py(values)))))

    return map_f


def make_map_binary(
    f: Callable[[IN_1, IN_2], OUT]
) -> Callable[[DataBlock[Sequence[IN_1]], DataBlock[Sequence[IN_2]]], PyListDataBlock[OUT]]:
    def map_f(values: DataBlock[Sequence[IN_1]], others: DataBlock[Sequence[IN_2]]) -> PyListDataBlock[OUT]:
        return PyListDataBlock(list(f(v, o) for v, o in zip_blocks_as_py(values, others)))

    return map_f


def assert_invalid_pylist_operation(*args, **kwargs):
    raise AssertionError(f"This is an invalid operation on a PyListDataBlock and should never be executing")


def pylist_is_none(obj: Any):
    return obj is None


def pylist_if_else(
    cond: DataBlock[Sequence[IN_1]], x: DataBlock[Sequence[IN_2]], y: DataBlock[Sequence[IN_2]]
) -> PyListDataBlock[IN_2]:
    return PyListDataBlock([xitem if c else yitem for c, xitem, yitem in zip_blocks_as_py(cond, x, y)])


class PyListEvaluator(OperatorEvaluator["PyListDataBlock"]):
    NEGATE = make_map_unary(operator.neg)
    POSITIVE = PyListDataBlock.identity
    ABS = make_map_unary(operator.abs)
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

    # Placeholders: these change the cardinality of the block and should never be evaluated from the Evaluator
    # They exist on the Evaluator only to provide correct typing information
    SUM = PyListDataBlock.identity
    MEAN = PyListDataBlock.identity
    MIN = PyListDataBlock.identity
    MAX = PyListDataBlock.identity
    COUNT = PyListDataBlock.identity
    EXPLODE = PyListDataBlock.identity

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
