from __future__ import annotations

import builtins
from typing import TYPE_CHECKING

import pyarrow as pa

from daft.daft import PyDataType

if TYPE_CHECKING:
    pass


class DataType:
    _dtype: PyDataType

    def __init__(self) -> None:
        raise NotImplementedError(
            "We do not support creating a DataType via __init__ "
            "use a creator method like DataType.int32() or use DataType.from_arrow_type(pa_type)"
        )

    @staticmethod
    def _from_pydatatype(pydt: PyDataType) -> DataType:
        dt = DataType.__new__(DataType)
        dt._dtype = pydt
        return dt

    @classmethod
    def int8(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.int8())

    @classmethod
    def int16(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.int16())

    @classmethod
    def int32(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.int32())

    @classmethod
    def int64(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.int64())

    @classmethod
    def uint8(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.uint8())

    @classmethod
    def uint16(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.uint16())

    @classmethod
    def uint32(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.uint32())

    @classmethod
    def uint64(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.uint64())

    @classmethod
    def float32(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.float32())

    @classmethod
    def float64(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.float64())

    @classmethod
    def string(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.string())

    @classmethod
    def bool(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.bool())

    @classmethod
    def binary(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.binary())

    @classmethod
    def null(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.null())

    @classmethod
    def date(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.date())

    @classmethod
    def fixed_size_list(cls, name: str, dtype: DataType, size: int) -> DataType:
        return cls._from_pydatatype(PyDataType.fixed_size_list(name, dtype._dtype, size))

    @classmethod
    def from_arrow_type(cls, arrow_type: pa.lib.DataType) -> DataType:
        if pa.types.is_int8(arrow_type):
            return cls.int8()
        elif pa.types.is_int16(arrow_type):
            return cls.int16()
        elif pa.types.is_int32(arrow_type):
            return cls.int32()
        elif pa.types.is_int64(arrow_type):
            return cls.int64()
        elif pa.types.is_uint8(arrow_type):
            return cls.uint8()
        elif pa.types.is_uint16(arrow_type):
            return cls.uint16()
        elif pa.types.is_uint32(arrow_type):
            return cls.uint32()
        elif pa.types.is_uint64(arrow_type):
            return cls.uint64()
        elif pa.types.is_float32(arrow_type):
            return cls.float32()
        elif pa.types.is_float64(arrow_type):
            return cls.float64()
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return cls.string()
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return cls.binary()
        elif pa.types.is_boolean(arrow_type):
            return cls.bool()
        elif pa.types.is_fixed_size_list(arrow_type):
            assert isinstance(arrow_type, pa.FixedSizeListType)
            field = arrow_type.value_field
            return cls.fixed_size_list(field.name, cls.from_arrow_type(field.type), arrow_type.list_size)
        elif pa.types.is_null(arrow_type):
            return cls.null()
        elif pa.types.is_date32(arrow_type):
            return cls.date()
        else:
            raise NotImplementedError(f"we cant convert arrow type: {arrow_type} to a daft type")

    @classmethod
    def python(cls) -> DataType:
        return cls._from_pydatatype(PyDataType.python())

    def _is_python_type(self) -> builtins.bool:
        # NOTE: This is currently used in a few places still. We can get rid of it once these are refactored away. To be discussed.
        # 1. Visualizations - we can get rid of it if we do all our repr and repr_html logic in a Series instead of in Python
        # 2. Hypothesis test data generation - we can get rid of it if we allow for creation of Series from a Python list and DataType

        return self == DataType.python()

    def __repr__(self) -> str:
        return self._dtype.__repr__()

    def __eq__(self, other: object) -> builtins.bool:
        return isinstance(other, DataType) and self._dtype.is_equal(other._dtype)

    def __getstate__(self) -> bytes:
        return self._dtype.__getstate__()

    def __setstate__(self, state: bytes) -> None:
        self._dtype = PyDataType.__new__(PyDataType)
        self._dtype.__setstate__(state)

    def __hash__(self) -> int:
        return self._dtype.__hash__()
