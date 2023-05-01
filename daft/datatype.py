from __future__ import annotations

import builtins

import pyarrow as pa

from daft.daft import PyDataType


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
    def list(cls, name: str, dtype: DataType) -> DataType:
        return cls._from_pydatatype(PyDataType.list(name, dtype._dtype))

    @classmethod
    def fixed_size_list(cls, name: str, dtype: DataType, size: int) -> DataType:
        if not isinstance(size, int) or size <= 0:
            raise ValueError("The size for a fixed-size list must be a positive integer, but got: ", size)
        return cls._from_pydatatype(PyDataType.fixed_size_list(name, dtype._dtype, size))

    @classmethod
    def struct(cls, fields: dict[str, DataType]) -> DataType:
        return cls._from_pydatatype(PyDataType.struct({name: datatype._dtype for name, datatype in fields.items()}))

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
        elif pa.types.is_null(arrow_type):
            return cls.null()
        elif pa.types.is_date32(arrow_type):
            return cls.date()
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            assert isinstance(arrow_type, (pa.ListType, pa.LargeListType))
            field = arrow_type.value_field
            return cls.list(field.name, cls.from_arrow_type(field.type))
        elif pa.types.is_fixed_size_list(arrow_type):
            assert isinstance(arrow_type, pa.FixedSizeListType)
            field = arrow_type.value_field
            return cls.fixed_size_list(field.name, cls.from_arrow_type(field.type), arrow_type.list_size)
        elif pa.types.is_struct(arrow_type):
            assert isinstance(arrow_type, pa.StructType)
            fields = [arrow_type[i] for i in range(arrow_type.num_fields)]
            return cls.struct({field.name: cls.from_arrow_type(field.type) for field in fields})
        else:
            # Fall back to a Python object type.
            # TODO(Clark): Add native support for remaining Arrow types and extension types.
            return cls.python()

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
