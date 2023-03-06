from __future__ import annotations

import datetime
from dataclasses import dataclass
from enum import Enum

_NUMPY_AVAILABLE = True
try:
    import numpy as np
except ImportError:
    _NUMPY_AVAILABLE = False

import pyarrow as pa


class ExpressionType:
    @staticmethod
    def unknown() -> ExpressionType:
        return _TYPE_REGISTRY["unknown"]

    @staticmethod
    def python_object() -> ExpressionType:
        return _TYPE_REGISTRY["pyobj"]

    @staticmethod
    def integer() -> ExpressionType:
        return _TYPE_REGISTRY["integer"]

    @staticmethod
    def float() -> ExpressionType:
        return _TYPE_REGISTRY["float"]

    @staticmethod
    def logical() -> ExpressionType:
        return _TYPE_REGISTRY["logical"]

    @staticmethod
    def string() -> ExpressionType:
        return _TYPE_REGISTRY["string"]

    @staticmethod
    def date() -> ExpressionType:
        return _TYPE_REGISTRY["date"]

    @staticmethod
    def bytes() -> ExpressionType:
        return _TYPE_REGISTRY["bytes"]

    @staticmethod
    def null() -> ExpressionType:
        return _TYPE_REGISTRY["null"]

    @staticmethod
    def _infer_from_py_type(t: type) -> ExpressionType:
        """Infers an ExpressionType from a Python type"""
        if t in _PY_TYPE_TO_EXPRESSION_TYPE:
            return _PY_TYPE_TO_EXPRESSION_TYPE[t]
        return ExpressionType.python(t)

    @staticmethod
    def python(obj_type: type) -> ExpressionType:
        return PythonExpressionType(obj_type)

    @staticmethod
    def from_arrow_type(datatype: pa.DataType) -> ExpressionType:
        if pa.types.is_list(datatype):
            return ExpressionType.python(list)
        elif pa.types.is_struct(datatype):
            return ExpressionType.python(dict)
        if datatype not in _PYARROW_TYPE_TO_EXPRESSION_TYPE:
            return ExpressionType.python_object()
        return _PYARROW_TYPE_TO_EXPRESSION_TYPE[datatype]

    @staticmethod
    def from_numpy_type(datatype: np.dtype) -> ExpressionType:
        return ExpressionType.from_arrow_type(pa.from_numpy_dtype(datatype))

    @staticmethod
    def _infer_type_from_list(data: list) -> ExpressionType:
        found_types = {type(o) for o in data} - {type(None)}
        if len(found_types) == 0:
            return ExpressionType.null()
        elif len(found_types) == 1:
            t = found_types.pop()
            return ExpressionType._infer_from_py_type(t)
        elif found_types == {int, float}:
            return ExpressionType.float()
        return ExpressionType.python_object()

    @staticmethod
    def _infer_type(data: list | np.ndarray | pa.Array) -> ExpressionType:
        """Infers an ExpressionType from the provided collection of data

        Args:
            data (list | np.ndarray): provided collection of data

        Returns:
            ExpressionType: Inferred ExpressionType
        """
        if isinstance(data, list):
            return ExpressionType._infer_type_from_list(data)
        elif _NUMPY_AVAILABLE and isinstance(data, np.ndarray):
            # TODO: change this logic once we support nested types
            if len(data.shape) > 1:
                return ExpressionType._infer_type_from_list(list(data))
            elif data.dtype == np.object:
                return ExpressionType._infer_type_from_list(list(data))
            else:
                return ExpressionType.from_numpy_type(data.dtype)
        elif isinstance(data, pa.Array) or isinstance(data, pa.ChunkedArray):
            return ExpressionType.from_arrow_type(data.type)
        else:
            raise ValueError(
                f"Expected inferred data to be of type list, np.ndarray or pa.Array, but received {type(data)}"
            )

    def to_arrow_type(self) -> pa.DataType:
        assert not self._is_python_type(), f"Cannot convert {self} to an Arrow type"
        return _EXPRESSION_TYPE_TO_PYARROW_TYPE[self]

    def _is_python_type(self) -> bool:
        raise NotImplementedError("Subclass to implement")


@dataclass(frozen=True, eq=True)
class PrimitiveExpressionType(ExpressionType):
    class TypeEnum(Enum):

        UNKNOWN = 1
        INTEGER = 2
        FLOAT = 3
        LOGICAL = 4
        STRING = 5
        DATE = 6
        BYTES = 7
        NULL = 8

    enum: PrimitiveExpressionType.TypeEnum

    def __repr__(self) -> str:
        return self.enum.name

    def _is_python_type(self) -> bool:
        return False


@dataclass(frozen=True, eq=True)
class PythonExpressionType(ExpressionType):
    python_cls: type

    def __repr__(self) -> str:
        return f"PY[{self.python_cls.__name__}]"

    def _is_python_type(self) -> bool:
        return True


_TYPE_REGISTRY: dict[str, ExpressionType] = {
    "unknown": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.UNKNOWN),
    "integer": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.INTEGER),
    "float": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.FLOAT),
    "logical": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.LOGICAL),
    "string": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.STRING),
    "date": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.DATE),
    "bytes": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.BYTES),
    "null": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.NULL),
    "pyobj": ExpressionType.python(object),
}


_EXPRESSION_TYPE_TO_PYARROW_TYPE = {
    _TYPE_REGISTRY["logical"]: pa.bool_(),
    _TYPE_REGISTRY["integer"]: pa.int64(),
    _TYPE_REGISTRY["float"]: pa.float64(),
    _TYPE_REGISTRY["string"]: pa.string(),
    _TYPE_REGISTRY["date"]: pa.date32(),
    _TYPE_REGISTRY["bytes"]: pa.binary(),
    _TYPE_REGISTRY["null"]: pa.null(),
}


_PYARROW_TYPE_TO_EXPRESSION_TYPE = {
    pa.null(): _TYPE_REGISTRY["null"],
    pa.bool_(): _TYPE_REGISTRY["logical"],
    pa.int8(): _TYPE_REGISTRY["integer"],
    pa.int16(): _TYPE_REGISTRY["integer"],
    pa.int32(): _TYPE_REGISTRY["integer"],
    pa.int64(): _TYPE_REGISTRY["integer"],
    pa.uint8(): _TYPE_REGISTRY["integer"],
    pa.uint16(): _TYPE_REGISTRY["integer"],
    pa.uint32(): _TYPE_REGISTRY["integer"],
    pa.uint64(): _TYPE_REGISTRY["integer"],
    pa.float16(): _TYPE_REGISTRY["float"],
    pa.float32(): _TYPE_REGISTRY["float"],
    pa.float64(): _TYPE_REGISTRY["float"],
    pa.date32(): _TYPE_REGISTRY["date"],
    # pa.date64(): _TYPE_REGISTRY["unknown"],
    pa.string(): _TYPE_REGISTRY["string"],
    pa.utf8(): _TYPE_REGISTRY["string"],
    pa.binary(): _TYPE_REGISTRY["bytes"],
    pa.large_binary(): _TYPE_REGISTRY["bytes"],
    pa.large_string(): _TYPE_REGISTRY["string"],
    pa.large_utf8(): _TYPE_REGISTRY["string"],
}

_PY_TYPE_TO_EXPRESSION_TYPE = {
    int: _TYPE_REGISTRY["integer"],
    float: _TYPE_REGISTRY["float"],
    str: _TYPE_REGISTRY["string"],
    bool: _TYPE_REGISTRY["logical"],
    datetime.date: _TYPE_REGISTRY["date"],
    bytes: _TYPE_REGISTRY["bytes"],
    type(None): _TYPE_REGISTRY["null"],
}
