from __future__ import annotations

import datetime
from dataclasses import dataclass
from enum import Enum

import pyarrow as pa


class ExpressionType:
    @staticmethod
    def is_primitive(t: ExpressionType) -> bool:
        return isinstance(t, PrimitiveExpressionType)

    @staticmethod
    def is_py(t: ExpressionType) -> bool:
        assert isinstance(t, ExpressionType), f"method must be called on an ExpressionType but got {type(t)}"
        return isinstance(t, PythonExpressionType)

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
    def from_py_type(obj_type: type) -> ExpressionType:
        """Gets the appropriate ExpressionType from a Python object, or _TYPE_REGISTRY["unknown"]
        if unable to find the appropriate type. ExpressionTypes.Python is never returned.
        """
        if obj_type not in _PY_TYPE_TO_EXPRESSION_TYPE:
            return PythonExpressionType(obj_type)
        return _PY_TYPE_TO_EXPRESSION_TYPE[obj_type]

    @staticmethod
    def from_arrow_type(datatype: pa.DataType) -> ExpressionType:
        if pa.types.is_list(datatype):
            return PythonExpressionType(list)
        elif pa.types.is_struct(datatype):
            return PythonExpressionType(dict)
        if datatype not in _PYARROW_TYPE_TO_EXPRESSION_TYPE:
            return ExpressionType.python_object()
        return _PYARROW_TYPE_TO_EXPRESSION_TYPE[datatype]

    def to_arrow_type(self) -> pa.DataType:
        assert not ExpressionType.is_py(self), f"Cannot convert {self} to an Arrow type"
        return _EXPRESSION_TYPE_TO_PYARROW_TYPE[self]


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


@dataclass(frozen=True, eq=True)
class PythonExpressionType(ExpressionType):
    python_cls: type

    def __repr__(self) -> str:
        return f"PY[{self.python_cls.__name__}]"


_TYPE_REGISTRY: dict[str, ExpressionType] = {
    "unknown": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.UNKNOWN),
    "integer": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.INTEGER),
    "float": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.FLOAT),
    "logical": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.LOGICAL),
    "string": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.STRING),
    "date": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.DATE),
    "bytes": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.BYTES),
    "null": PrimitiveExpressionType(PrimitiveExpressionType.TypeEnum.NULL),
    "pyobj": PythonExpressionType(object),
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
