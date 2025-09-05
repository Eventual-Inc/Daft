from __future__ import annotations

import logging
from dataclasses import is_dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, get_type_hints

logger = logging.getLogger(__name__)

from .dependencies import pyd, pyd_arr

try:
    BaseModel = pyd.BaseModel
    get_pyarrow_schema = pyd_arr.get_pyarrow_schema
except ImportError as err:
    err.msg = f"pydantic is missing. Install with `pip install daft[pydantic]` or install your own compatible Pydantic version.\n{err.msg}"
    raise err

from types import NoneType, UnionType  # type: ignore
from typing import Generic, TypeVar, Union, get_args, get_origin

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any


from .datatype import DataType

__all__: Sequence[str] = ("daft_datatype_for",)


def daft_datatype_for(f_type: type[Any]) -> DataType:
    """Produces the appropriate Daft DataType given a Python type.

    Supports the following types:
        - built-ins (str, int, float, bool, bytes)
        - datetime classes
        - Pydantic BaseModel classes
        - @dataclass(frozen=True) decorated classes
        - Generics in BaseModel or @dataclass classes
        - lists
        - dicts
        - any combination of the above!
    """
    try:
        return _daft_datatype_for(f_type)
    except TypeError as e:
        if str(e) == "issubclass() arg 1 must be a class":
            raise TypeError(
                f"You must supply a type, not an instance of a type. You provided: {type(f_type)}={f_type!r}"
            ) from e
        else:
            raise e


def _daft_datatype_for(f_type: type[Any]) -> DataType:
    """Implements logic of :func:`daft_pyarrow_datatype`."""
    if get_origin(f_type) is Union or get_origin(f_type) is UnionType:
        targs = get_args(f_type)
        if len(targs) == 2:
            if targs[0] is NoneType and targs[1] is not NoneType:
                refined_inner = targs[1]
            elif targs[0] is not NoneType and targs[1] is NoneType:
                refined_inner = targs[0]
            else:
                raise TypeError(f"Cannot convert a general union type {f_type} into a pyarrow.DataType!")
            inner_type = _daft_datatype_for(refined_inner)

        else:
            raise TypeError(f"Cannot convert a general union type {f_type} into a pyarrow.DataType!")

    elif get_origin(f_type) is list:
        targs = get_args(f_type)
        if len(targs) != 1:
            raise TypeError(
                f"Expected list type {f_type} with inner element type but got {len(targs)} inner-types: {targs}"
            )
        element_type = targs[0]
        inner_type = DataType.list(_daft_datatype_for(element_type))

    elif get_origin(f_type) is dict:
        targs = get_args(f_type)
        if len(targs) != 2:
            raise TypeError(
                f"Expected dict type {f_type} with inner key-value types but got " f"{len(targs)} inner-types: {targs}"
            )
        kt, vt = targs
        pyarrow_kt = _daft_datatype_for(kt)
        pyarrow_vt = _daft_datatype_for(vt)
        inner_type = DataType.map(pyarrow_kt, pyarrow_vt)

    elif get_origin(f_type) is tuple:
        raise TypeError(f"Cannot support tuple types: {f_type}")

    elif is_dataclass(f_type) or (hasattr(f_type, "__origin__") and is_dataclass(f_type.__origin__)):
        field_names_types: dict[str, DataType] = {}

        # handle a data class parameterized by generics
        if hasattr(f_type, "__origin__"):
            for base in f_type.__origin__.__orig_bases__:
                base_origin = get_origin(base)
                if base_origin is not None and base_origin == Generic:
                    generic_vars = get_args(base)
                    break
            else:
                raise TypeError(f"@dataclass {f_type} has origin but doesn't extend Generic!")
            concrete_types = get_args(f_type)
            generic_to_type: dict[TypeVar, type] = dict(zip(generic_vars, concrete_types))

            type_annotations: dict[str, type | TypeVar] = get_type_hints(f_type.__origin__)

            for inner_f_name, inner_f_type_or_type_var in type_annotations.items():
                if isinstance(inner_f_type_or_type_var, TypeVar):
                    inner_f_type = generic_to_type[inner_f_type_or_type_var]
                    field_names_types[inner_f_name] = _daft_datatype_for(inner_f_type)
                else:
                    field_names_types[inner_f_name] = _daft_datatype_for(inner_f_type_or_type_var)
        else:
            # straightforward: all concrete types
            for inner_f_name, inner_f_type in get_type_hints(f_type).items():
                field_names_types[inner_f_name] = _daft_datatype_for(inner_f_type)

        inner_type = DataType.struct(field_names_types)

    elif issubclass(f_type, BaseModel):
        # schema = get_pyarrow_schema(f_type, allow_losing_tz=True)
        # inner_type = DataType.from_arrow_type(pa.struct([(f, schema.field(f).type) for f in schema.names]))
        field_names_types = {}
        for inner_f_name, inner_field_info in f_type.model_fields.items():
            field_names_types[inner_f_name] = _daft_datatype_for(inner_field_info.annotation)
        inner_type = DataType.struct(field_names_types)

    # bool must come before int in type checking!
    elif issubclass(f_type, bool):
        inner_type = DataType.bool()

    elif issubclass(f_type, str):
        inner_type = DataType.string()

    elif issubclass(f_type, int):
        inner_type = DataType.int64()

    elif issubclass(f_type, float):
        inner_type = DataType.float64()

    elif issubclass(f_type, bytes):
        inner_type = DataType.binary()

    elif issubclass(f_type, datetime):
        inner_type = DataType.date()

    elif issubclass(f_type, timedelta):
        # inner_type = pyarrow.duration(timeunit=???)
        raise TypeError(
            "Unimplemented: handle conversion of (days, seconds, microseconds) into duration with a single unit!"
        )

    else:
        raise TypeError(f"Cannot handle general Python objects in Arrow: {f_type}")

    return inner_type
