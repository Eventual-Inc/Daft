from __future__ import annotations

import logging
from dataclasses import Field as DataclassField
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
from typing import TypeVar, Union, get_args, get_origin

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from typing import Any


from .datatype import DataType

__all__: Sequence[str] = (
    "daft_pyarrow_datatype",
    # "pyarrow_datatype",
    # "infer_daft_arrow_function_types",
)


def daft_pyarrow_datatype(f_type: type[Any]) -> DataType:
    """Produces the appropriate Daft DataType given a Python type.

    Supports the following types:
        - built-ins (str, int, float, bool, bytes)
        - datetime instances
        - Pydantic BaseModel instances
        - lists
        - dicts
        - any combination of the above!

    Uses :func:`pyarrow_datatype` to determine the appropriate Arrow type, then uses that
    as the basis for the Daft data type.
    """
    try:
        return _daft_datatype_for(f_type)
    except TypeError as e:
        if str(e) == "issubclass() arg 1 must be a class":
            # drop the underlying error as it will be noise
            raise TypeError(
                f"You must supply a type, not an instance of a type. You provided: {type(f_type)}={f_type!r}"
            ) from e
        else:
            raise e


def _daft_datatype_for(f_type: type[Any]) -> DataType:
    """Implements logic of :func:`daft_pyarrow_datatype`."""
    print(f"\t{f_type=} | {type(f_type)=}")
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
            import ipdb

            ipdb.set_trace()
            generic_vars = get_args(f_type.__origin__.__orig_bases__[0])
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
                import ipdb

                ipdb.set_trace()
                field_names_types[inner_f_name] = _daft_datatype_for(inner_f_type)

        inner_type = DataType.struct(field_names_types)

    # bool must come before int in type checking!
    elif issubclass(f_type, bool):
        inner_type = DataType.bool()

    elif issubclass(f_type, BaseModel):
        # schema = get_pyarrow_schema(f_type, allow_losing_tz=True)
        # inner_type = DataType.from_arrow_type(pa.struct([(f, schema.field(f).type) for f in schema.names]))
        field_names_types: dict[str, DataType] = {}
        for inner_f_name, inner_field_info in f_type.model_fields.items():
            field_names_types[inner_f_name] = _daft_datatype_for(inner_field_info.annotation)
        inner_type = DataType.struct(field_names_types)

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


def _dataclass_field_types_defaults(
    dataclass_type: type,
) -> list[tuple[str, type]]:
    """Obtain the fields & their expected types for the given @dataclass type."""
    if hasattr(dataclass_type, "__origin__"):
        dataclass_fields = dataclass_type.__origin__.__dataclass_fields__  # type: ignore
        generic_to_concrete = dict(_align_generic_concrete(dataclass_type))

        def handle_field(data_field: Field) -> Tuple[str, Type, Optional[Any]]:
            if data_field.type in generic_to_concrete:
                typ = generic_to_concrete[data_field.type]
            elif hasattr(data_field.type, "__parameters__"):
                tn = _fill(generic_to_concrete, data_field.type)
                typ = _exec(data_field.type.__origin__, tn)
            else:
                typ = data_field.type
            return data_field.name, typ, _default_of(data_field)

    else:
        dataclass_fields = dataclass_type.__dataclass_fields__  # type: ignore

        def handle_field(data_field: Field) -> Tuple[str, Type, Optional[Any]]:
            return data_field.name, data_field.type, _default_of(data_field)

    return list(map(handle_field, dataclass_fields.values()))


def _align_generic_concrete(
    data_type_with_generics: type,
) -> Iterator[tuple[type, type]]:
    """Yields pairs of (parameterized type name, runtime type value) for the input type.

    Accepts a class type that has parameterized generic types.
    Returns an iterator that yields pairs of (generic type variable name, instantiated type).

    NOTE: If the supplied type derives from a Sequence or Mapping,
          then the generics will be handled appropriately.
    """
    try:
        origin = data_type_with_generics.__origin__  # type: ignore
        if issubclass(origin, Sequence):
            generics = [TypeVar("T")]  # type: ignore
            values = get_args(data_type_with_generics)
        elif issubclass(origin, Mapping):
            generics = [TypeVar("KT"), TypeVar("VT_co")]  # type: ignore
            values = get_args(data_type_with_generics)
        else:
            # should be a dataclass
            generics = origin.__parameters__  # type: ignore
            values = get_args(data_type_with_generics)  # type: ignore
        for g, v in zip(generics, values):
            yield g, v  # type: ignore
    except AttributeError as e:
        raise ValueError(
            f"Cannot find __origin__, __dataclass_fields__ on type '{data_type_with_generics}'",
            e,
        )


def _default_of(data_field: DataclassField) -> Any | None:
    return (
        None if isinstance(data_field.default, _MISSING_TYPE) else serialize(data_field.default, no_none_values=False)
    )


# def daft_pyarrow_datatype(f_type: type[Any]) -> DataType:
#     """Produces the appropriate Daft DataType given a Python type.

#     Supports the following types:
#         - built-ins (str, int, float, bool, bytes)
#         - datetime instances
#         - Pydantic BaseModel instances
#         - lists
#         - dicts
#         - any combination of the above!

#     Uses :func:`pyarrow_datatype` to determine the appropriate Arrow type, then uses that
#     as the basis for the Daft data type.
#     """
#     return DataType.from_arrow_type(pyarrow_datatype(f_type))


# def pyarrow_datatype(f_type: type[Any]) -> pa.DataType:
#     """Determines the correct Arrow type to use for a given input type.

#     Follows same rules as :func:`daft_pyarrow_datatype`.
#     """
#     if get_origin(f_type) is Union:
#         targs = get_args(f_type)
#         if len(targs) == 2:
#             if targs[0] is NoneType and targs[1] is not NoneType:
#                 refined_inner = targs[1]
#             elif targs[0] is not NoneType and targs[1] is NoneType:
#                 refined_inner = targs[0]
#             else:
#                 raise TypeError(f"Cannot convert a general union type {f_type} into a pyarrow.DataType!")
#             inner_type = pyarrow_datatype(refined_inner)

#         else:
#             raise TypeError(f"Cannot convert a general union type {f_type} into a pyarrow.DataType!")

#     elif get_origin(f_type) is list:
#         targs = get_args(f_type)
#         if len(targs) != 1:
#             raise TypeError(
#                 f"Expected list type {f_type} with inner element type but " f"got {len(targs)} inner-types: {targs}"
#             )
#         element_type = targs[0]
#         inner_type = pa.list_(pyarrow_datatype(element_type))

#     elif get_origin(f_type) is dict:
#         targs = get_args(f_type)
#         if len(targs) != 2:
#             raise TypeError(
#                 f"Expected dict type {f_type} with inner key-value types but got " f"{len(targs)} inner-types: {targs}"
#             )
#         kt, vt = targs
#         pyarrow_kt = pyarrow_datatype(kt)
#         pyarrow_vt = pyarrow_datatype(vt)
#         inner_type = pa.map_(pyarrow_kt, pyarrow_vt)

#     elif get_origin(f_type) is tuple:
#         raise TypeError(f"Cannot support tuple types: {f_type}")

#     # bool must come before int in type checking!
#     elif issubclass(f_type, bool):
#         inner_type = pa.bool_()

#     elif issubclass(f_type, BaseModel):
#         schema = get_pyarrow_schema(f_type, allow_losing_tz=True)
#         inner_type = pa.struct([(f, schema.field(f).type) for f in schema.names])

#     elif issubclass(f_type, str):
#         inner_type = pa.string()

#     elif issubclass(f_type, int):
#         inner_type = pa.int64()

#     elif issubclass(f_type, float):
#         inner_type = pa.float64()

#     elif issubclass(f_type, bytes):
#         inner_type = pa.binary()

#     elif issubclass(f_type, datetime):
#         inner_type = pa.date64()

#     elif issubclass(f_type, timedelta):
#         # inner_type = pyarrow.duration(timeunit=???)
#         raise TypeError(
#             "Unimplemented: handle conversion of (days, seconds, microseconds) into duration with a single unit!"
#         )
#     else:
#         raise TypeError(f"Cannot handle general Python objects in Arrow: {f_type}")

#     return inner_type


# def infer_daft_arrow_function_types(f: Callable[[Any, ...], Any]) -> tuple[DataType, DataType]:  # type: ignore
#     """Produces the Daft DataTypes for the given function's annotated input and output."""
#     try:
#         func_annos: dict[str, type] = f.__annotations__
#     except AttributeError as err:
#         raise ValueError(f"Need to supply type-annotated function, not: {f}") from err

#     if len(func_annos) == 0:
#         raise ValueError(
#             "Need to provide type annotations to function! " f"Cannot infer input and output types of: {f}"
#         )

#     if len(func_annos) != 2:
#         raise ValueError(
#             "Function must have 1 input and 1 output. "
#             f"Found a total of {len(func_annos)} type annotations: {func_annos}"
#         )

#     if "return" not in func_annos:
#         raise ValueError(f"Function is missing annotated return type. Only found input types: {func_annos}")

#     # exactly 1 output ('return') and one input (the other named one)

#     input_annos = dict(**func_annos)
#     del input_annos["return"]
#     input_daft = daft_pyarrow_datatype(func_annos[next(iter(input_annos.keys()))])

#     output_daft = daft_pyarrow_datatype(func_annos["return"])

#     return input_daft, output_daft
