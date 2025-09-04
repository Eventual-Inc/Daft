from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from .dependencies import pa, pydantic

try:
    from pydantic import BaseModel
    from pydantic_to_pyarrow import get_pyarrow_schema
except ImportError as err:
    err.msg = f"pydantic is missing. Install with `pip install daft[pydantic]` or install your own compatible Pydantic version.\n{err.msg}"
    raise err


if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import NoneType
    from typing import Any, TypeVar, Union, get_args, get_origin

    P = TypeVar("P", bound=BaseModel)

    A = TypeVar("A", bound=pa.StructType)


from .datatype import DataType

__all__: Sequence[str] = (
    "daft_pyarrow_datatype",
    "infer_daft_arrow_function_types",
    "pyarrow_datatype",
)


def daft_pyarrow_datatype(f_type: type[Any]) -> DataType:
    """Produces the appropriate Daft DataType given a Python type.

    Supports the following types:
        - built-ins (str, int, float, bool)
        - Pydantic BaseModel instances
        - lists
        - dicts
        - any combination of the above!

    Uses :func:`pyarrow_datatype` to determine the appropriate Arrow type, then uses that
    as the basis for the Daft data type.
    """
    return DataType.from_arrow_type(pyarrow_datatype(f_type))


def pyarrow_datatype(f_type: type[Any]) -> pa.DataType:
    if get_origin(f_type) is Union:
        targs = get_args(f_type)
        if len(targs) == 2:
            if targs[0] is NoneType and targs[1] is not NoneType:
                refined_inner = targs[1]
            elif targs[0] is not NoneType and targs[1] is NoneType:
                refined_inner = targs[0]
            else:
                raise TypeError(f"Cannot convert a general union type {f_type} into a pyarrow.DataType!")
            inner_type = pyarrow_datatype(refined_inner)
        else:
            raise TypeError(f"Cannot convert a general union type {f_type} into a pyarrow.DataType!")

    elif get_origin(f_type) is list:
        targs = get_args(f_type)
        if len(targs) != 1:
            raise TypeError(
                f"Expected list type {f_type} with inner element type but " f"got {len(targs)} inner-types: {targs}"
            )
        element_type = targs[0]
        inner_type = pa.list_(pyarrow_datatype(element_type))

    elif get_origin(f_type) is dict:
        targs = get_args(f_type)
        if len(targs) != 2:
            raise TypeError(
                f"Expected dict type {f_type} with inner key-value types but got " f"{len(targs)} inner-types: {targs}"
            )
        kt, vt = targs
        pyarrow_kt = pyarrow_datatype(kt)
        pyarrow_vt = pyarrow_datatype(vt)
        inner_type = pa.map_(pyarrow_kt, pyarrow_vt)

    elif get_origin(f_type) is tuple:
        raise TypeError(f"Cannot support tuple types: {f_type}")

    elif issubclass(f_type, pydantic.BaseModel):
        schema = get_pyarrow_schema(f_type)
        inner_type = pa.struct([(f, schema.field(f).type) for f in schema.names])

    elif issubclass(f_type, str):
        inner_type = pa.string()

    elif issubclass(f_type, int):
        inner_type = pa.int64()

    elif issubclass(f_type, float):
        inner_type = pa.float64()

    elif issubclass(f_type, bool):
        inner_type = pa.bool_()

    elif issubclass(f_type, bytes):
        inner_type = pa.binary()

    elif issubclass(f_type, datetime):
        inner_type = pa.date64()

    elif issubclass(f_type, timedelta):
        raise NotImplementedError(
            "TODO: handle conversion of (days, seconds, microseconds) " "into duration with a single unit!"
        )
        # inner_type = pyarrow.duration(timeunit=???)
    else:
        raise TypeError(f"Cannot handle general Python objects in Arrow: {f_type}")

    return inner_type


def infer_daft_arrow_function_types(f: callable) -> tuple[DataType, DataType]:
    """Produces the Daft DataTypes for the given function's annotated input and output."""
    try:
        func_annos: dict[str, type] = f.__annotations__  # type: ignore
    except AttributeError as err:
        raise ValueError(f"Need to supply type-annotated function, not: {f}") from err

    if len(func_annos) == 0:
        raise ValueError(
            "Need to provide type annotations to function! " f"Cannot infer input and output types of: {f}"
        )

    if len(func_annos) != 2:
        raise ValueError(
            "Function must have 1 input and 1 output. "
            f"Found a total of {len(func_annos)} type annotations: {func_annos}"
        )

    if "return" not in func_annos:
        raise ValueError(f"Function is missing annotated return type. Only found input types: {func_annos}")

    # exactly 1 output ('return') and one input (the other named one)

    input_annos = dict(**func_annos)
    del input_annos["return"]
    input_daft = daft_pyarrow_datatype(func_annos[list(input_annos.keys())[0]])

    output_daft = daft_pyarrow_datatype(func_annos["return"])

    return input_daft, output_daft
