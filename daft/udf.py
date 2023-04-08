from __future__ import annotations

from daft.datatype import DataType

import enum
import functools

from typing import Any, Callable, Sequence, Union, List

from daft.series import Series
from daft.expressions import Expression

import inspect
import sys
if sys.version_info < (3, 8):
    from typing_extensions import get_origin
else:
    from typing import get_origin


_POLARS_AVAILABLE = True
try:
    import polars  # noqa: F401
except ImportError:
    _POLARS_AVAILABLE = False

_NUMPY_AVAILABLE = True
try:
    import numpy as np  # noqa: F401
except ImportError:
    _NUMPY_AVAILABLE = False

_PANDAS_AVAILABLE = True
try:
    import pandas as pd  # noqa: F401
except ImportError:
    _PANDAS_AVAILABLE = False

_PYARROW_AVAILABLE = True
try:
    import pyarrow as pa  # noqa: F401
except ImportError:
    _PYARROW_AVAILABLE = False


_StatefulPythonFunction = type  # stateful UDFs are provided as Python Classes
_StatelessPythonFunction = Callable[..., Sequence]
_PythonFunction = Union[_StatefulPythonFunction, _StatelessPythonFunction]


class UdfInputType(enum.Enum):
    """Enum for the different types a UDF can pass inputs in as"""

    LIST = "list"
    NUMPY = "np"
    PANDAS = "pandas"
    PYARROW = "pyarrow"
    POLARS = "polars"

    @classmethod
    def from_type_hint(cls, hint: type | str) -> UdfInputType:
        if hint == list or hint == List or get_origin(hint) == list or get_origin(hint) == List:
            return UdfInputType.LIST
        elif _NUMPY_AVAILABLE and (hint == np.ndarray or get_origin(hint) == np.ndarray):
            return UdfInputType.NUMPY
        elif _PANDAS_AVAILABLE and hint == pd.Series:
            return UdfInputType.PANDAS
        elif _PYARROW_AVAILABLE and hint == pa.Array:
            return UdfInputType.PYARROW
        elif _POLARS_AVAILABLE and hint == polars.Series:
            return UdfInputType.POLARS
        raise ValueError(f"UDF input array type {hint} is not supported")


#     def convert_series(self, arg: Series) -> Any:
#         """Converts a UDF argument input to the appropriate user-facing container type"""
#         if self == UdfInputType.LIST:
#             return arg.to_pylist()
#         elif self == UdfInputType.NUMPY:
#             # TODO: [RUST-INT][PY] For Python types, we should do a arg.to_pylist() first instead of arrow
#             return np.array(arg.to_arrow())
#         elif self == UdfInputType.PANDAS:
#             return pd.Series(arg.to_numpy())
#         elif self == UdfInputType.PYARROW:
#             # TODO: [RUST-INT][PY] For Python types, we should throw an error early here
#             return arg.to_arrow()
#         elif self == UdfInputType.POLARS:
#             # TODO: [RUST-INT][PY] For Python types, we should do a arg.to_pylist() first instead of arrow
#             return polars.Series(arg.to_arrow())
#         raise NotImplementedError(f"Unsupported UDF input type {self}")


class UDF:
    def __init__(self, f: _PythonFunction, input_columns: dict[str, type], return_dtype: DataType):
        self._f = f
        self._input_types = {
            arg_name: UdfInputType.from_type_hint(type_hint) for arg_name, type_hint in input_columns.items()
        }
        self._func_ret_type = return_dtype

        # Get function argument names, excluding `self` if it is a class method
        call_method = f.__call__ if isinstance(f, type) else f
        self._ordered_func_arg_names = list(inspect.signature(call_method).parameters.keys())
        if isinstance(f, type):
            self._ordered_func_arg_names = self._ordered_func_arg_names[1:]

    @property
    def func(self) -> _PythonFunction:
        """Returns the wrapped function. Useful for local testing of your UDF.

        Example:

            >>> @udf(return_dtype=int, input_columns={"x": list})
            >>> def my_udf(x: list, y: int):
            >>>     return [i + y for i in x]
            >>>
            >>> assert my_udf.func([1, 2, 3], 1) == [2, 3, 4]

        Returns:
            _PythonFunction: The function (or class!) wrapped by the @udf decorator
        """
        return self._f

    def __call__(self, *args, **kwargs) -> Expression:
        """Call the UDF on arguments which can be Expressions, or normal Python values

        Raises:
            ValueError: if non-Expression objects are provided for parameters that are specified as `input_columns`

        Returns:
            UdfExpression: The resulting UDFExpression representing an execution of the UDF on its inputs
        """
        # Validate that arguments specified in input_columns receive Expressions as input
        arg_names_and_args = list(zip(self._ordered_func_arg_names, args)) + list(kwargs.items())
        for arg_name, arg in arg_names_and_args:
            if arg_name in self._input_types and not isinstance(arg, Expression):
                raise ValueError(
                    f"`{arg_name}` is an input_column. UDF expects an Expression as input but received instead: {arg}"
                )
            if arg_name not in self._input_types and isinstance(arg, Expression):
                raise ValueError(
                    f"`{arg_name}` is not an input_column. UDF expects a non-Expression as input but received: {arg}"
                )
        return Expression.udf(
            func=self._f,
            input_types=self._input_types,
            args=args,
            kwargs=kwargs,
        )


def udf(
    *,
    return_dtype: DataType,
    input_columns: dict[str, type],
) -> UDF:
    def _udf(f: _PythonFunction) -> UDF:
        return UDF(
            f,
            input_columns,
            return_dtype,
        )
    return _udf