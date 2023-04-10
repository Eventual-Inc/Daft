from __future__ import annotations

import inspect
import sys
from typing import Callable, List, Sequence

from daft.datatype import DataType
from daft.expressions import Expression

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


_PythonFunction = Callable[..., Sequence]


def udf_input_type_from_type_hint(hint: type) -> str:
    if hint == list or hint == List or get_origin(hint) == list or get_origin(hint) == List:
        return "list"
    elif _NUMPY_AVAILABLE and (hint == np.ndarray or get_origin(hint) == np.ndarray):
        return "numpy"
    elif _PANDAS_AVAILABLE and hint == pd.Series:
        return "pandas"
    elif _PYARROW_AVAILABLE and hint == pa.Array:
        return "pyarrow"
    elif _POLARS_AVAILABLE and hint == polars.Series:
        return "polars"
    raise ValueError(f"UDF input array type {hint} is not supported")


class UDF:
    def __init__(self, f: _PythonFunction, input_columns: dict[str, type], return_dtype: DataType):
        self._f = f
        self._input_types = {
            arg_name: udf_input_type_from_type_hint(type_hint) for arg_name, type_hint in input_columns.items()
        }
        self._func_ret_type = return_dtype
        self._func_signature = inspect.signature(f)

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
        bound_args = self._func_signature.bind(*args, **kwargs)
        bound_args.apply_defaults()

        # Validate that arguments specified in input_columns receive Expressions as input
        for arg_name, arg in bound_args.arguments.items():
            if arg_name in self._input_types and not isinstance(arg, Expression):
                raise TypeError(
                    f"`{arg_name}` is an input_column. UDF expects an Expression as input but received instead: {arg}"
                )
            if arg_name not in self._input_types and isinstance(arg, Expression):
                raise TypeError(
                    f"`{arg_name}` is not an input_column. UDF expects a non-Expression as input but received: {arg}"
                )

        return Expression.udf(
            func=self._f,
            input_types=self._input_types,
            bound_args=bound_args,
        )


def udf(
    *,
    return_dtype: DataType,
    input_columns: dict[str, type],
) -> Callable[[_PythonFunction], UDF]:
    def _udf(f: _PythonFunction) -> UDF:
        return UDF(
            f,
            input_columns,
            return_dtype,
        )

    return _udf
