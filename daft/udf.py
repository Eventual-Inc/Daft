from __future__ import annotations

import enum
import functools
import inspect
import logging
from typing import Any, Callable, List, Sequence, Union, get_origin, get_type_hints

from daft.execution.operators import ExpressionType
from daft.expressions import UdfExpression
from daft.runners.blocks import DataBlock

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

StatefulUDF = type  # stateful UDFs are provided as Python Classes
StatelessUDF = Callable[..., Sequence]
UDF = Union[StatefulUDF, StatelessUDF]


logger = logging.getLogger(__name__)


def _initialize_func(func):
    """Initializes a function if it is a class, otherwise noop"""
    try:
        return func() if isinstance(func, type) else func
    except:
        logger.error(f"Encountered error when initializing user-defined function {func.__name__}")
        raise


class UdfInputType(enum.Enum):
    """Enum for the different types a UDF can pass inputs in as"""

    UNKNOWN = 0
    LIST = 1
    NUMPY = 2
    PANDAS = 3
    PYARROW = 4
    PYARROW_CHUNKED = 5
    POLARS = 6


def _get_input_types_from_annotation(func: Callable) -> dict[str, UdfInputType]:
    """Parses a function's type annotations to determine the input types for each argument"""
    assert callable(func), f"Expected func to be callable, got {func}"

    type_hints = get_type_hints(func)
    param_types = {param: type_hints.get(param, None) for param in inspect.signature(func).parameters}

    udf_input_types = {}
    for name, annotation in param_types.items():
        if annotation == list or annotation == List or get_origin(annotation) == list or get_origin(annotation) == List:
            udf_input_types[name] = UdfInputType.LIST
        elif _NUMPY_AVAILABLE and (annotation == np.ndarray or get_origin(annotation) == np.ndarray):
            udf_input_types[name] = UdfInputType.NUMPY
        elif _PANDAS_AVAILABLE and annotation == pd.Series:
            udf_input_types[name] = UdfInputType.PANDAS
        elif _PYARROW_AVAILABLE and annotation == pa.Array:
            udf_input_types[name] = UdfInputType.PYARROW
        elif _PYARROW_AVAILABLE and annotation == pa.ChunkedArray:
            udf_input_types[name] = UdfInputType.PYARROW_CHUNKED
        elif _POLARS_AVAILABLE and annotation == polars.Series:
            udf_input_types[name] = UdfInputType.POLARS
        else:
            udf_input_types[name] = UdfInputType.UNKNOWN

    return udf_input_types


def _convert_argument(arg: Any, input_type: UdfInputType, partition_length: int) -> Any:
    """Converts a UDF argument input to the appropriate user-facing container type"""
    if isinstance(arg, DataBlock) and arg.is_scalar():
        return next(arg.iter_py())
    elif isinstance(arg, DataBlock):
        if input_type == UdfInputType.UNKNOWN or input_type == UdfInputType.LIST:
            arg_iterator = arg.iter_py()
            return [next(arg_iterator) for _ in range(partition_length)]
        elif input_type == UdfInputType.NUMPY:
            return arg.to_numpy()
        elif input_type == UdfInputType.PANDAS:
            return pd.Series(arg.to_numpy())
        elif input_type == UdfInputType.PYARROW:
            return arg.to_arrow().combine_chunks()
        elif input_type == UdfInputType.PYARROW_CHUNKED:
            return arg.to_arrow()
        elif input_type == UdfInputType.POLARS:
            return arg.to_polars()
        else:
            raise NotImplementedError(f"Unsupported UDF input type {input_type}")
    return arg


def udf(
    f: Callable | None = None,
    *,
    return_type: type,
    num_gpus: int | float | None = None,
    num_cpus: int | float | None = None,
    memory_bytes: int | float | None = None,
) -> Callable:
    """Decorator for creating a UDF. This decorator wraps any custom Python code into a funciton that can be used to process
    columns of data in a Daft DataFrame.

    .. NOTE::
        UDFs are much slower than native Daft expressions because they run Python code instead of Daft's optimized Rust kernels.
        You should only use UDFs when performing operations that are not supported by Daft's native expressions, or when you
        need to run custom Python code. For example, the following UDF will be much slower than ``df["x"] + 100``.

    Example:

    >>> @udf(return_type=int)
    >>> def add_val(x, val=1):
    >>>    # Your custom Python code here
    >>>    return [x + 1 for value in x]

    To invoke your UDF, you can use the ``DataFrame.with_column`` method:

    >>> df = DataFrame.from_pydict({"x": [1, 2, 3]})
    >>> df = df.with_column("x_add_100", add_val(df["x"], val=100))

    Input/Return Types
    ^^^^^^^^^^^^^^^^^^
    By default, Daft will pass columns of data into your function as Python lists. However, if this is a bottleneck for your
    application, you may choose more optimized types for your inputs by annotating your function inputs with type hints.

    In the following example, we annotate the ``x`` input parameter as a numpy array. Daft will now pass your data in as a Numpy
    array which is much more efficient to work with than a Python list.

    >>> import numpy as np
    >>>
    >>> @udf(return_type=int)
    >>> def add_val(x: np.ndarray, val: int = 1):
    >>>     return x + val

    Note also that Daft supports return types other than lists. In the above example, the returned value is a Numpy array as well.

    Input and Return types supported by Daft UDFs and their respective type annotations:

    1. Numpy Arrays (``np.ndarray``)
    2. Pandas Series (``pd.Series``)
    3. Polars Series (``polars.Series``)
    4. PyArrow Arrays (``pa.Array``)
    5. Python lists (``list`` or ``typing.List``)

    Stateful UDFs
    ^^^^^^^^^^^^^

    UDFs can also be created on Classes, which allow for initialization on some expensive state that can be shared
    between invocations of the class, for example downloading data or creating a model.

    >>> @udf(return_type=int)
    >>> class RunModel:
    >>>     def __init__(self):
    >>>         # Perform expensive initializations
    >>>         self._model = create_model()
    >>>
    >>>     def __call__(self, features_col):
    >>>         return self._model(features_col)

    Args:
        f: Function to wrap as a UDF, accepts column inputs as Numpy arrays and returns a column of data as a Polars Series/Numpy array/Python list/Pandas series.
        return_type: The return type of the UDF
        num_gpus: Deprecated - please use `DataFrame.with_column(..., resource_request=...)` instead
        num_cpus: Deprecated - please use `DataFrame.with_column(..., resource_request=...)` instead
        memory_bytes: Deprecated - please use `DataFrame.with_column(..., resource_request=...)` instead
    """
    if any(arg is not None for arg in [num_gpus, num_cpus, memory_bytes]):
        raise ValueError(
            "The num_gpus, num_cpus, and memory_bytes kwargs have been deprecated for @udf. Please use `DataFrame.with_column(..., resource_request=...)` instead"
        )

    func_ret_type = ExpressionType.from_py_type(return_type)

    def udf_decorator(func: UDF) -> Callable:
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):

            call_method = func.__call__ if isinstance(func, type) else func
            input_types = _get_input_types_from_annotation(call_method)
            ordered_func_arg_names = list(inspect.signature(call_method).parameters.keys())

            @functools.wraps(func)
            def pre_process_data_block_func(*args, **kwargs):
                # TODO: The initialization of stateful UDFs is currently done on the execution on every partition here,
                # but should instead be done on a higher level so that state initialization cost can be amortized across partitions.
                # See: https://github.com/Eventual-Inc/Daft/issues/196
                initialized_func = _initialize_func(func)

                # Calculate len of partition, or 0 if all datablocks are scalars
                arg_lengths = [len(arg) if isinstance(arg, DataBlock) else 0 for arg in args]
                kwarg_lengths = [len(kwargs[kwarg]) if isinstance(kwargs[kwarg], DataBlock) else 0 for kwarg in kwargs]
                datablock_lengths = set(arg_lengths + kwarg_lengths)
                datablock_lengths = datablock_lengths - {0}
                assert (
                    len(datablock_lengths) <= 1
                ), "All DataBlocks passed into a UDF must have the same length, or be scalar"
                partition_length = datablock_lengths.pop() if len(datablock_lengths) > 0 else 0

                # Convert DataBlock arguments to the correct type
                converted_args = tuple(
                    _convert_argument(arg, input_types[arg_name], partition_length)
                    for arg_name, arg in zip(ordered_func_arg_names, args)
                )
                converted_kwargs = {
                    kwarg_name: _convert_argument(arg, input_types[kwarg_name], partition_length)
                    for kwarg_name, arg in kwargs.items()
                }

                try:
                    results = initialized_func(*converted_args, **converted_kwargs)
                except:
                    logger.error(f"Encountered error when running user-defined function {func.__name__}")
                    raise

                return results

            out_expr = UdfExpression(
                func=pre_process_data_block_func,
                func_ret_type=func_ret_type,
                func_args=args,
                func_kwargs=kwargs,
            )
            return out_expr

        return wrapped_func

    if f is None:
        return udf_decorator
    return udf_decorator(f)


def polars_udf(*args, **kwargs):
    raise NotImplementedError(
        "Polars_udf is deprecated. Please use @udf instead and decorate your input arguments with `pl.Series`"
    )
