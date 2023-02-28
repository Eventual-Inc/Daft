from __future__ import annotations

import enum
import functools
import inspect
import logging
import sys
from typing import Any, Callable, List, Sequence, Union

if sys.version_info < (3, 8):
    from typing_extensions import get_origin
else:
    from typing import get_origin

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

    @classmethod
    def from_type_hint(cls, hint: type) -> UdfInputType:
        if hint == list or hint == List or get_origin(hint) == list or get_origin(hint) == List:
            return UdfInputType.LIST
        elif _NUMPY_AVAILABLE and (hint == np.ndarray or get_origin(hint) == np.ndarray):
            return UdfInputType.NUMPY
        elif _PANDAS_AVAILABLE and hint == pd.Series:
            return UdfInputType.PANDAS
        elif _PYARROW_AVAILABLE and hint == pa.Array:
            return UdfInputType.PYARROW
        elif _PYARROW_AVAILABLE and hint == pa.ChunkedArray:
            return UdfInputType.PYARROW_CHUNKED
        elif _POLARS_AVAILABLE and hint == polars.Series:
            return UdfInputType.POLARS
        raise ValueError(f"UDF input array type {hint} is not supported")


def _convert_argument(arg: Any, input_type: UdfInputType, partition_length: int) -> Any:
    """Converts a UDF argument input to the appropriate user-facing container type"""
    if isinstance(arg, DataBlock) and arg.is_scalar():
        return next(arg.iter_py())
    elif isinstance(arg, DataBlock):
        if input_type == UdfInputType.LIST:
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
    return_dtype: type,
    input_columns: dict[str, type],
    **kwargs,
) -> Callable:
    """Decorator for creating a UDF. This decorator wraps any custom Python code into a function that can be used to process
    columns of data in a Daft DataFrame.

    Each UDF will process a **batch of columnar data**, and output a **batch of columnar data** as well. At runtime, Daft runs
    your UDF on a partition of data at a time, and your UDF will receive input batches of length equal to the partition size.

    .. NOTE::
        UDFs are much slower than native Daft expressions because they run Python code instead of Daft's optimized Rust kernels.
        You should only use UDFs when performing operations that are not supported by Daft's native expressions, or when you
        need to run custom Python code.

        The following example UDF, while a simple example, will be much slower than ``df["x"] + 100`` since it is run as Python
        instead of as a Rust addition kernel using the Expressions API.

    Example:

    >>> @udf(
    >>>     # Annotate the return dtype as an integer
    >>>     return_dtype=int,
    >>>     # Mark the `x` input parameter as a column, and tell Daft to pass it in as a list
    >>>     input_columns={"x": list},
    >>> )
    >>> def add_val(x, val=1):
    >>>    # Your custom Python code here
    >>>    return [x + val for value in x]

    To invoke your UDF, you can use the ``DataFrame.with_column`` method:

    >>> df = DataFrame.from_pydict({"x": [1, 2, 3]})
    >>> df = df.with_column("x_add_100", add_val(df["x"], val=100))

    **UDF Function Outputs**

    The ``return_dtype`` argument specifies what type of column your UDF will return. For user convenience, you may specify a Python
    type such as `str`, `int`, `float`, `bool` and `datetime.date`, which will be converted into a Daft dtype for you.

    Python types that are not recognized as Daft types will be represented as a Daft Python object dtype. For example, if you specify
    ``return_dtype=np.ndarray``, then your returned column will have type ``PY[np.ndarray]``.

    >>> @udf(
    >>>     # Annotate the return dtype as an numpy array
    >>>     return_dtype=np.ndarray,
    >>>     input_columns={"x": list},
    >>> )
    >>> def create_np_zero_arrays(x, dim=128):
    >>>    return [np.zeros((dim,)) for i in range(len(x))]

    Your UDF needs to return a batch of columnar data, and can do so as any one of the following array types:

    1. Numpy Arrays (``np.ndarray``)
    2. Pandas Series (``pd.Series``)
    3. Polars Series (``polars.Series``)
    4. PyArrow Arrays (``pa.Array``) or (``pa.ChunkedArray``)
    5. Python lists (``list`` or ``typing.List``)

    **UDF Function Inputs**

    The ``input_columns`` argument is a dictionary. The keys specify which input parameters of your functions are columns.
    The values specify the array container type that Daft should use when passing data into your function.

    Here's an example where the same column is used multiple times in a UDF, but passed in as different types to illustrate
    how this works!

    >>> @udf(
    >>>     return_dtype=int,
    >>>     input_columns={"x_as_list": list, "x_as_numpy": np.ndarray, "x_as_pandas": pd.Series},
    >>> )
    >>> def example_func(x_as_list, x_as_numpy, x_as_pandas):
    >>>     assert isinstance(x_as_list, list)
    >>>     assert isinstance(x_as_numpy, np.ndarray)
    >>>     assert isinstance(x_as_pandas, pd.Series)
    >>>
    >>> df = df.with_column("foo", example_func(df["x"], df["x"], df["x"]))

    In the above example, when your DataFrame is executed, Daft will pass in batches of column data as equal-length arrays
    into your function. The actual type of those arrays will take on the types indicated by ``input_columns``.

    Input types supported by Daft UDFs and their respective type annotations:

    1. Numpy Arrays (``np.ndarray``)
    2. Pandas Series (``pd.Series``)
    3. Polars Series (``polars.Series``)
    4. PyArrow Arrays (``pa.Array``) or (``pa.ChunkedArray``)
    5. Python lists (``list`` or ``typing.List``)

    .. NOTE::
        Certain array formats have some restrictions around the type of data that they can handle:

        1. **Null Handling**: In Pandas and Numpy, nulls are represented as NaNs for numeric types, and Nones for non-numeric types.
        Additionally, the existence of nulls will trigger a type casting from integer to float arrays. If null handling is important to
        your use-case, we recommend using one of the other available options.

        2. **Python Objects**: PyArrow array formats cannot support object-type columns.

        We recommend using Python lists if performance is not a major consideration, and using the arrow-native formats such as
        PyArrow arrays and Polars series if performance is important.

    **Stateful UDFs**

    UDFs can also be created on Classes, which allow for initialization on some expensive state that can be shared
    between invocations of the class, for example downloading data or creating a model.

    >>> @udf(return_dtype=int, input_columns={"features_col": np.ndarray})
    >>> class RunModel:
    >>>     def __init__(self):
    >>>         # Perform expensive initializations
    >>>         self._model = create_model()
    >>>
    >>>     def __call__(self, features_col):
    >>>         return self._model(features_col)

    Args:
        f: Function to wrap as a UDF, accepts column inputs as Numpy arrays and returns a column of data as a Polars Series/Numpy array/Python list/Pandas series.
        return_dtype: The return dtype of the UDF
        input_columns: Optional dictionary of input parameter names to their types. If provided, this will override type hints provided using the function's type annotations.
    """
    if "num_cpus" in kwargs:
        raise ValueError(
            f"The `num_cpus` keyword argument has been deprecated. Please specify resource requirements when invoking your UDF in `.with_column` instead"
        )
    if "num_gpus" in kwargs:
        raise ValueError(
            f"The `num_gpus` keyword argument has been deprecated. Please specify resource requirements when invoking your UDF in `.with_column` instead"
        )
    if "memory_bytes" in kwargs:
        raise ValueError(
            f"The `memory_bytes` keyword argument has been deprecated. Please specify resource requirements when invoking your UDF in `.with_column` instead"
        )
    if "return_type" in kwargs:
        raise ValueError(f"The `return_type` keyword argument has been deprecated and renamed to return_dtype.")
    if "type_hints" in kwargs:
        raise ValueError(f"The `type_hints` keyword argument has been deprecated and renamed to input_columns.")

    func_ret_type = ExpressionType.from_py_type(return_dtype)

    def udf_decorator(func: UDF) -> Callable:

        call_method = func.__call__ if isinstance(func, type) else func
        input_types = {
            arg_name: UdfInputType.from_type_hint(type_hint) for arg_name, type_hint in input_columns.items()
        }

        # Get function argument names, excluding `self` if it is a class method
        ordered_func_arg_names = list(inspect.signature(call_method).parameters.keys())
        if isinstance(func, type):
            ordered_func_arg_names = ordered_func_arg_names[1:]

        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
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
                    _convert_argument(arg, input_types[arg_name], partition_length) if arg_name in input_types else arg
                    for arg_name, arg in zip(ordered_func_arg_names, args)
                )
                converted_kwargs = {
                    kwarg_name: _convert_argument(arg, input_types[kwarg_name], partition_length)
                    if kwarg_name in input_types
                    else arg
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
        "Polars_udf is deprecated. Please use @udf instead and indicate `pl.Series` in your `input_columns`"
    )
