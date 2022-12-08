from __future__ import annotations

import functools
import logging
import warnings
from typing import Callable, Sequence, Union

from daft.execution.operators import ExpressionType
from daft.expressions import UdfExpression
from daft.resource_request import ResourceRequest
from daft.runners.blocks import DataBlock

_POLARS_AVAILABLE = True
try:
    import polars  # noqa: F401
except ImportError:
    _POLARS_AVAILABLE = False

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


def udf(
    f: Callable | None = None,
    *,
    return_type: type,
    num_gpus: int | float | None = None,
    num_cpus: int | float | None = None,
    memory_bytes: int | float | None = None,
) -> Callable:
    """Decorator for creating a UDF

    NOTE: This decorator will be deprecated in favor of @polars_udf in v0.1.0. @polars_udf is much more efficient and handles Null/NaN semantics correctly.

    This decorator wraps a function into a DaFt UDF that can then be used on Dataframes.
    At runtime, DaFt will pass columns of data to the function as equal-length Numpy arrays.

    The possible types of input a UDF can take in are:

    1. f(col("foo")) - A Column expression, which will instruct DaFt to pass the referenced column into the function at runtime as a Numpy array.
    2. f(x) - When `x` is some object that is not a Column, DaFt will pass it into the function with no modifications. Note that this object must be pickleable, and that users should use this to pass light data around. For heavier initializations, use the __init__ method in a stateful UDF.

    For example, a simple UDF that randomly rotates some user-defined Image type could look like:

    >>> @udf(return_type=Image)
    >>> def random_rotations(image_col, rotation_bounds_degrees: int):
    >>>     return [
    >>>         img.rotate(random.uniform(0, 1) * rotation_bounds_degrees)
    >>>         for img in image_col
    >>>     ]
    >>>
    >>> # Usage on a DataFrame:
    >>> df.select(random_rotations(col("images"), rotation_bounds_degrees=90))

    UDFs can also be created on Classes, which allow for initialization on some expensive state that can be shared
    between invocations of the class, for example downloading data or creating a model.

    >>> @udf(return_type=int)
    >>> class RunModel:
    >>>
    >>>     def __init__(self):
    >>>         # Perform expensive initializations
    >>>         self._model = create_model()
    >>>
    >>>     def __call__(self, features_col):
    >>>         return self._model(features_col)

    Args:
        f: Function to wrap as a UDF, accepts column inputs as Numpy arrays and returns a column of data as a Polars Series/Numpy array/Python list/Pandas series.
        return_type: The return type of the UDF
        num_gpus: How many GPUs the UDF requires for execution, used for resource allocation when running in a distributed setting
        num_cpus: How many CPUs the UDF requires for execution, used for resource allocation when running in a distributed setting
        memory_bytes: How many bytes of memory this UDF requires for execution, used for resource allocation when running in a distributed setting
    """
    warnings.warn(
        "DEPRECATION WARNING: @udf will be deprecated in 0.1.0 in favor of @polars_udf which is much more efficient and handles Null/NaN semantics correctly"
    )

    func_ret_type = ExpressionType.from_py_type(return_type)

    def udf_decorator(func: UDF) -> Callable:
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            @functools.wraps(func)
            def prepost_process_data_block_func(*args, **kwargs):
                # TODO: The initialization of stateful UDFs is currently done on the execution on every partition here,
                # but should instead be done on a higher level so that state initialization cost can be amortized across partitions.
                initialized_func = _initialize_func(func)

                converted_args = tuple(arg.to_numpy() if isinstance(arg, DataBlock) else arg for arg in args)
                converted_kwargs = {
                    kw: arg.to_numpy() if isinstance(arg, DataBlock) else arg for kw, arg in kwargs.items()
                }

                try:
                    results = initialized_func(*converted_args, **converted_kwargs)
                except:
                    logger.error(f"Encountered error when running user-defined function {func.__name__}")
                    raise

                return results

            out_expr = UdfExpression(
                func=prepost_process_data_block_func,
                func_ret_type=func_ret_type,
                func_args=args,
                func_kwargs=kwargs,
                resource_request=ResourceRequest(num_cpus=num_cpus, num_gpus=num_gpus, memory_bytes=memory_bytes),
            )
            return out_expr

        return wrapped_func

    if f is None:
        return udf_decorator
    return udf_decorator(f)


def polars_udf(
    f: Callable | None = None,
    *,
    return_type: type,
    num_gpus: int | float | None = None,
    num_cpus: int | float | None = None,
    memory_bytes: int | float | None = None,
) -> Callable:
    """Decorator for creating a Polars UDF

    This decorator wraps a function into a Daft UDF that can then be used on Dataframes.
    At runtime, DaFt will pass columns of data to the function as separate equal-length Polars Series.

    You may call a UDF on two types of inputs:

    1. Non-Expressions: `f(x)` when `x` is not an Expression - Daft will pass `x` into the function with no modifications. Note that this object must be pickleable, and that users should use this to pass light data around. For heavier initializations, use the __init__ method in a stateful UDF.
    2. Expressions: `f(expr)` when `expr` is an Expression - Daft will compute the results of the expression and pass the results into the UDF at runtime as a Polars Series.

    For example, a simple UDF that randomly rotates some user-defined Image type could look like:

    >>> @udf(return_type=Image)
    >>> def random_rotations(image_col, rotation_bounds_degrees: int):
    >>>     return [
    >>>         img.rotate(random.uniform(0, 1) * rotation_bounds_degrees)
    >>>         for img in image_col
    >>>     ]
    >>>
    >>> # Usage on a DataFrame:
    >>> df.select(random_rotations(col("images"), rotation_bounds_degrees=90))

    UDFs can also be created on Classes, which allow for initialization on some expensive state that can be shared
    between invocations of the class, for example downloading data or creating a model.

    >>> @udf(return_type=int)
    >>> class RunModel:
    >>>
    >>>     def __init__(self):
    >>>         # Perform expensive initializations
    >>>         self._model = create_model()
    >>>
    >>>     def __call__(self, features_col):
    >>>         return self._model(features_col)

    Args:
        f: Function to wrap as a UDF, accepts column inputs as Polars Series and returns a column of data as a Polars Series/Numpy array/Python list/Pandas series.
        return_type: The return type of the UDF
        num_gpus: How many GPUs the UDF requires for execution, used for resource allocation when running in a distributed setting
        num_cpus: How many CPUs the UDF requires for execution, used for resource allocation when running in a distributed setting
        memory_bytes: How many bytes of memory this UDF requires for execution, used for resource allocation when running in a distributed setting
    """
    if not _POLARS_AVAILABLE:
        raise ImportError("polars_udf requires polars to be installed")

    func_ret_type = ExpressionType.from_py_type(return_type)

    def udf_decorator(func: UDF) -> Callable:
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            @functools.wraps(func)
            def prepost_process_data_block_func(*args, **kwargs):
                # TODO: The initialization of stateful UDFs is currently done on the execution on every partition here,
                # but should instead be done on a higher level so that state initialization cost can be amortized across partitions.
                initialized_func = _initialize_func(func)

                converted_args = tuple(arg.to_polars() if isinstance(arg, DataBlock) else arg for arg in args)
                converted_kwargs = {
                    kw: arg.to_polars() if isinstance(arg, DataBlock) else arg for kw, arg in kwargs.items()
                }

                try:
                    results = initialized_func(*converted_args, **converted_kwargs)
                except:
                    logger.error(f"Encountered error when running user-defined function {func.__name__}")
                    raise

                return results

            out_expr = UdfExpression(
                func=prepost_process_data_block_func,
                func_ret_type=func_ret_type,
                func_args=args,
                func_kwargs=kwargs,
                resource_request=ResourceRequest(num_cpus=num_cpus, num_gpus=num_gpus, memory_bytes=memory_bytes),
            )
            return out_expr

        return wrapped_func

    if f is None:
        return udf_decorator
    return udf_decorator(f)
