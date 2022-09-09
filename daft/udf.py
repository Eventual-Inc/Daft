import functools
import logging
from typing import Callable, Optional, Sequence, Type, Union

from daft.execution.operators import ExpressionType
from daft.expressions import UdfExpression
from daft.resource_request import ResourceRequest
from daft.runners.blocks import DataBlock

StatefulUDF = type  # stateful UDFs are provided as Python Classes
StatelessUDF = Callable[..., Sequence]
UDF = Union[StatefulUDF, StatelessUDF]


logger = logging.getLogger(__name__)


def udf(
    f: Optional[Callable] = None,
    *,
    return_type: Type,
    num_gpus: Optional[int] = None,
    num_cpus: Optional[int] = None,
) -> Callable:
    """Decorator for creating a UDF

    This decorator wraps a function into a DaFt UDF that can then be used on Dataframes.
    At runtime, DaFt will pass columns of data to the function as equal-length Numpy arrays.

    The possible types of input a UDF can take in are:

    1. `f(col("foo"))` - A Column expression, which will instruct DaFt to pass the referenced column into the
        function at runtime as a Numpy array.
    2. `f(x)` - When `x` is some object that is not a Column, DaFt will pass it into the function with no modifications.
        Note that this object must be pickleable, and that users should use this to pass light data around. For heavier
        initializations, use the __init__ method in a stateful UDF.

    For example, a simple UDF that adds a number to an integer column looks like:

    >>> @udf(return_type=int)
    >>> def add_to_col(int_col, num=1):
    >>>     return int_col + num
    >>>
    >>> # Usage on a DataFrame:
    >>> df.select(add_to_col(col("foo"), num=10))

    UDFs can also be created on Classes, which allow for initialization on some expensive state that can be shared
    between invocations of the class, for example downloading data or creating a model.

    >>> @udf(return_type=int)
    >>> class RunModel:
    >>>     def __init__(self):
    >>>         self._model = create_model()
    >>>     def __call__(self, features_col):
    >>>         return self._model(features_col)
    """
    func_ret_type = ExpressionType.from_py_type(return_type)

    def udf_decorator(func: UDF) -> Callable:
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            @functools.wraps(func)
            def prepost_process_data_block_func(*args, **kwargs):
                # TODO: The initialization of stateful UDFs is currently done on the execution on every partition here,
                # but should instead be done on a higher level so that state initialization cost can be amortized across partitions.
                try:
                    initialized_func = func() if isinstance(func, type) else func
                except:
                    logger.error(f"Encountered error when initializing user-defined function {func.__name__}")
                    raise

                converted_args = tuple(arg.to_numpy() if isinstance(arg, DataBlock) else arg for arg in args)
                converted_kwargs = {
                    kw: arg.to_numpy() if isinstance(arg, DataBlock) else arg for kw, arg in kwargs.items()
                }

                try:
                    results = initialized_func(*converted_args, **converted_kwargs)
                except:
                    logger.error(f"Encountered error when running user-defined function {func.__name__}")
                    raise
                return DataBlock.make_block(results)

            out_expr = UdfExpression(
                func=prepost_process_data_block_func,
                func_ret_type=func_ret_type,
                func_args=args,
                func_kwargs=kwargs,
                resource_request=ResourceRequest(num_cpus=num_cpus, num_gpus=num_gpus),
            )
            return out_expr

        return wrapped_func

    if f is None:
        return udf_decorator
    return udf_decorator(f)
