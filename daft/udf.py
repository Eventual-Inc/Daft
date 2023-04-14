from __future__ import annotations

import dataclasses
import functools
import inspect
import sys
from typing import Callable

from daft.datatype import DataType
from daft.expressions import Expression
from daft.series import Series

_NUMPY_AVAILABLE = True
try:
    import numpy as np
except ImportError:
    _NUMPY_AVAILABLE = False

UserProvidedPythonFunction = Callable[..., Series]


@dataclasses.dataclass(frozen=True)
class PartialUDF:
    udf: UDF

    # Arguments that UDF was called with, potentially symbolic (i.e. containing Expressions)
    bound_args: inspect.BoundArguments

    def expressions(self) -> dict[str, Expression]:
        return {key: val for key, val in self.bound_args.arguments.items() if isinstance(val, Expression)}

    def __call__(self, evaluated_expressions: list[Series]) -> Series:
        kwarg_keys = list(self.bound_args.kwargs.keys())
        arg_keys = [k for k in self.bound_args.arguments.keys() if k not in self.bound_args.kwargs.keys()]
        pyvalues = {key: val for key, val in self.bound_args.arguments.items() if not isinstance(val, Expression)}
        expressions = self.expressions()
        assert len(evaluated_expressions) == len(
            expressions
        ), "Computed series must map 1:1 to the expressions that were evaluated"
        function_parameter_name_to_index = {name: i for i, name in enumerate(expressions)}

        args = []
        for name in arg_keys:
            assert name in pyvalues or name in function_parameter_name_to_index
            if name in pyvalues:
                args.append(pyvalues[name])
            else:
                args.append(evaluated_expressions[function_parameter_name_to_index[name]])

        kwargs = {}
        for name in kwarg_keys:
            assert name in pyvalues or name in function_parameter_name_to_index
            if name in pyvalues:
                kwargs[name] = pyvalues[name]
            else:
                kwargs[name] = evaluated_expressions[function_parameter_name_to_index[name]]

        result = self.udf.func(*args, **kwargs)

        # HACK: Series have names and the logic for naming fields/series in a UDF is to take the first
        # Expression's name. Note that this logic is tied to the `to_field` implementation of the Rust PythonUDF
        # and is quite error prone! If our Series naming logic here is wrong, things will break when the UDF is run on a table.
        name = evaluated_expressions[0].name()

        # Post-processing of results into a Series of the appropriate dtype
        if isinstance(result, Series):
            return result.rename(name).cast(self.udf.return_dtype)._series
        elif isinstance(result, list):
            return Series.from_pylist(result, name=name).cast(self.udf.return_dtype)._series
        elif _NUMPY_AVAILABLE and isinstance(result, np.ndarray):
            return Series.from_numpy(result, name=name).cast(self.udf.return_dtype)._series
        else:
            raise NotImplementedError(f"Return type not supported for UDF: {type(result)}")


@dataclasses.dataclass
class UDF:
    func: UserProvidedPythonFunction
    return_dtype: DataType

    def __post_init__(self):
        """Analagous to the @functools.wraps(self.func) pattern

        This will swap out identifiers on `self` to match `self.func`. Most notably, this swaps out
        self.__module__ and self.__qualname__, which is used in `__reduce__` during serialization.
        """
        functools.update_wrapper(self, self.func)

    def __reduce__(self):
        """Overrides default pickling behavior"""
        global_var = getattr(sys.modules[self.__module__], self.__qualname__)

        # CASE 1: User function was overridden by the UDF
        #
        # This is the case when a user:
        #   1. Uses the @udf decorator, thus shadowing the original function with a UDF object
        #   2. Manually shadows the variable with a statement like: `f = udf(...)(f)`
        #
        # Here, self.func is NOT pickleable because it cannot be accessed by its global name any longer.
        # However, pickling of the UDF is simple because we now have a globally retrievable path.
        if isinstance(global_var, UDF):
            return self.__qualname__

        # CASE 2: User function was not overridden by UDF
        #
        # This is the case if a user wraps their function into a different variable: `f2 = udf(...)(f1)`
        #
        # Here self.__module__ and self.__qualname__ does NOT point to a UDF. It still points to the
        # user's function and we can pickle the UDF as per how normal objects are pickled.
        return super().__reduce__()

    def __call__(self, *args, **kwargs) -> Expression:
        bound_args = inspect.signature(self.func).bind(*args, **kwargs)
        bound_args.apply_defaults()

        partial_udf = PartialUDF(self, bound_args)
        expressions = list(partial_udf.expressions().values())
        return Expression.udf(
            func=partial_udf,
            expressions=expressions,
            return_dtype=self.return_dtype,
        )


def udf(
    *,
    return_dtype: DataType,
) -> Callable[[UserProvidedPythonFunction], UDF]:
    """Decorator to convert a Python function into a UDF

    UDFs allow users to run arbitrary Python code on the outputs of Expressions.

    .. NOTE::
        In most cases, UDFs will be slower than a native kernel/expression because of the required Rust and Python overheads. If
        your computation can be expressed using Daft expressions, you should do so instead of writing a UDF. If your UDF expresses a
        common use-case that isn't already covered by Daft, you should file a ticket or contribute this functionality back to Daft
        as a kernel!

    In the example below, we create a UDF that:

    1. Receives data under the argument name ``x``
    2. Converts the ``x`` Daft Series into a Python list using ``x.to_pylist()``
    3. Adds a Python constant value ``c`` to every element in ``x``
    3. Returns a new list of Python values which will be coerced to the specified return type: ``return_dtype=DataType.int64()``.
    4. We can call our UDF on a dataframe using any of the dataframe projection operations (``with_column``, ``select`` etc)

    Example:
        >>> @udf(return_dtype=DataType.int64())
        >>> def add_constant(x: Series, c=10):
        >>>     return [v + c for v in x.to_pylist()]
        >>>
        >>> df = df.with_column("new_x", add_constant(df["x"], c=20))

    Args:
        return_dtype (DataType): Returned type of the UDF

    Returns:
        Callable[[UserProvidedPythonFunction], UDF]: UDF decorator - converts a user-provided Python function as a UDF that can be called on Expressions
    """

    def _udf(f: UserProvidedPythonFunction) -> UDF:
        return UDF(
            func=f,
            return_dtype=return_dtype,
        )

    return _udf
