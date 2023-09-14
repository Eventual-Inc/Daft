from __future__ import annotations

import dataclasses
import functools
import inspect
import types
from typing import Callable

from daft.datatype import DataType
from daft.expressions import Expression
from daft.series import PySeries, Series

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

    def __call__(self, evaluated_expressions: list[Series]) -> PySeries:
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
            # special-case to skip `self` since that would be a redundant argument in a method call to a class-UDF
            if name == "self":
                continue

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

        # NOTE: We currently initialize the function once for every invocation of the PartialUDF.
        # This is not ideal and we should cache initializations across calls for the same process.
        func = self.udf.get_initialized_func()

        result = func(*args, **kwargs)

        # HACK: Series have names and the logic for naming fields/series in a UDF is to take the first
        # Expression's name. Note that this logic is tied to the `to_field` implementation of the Rust PythonUDF
        # and is quite error prone! If our Series naming logic here is wrong, things will break when the UDF is run on a table.
        name = evaluated_expressions[0].name()

        # Post-processing of results into a Series of the appropriate dtype
        if isinstance(result, Series):
            return result.rename(name).cast(self.udf.return_dtype)._series
        elif isinstance(result, list):
            if self.udf.return_dtype == DataType.python():
                return Series.from_pylist(result, name=name, pyobj="force")._series
            else:
                return Series.from_pylist(result, name=name, pyobj="allow").cast(self.udf.return_dtype)._series
        elif _NUMPY_AVAILABLE and isinstance(result, np.ndarray):
            return Series.from_numpy(result, name=name).cast(self.udf.return_dtype)._series
        else:
            raise NotImplementedError(f"Return type not supported for UDF: {type(result)}")

    def __hash__(self) -> int:
        # Make the bound arguments hashable in the basic case when every argument is itself hashable.
        # NOTE: This will fail if any of the arguments are not hashable (e.g. dicts, Python classes that
        # don't implement __hash__). In that case, Daft's Rust-side hasher will fall back to hashing the
        # pickled UDF. See daft-dsl/src/python/partial_udf.rs
        args = frozenset(self.bound_args.arguments.items())
        return hash((self.udf, args))


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

    def __call__(self, *args, **kwargs) -> Expression:
        bound_args = self.bind_func(*args, **kwargs)
        partial_udf = PartialUDF(self, bound_args)
        expressions = list(partial_udf.expressions().values())
        return Expression.udf(
            func=partial_udf,
            expressions=expressions,
            return_dtype=self.return_dtype,
        )

    def bind_func(self, *args, **kwargs):
        if isinstance(self.func, types.FunctionType):
            sig = inspect.signature(self.func)
            bound_args = sig.bind(*args, **kwargs)
        elif isinstance(self.func, type):
            sig = inspect.signature(self.func.__call__)
            bound_args = sig.bind(
                # Placeholder for `self`
                None,
                *args,
                **kwargs,
            )
        else:
            raise NotImplementedError(f"UDF type not supported: {type(self.func)}")
        bound_args.apply_defaults()
        return bound_args

    def get_initialized_func(self):
        if isinstance(self.func, types.FunctionType):
            return self.func
        elif isinstance(self.func, type):
            # NOTE: This potentially runs expensive initializations on the class
            return self.func()
        raise NotImplementedError(f"UDF type not supported: {type(self.func)}")

    def __hash__(self) -> int:
        return hash((self.func, self.return_dtype))


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
    2. Converts the ``x`` Daft Series into a Python list using :meth:`x.to_pylist() <daft.Series.to_pylist>`
    3. Adds a Python constant value ``c`` to every element in ``x``
    4. Returns a new list of Python values which will be coerced to the specified return type: ``return_dtype=DataType.int64()``.
    5. We can call our UDF on a dataframe using any of the dataframe projection operations (:meth:`df.with_column() <daft.DataFrame.with_column>`,
       :meth:`df.select() <daft.DataFrame.select>`, etc.)

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
