from __future__ import annotations

import dataclasses
import inspect
from typing import Callable

from daft.datatype import DataType
from daft.expressions import Expression
from daft.series import Series

UserProvidedPythonFunction = Callable[..., Series]


@dataclasses.dataclass(frozen=True)
class PartialUDF:
    func: UserProvidedPythonFunction
    return_dtype: DataType

    # Arguments that UDF was called with, potentially symbolic (i.e. containing Expressions)
    bound_args: inspect.BoundArguments

    def expressions(self) -> dict[str, Expression]:
        return {key: val for key, val in self.bound_args.arguments.items() if isinstance(val, Expression)}

    def __call__(self, evaluated_expressions: list[Series]) -> Series:
        kwarg_keys = list(self.bound_args.kwargs.keys())
        arg_keys = list(self.bound_args.arguments.keys() - self.bound_args.kwargs.keys())
        pyvalues = {key: val for key, val in self.bound_args.arguments.items() if not isinstance(val, Expression)}
        expressions = self.expressions()
        assert len(evaluated_expressions) == len(
            expressions
        ), "Computed series must map 1:1 to the expressions that were evaluated"
        function_parameter_name_to_index = {name: i for i, name in enumerate(expressions)}
        args = tuple(
            pyvalues.get(name, evaluated_expressions[function_parameter_name_to_index[name]]) for name in arg_keys
        )
        kwargs = {
            name: pyvalues.get(name, evaluated_expressions[function_parameter_name_to_index[name]])
            for name in kwarg_keys
        }
        return self.func(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class UDF:
    func: UserProvidedPythonFunction
    return_dtype: DataType

    def __call__(self, *args, **kwargs) -> Expression:
        bound_args = inspect.signature(self.func).bind(*args, **kwargs)
        bound_args.apply_defaults()

        partial_udf = PartialUDF(self.func, self.return_dtype, bound_args)
        expressions = list(partial_udf.expressions().values())
        return Expression.udf(
            func=partial_udf,
            expressions=expressions,
            return_dtype=self._func_ret_type,
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
