from __future__ import annotations

from typing import Callable, Sequence

from daft.datatype import DataType
from daft.expressions import Expression

_PythonFunction = Callable[..., Sequence]


class UDF:
    def __init__(self, f: _PythonFunction, return_dtype: DataType):
        self._f = f
        self._func_ret_type = return_dtype

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
        return Expression.udf(
            func=self._f,
            args=args,
            kwargs=kwargs,
        )


def udf(
    *,
    return_dtype: DataType,
) -> Callable[[_PythonFunction], UDF]:
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
        Callable[[_PythonFunction], UDF]: UDF decorator - converts a user-provided Python function as a UDF that can be called on Expressions
    """

    def _udf(f: _PythonFunction) -> UDF:
        return UDF(
            f,
            return_dtype,
        )

    return _udf
