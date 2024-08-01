from __future__ import annotations

import dataclasses
import functools
import inspect
from abc import abstractmethod
from typing import TYPE_CHECKING, Callable, Union

from daft.daft import PyDataType
from daft.datatype import DataType
from daft.expressions import Expression
from daft.series import PySeries, Series

_NUMPY_AVAILABLE = True
try:
    import numpy as np
except ImportError:
    _NUMPY_AVAILABLE = False

_PYARROW_AVAILABLE = True
try:
    import pyarrow as pa
except ImportError:
    _PYARROW_AVAILABLE = False

if TYPE_CHECKING:
    import numpy as np
    import pyarrow as pa

UserProvidedPythonFunction = Callable[..., Union[Series, "np.ndarray", list]]


@dataclasses.dataclass(frozen=True)
class BoundUDFArgs:
    # Arguments that UDF was called with, potentially symbolic (i.e. containing Expressions)
    bound_args: inspect.BoundArguments

    def expressions(self) -> dict[str, Expression]:
        parsed_expressions = {}
        signature = self.bound_args.signature

        for key, val in self.bound_args.arguments.items():
            # If the argument is VAR_POSITIONAL (e.g. `*args`), we parse each
            # entry in the tuple to find any expressions
            if signature.parameters[key].kind == inspect.Parameter.VAR_POSITIONAL:
                for idx, x in enumerate(val):
                    if isinstance(x, Expression):
                        parsed_expressions[f"{key}-{idx}"] = x
            # If the key is VAR_KEYWORD (e.g. `**kwargs`), we parse each entry
            # in the dict to find any expressions
            elif signature.parameters[key].kind == inspect.Parameter.VAR_KEYWORD:
                for kwarg_key, x in val.items():
                    if isinstance(x, Expression):
                        parsed_expressions[kwarg_key] = x
            elif isinstance(val, Expression):
                parsed_expressions[key] = val

        return parsed_expressions

    def arg_keys(self) -> list[str]:
        parsed_arg_keys = []
        signature = self.bound_args.signature
        for key, value in self.bound_args.arguments.items():
            if signature.parameters[key].kind == inspect.Parameter.VAR_POSITIONAL:
                for idx, _ in enumerate(value):
                    parsed_arg_keys.append(f"{key}-{idx}")
            elif key not in self.bound_args.kwargs and signature.parameters[key].kind != inspect.Parameter.VAR_KEYWORD:
                parsed_arg_keys.append(key)

        return parsed_arg_keys

    def __hash__(self) -> int:
        # Make the bound arguments hashable in the basic case when every argument is itself hashable.
        # NOTE: This will fail if any of the arguments are not hashable (e.g. dicts, Python classes that
        # don't implement __hash__).
        return hash(frozenset(self.bound_args.arguments.items()))


def run_udf(
    func: Callable, bound_args: BoundUDFArgs, evaluated_expressions: list[Series], py_return_dtype: PyDataType
) -> PySeries:
    """API to call from Rust code that will call an UDF (initialized, in the case of stateful UDFs) on the inputs"""
    return_dtype = DataType._from_pydatatype(py_return_dtype)
    kwarg_keys = list(bound_args.bound_args.kwargs.keys())
    arg_keys = bound_args.arg_keys()
    pyvalues = {key: val for key, val in bound_args.bound_args.arguments.items() if not isinstance(val, Expression)}
    expressions = bound_args.expressions()
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

    try:
        result = func(*args, **kwargs)
    except Exception as user_function_exception:
        raise RuntimeError(
            f"User-defined function `{func}` failed when executing on inputs with lengths: {tuple(len(series) for series in evaluated_expressions)}"
        ) from user_function_exception

    # HACK: Series have names and the logic for naming fields/series in a UDF is to take the first
    # Expression's name. Note that this logic is tied to the `to_field` implementation of the Rust PythonUDF
    # and is quite error prone! If our Series naming logic here is wrong, things will break when the UDF is run on a table.
    name = evaluated_expressions[0].name()

    # Post-processing of results into a Series of the appropriate dtype
    if isinstance(result, Series):
        return result.rename(name).cast(return_dtype)._series
    elif isinstance(result, list):
        if return_dtype == DataType.python():
            return Series.from_pylist(result, name=name, pyobj="force")._series
        else:
            return Series.from_pylist(result, name=name, pyobj="allow").cast(return_dtype)._series
    elif _NUMPY_AVAILABLE and isinstance(result, np.ndarray):
        return Series.from_numpy(result, name=name).cast(return_dtype)._series
    elif _PYARROW_AVAILABLE and isinstance(result, (pa.Array, pa.ChunkedArray)):
        return Series.from_arrow(result, name=name).cast(return_dtype)._series
    else:
        raise NotImplementedError(f"Return type not supported for UDF: {type(result)}")


class UDF:
    @abstractmethod
    def __call__(self, *args, **kwargs) -> Expression: ...


@dataclasses.dataclass
class PartialStatelessUDF:
    """Partially bound stateless UDF"""

    func: UserProvidedPythonFunction
    return_dtype: DataType
    bound_args: BoundUDFArgs


@dataclasses.dataclass
class PartialStatefulUDF:
    """Partially bound stateful UDF"""

    func_cls: Callable[[], UserProvidedPythonFunction]
    return_dtype: DataType
    bound_args: BoundUDFArgs


@dataclasses.dataclass
class StatelessUDF(UDF):
    func: UserProvidedPythonFunction
    return_dtype: DataType

    def __post_init__(self):
        """Analogous to the @functools.wraps(self.func) pattern

        This will swap out identifiers on `self` to match `self.func`. Most notably, this swaps out
        self.__module__ and self.__qualname__, which is used in `__reduce__` during serialization.
        """
        functools.update_wrapper(self, self.func)

    def __call__(self, *args, **kwargs) -> Expression:
        bound_args = BoundUDFArgs(self.bind_func(*args, **kwargs))
        expressions = list(bound_args.expressions().values())
        return Expression.stateless_udf(
            partial=PartialStatelessUDF(self.func, self.return_dtype, bound_args),
            expressions=expressions,
            return_dtype=self.return_dtype,
        )

    def bind_func(self, *args, **kwargs) -> inspect.BoundArguments:
        sig = inspect.signature(self.func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        return bound_args

    def __hash__(self) -> int:
        return hash((self.func, self.return_dtype))


@dataclasses.dataclass
class StatefulUDF(UDF):
    cls: type
    return_dtype: DataType

    def __post_init__(self):
        """Analogous to the @functools.wraps(self.cls) pattern

        This will swap out identifiers on `self` to match `self.cls`. Most notably, this swaps out
        self.__module__ and self.__qualname__, which is used in `__reduce__` during serialization.
        """
        functools.update_wrapper(self, self.cls)

    def __call__(self, *args, **kwargs) -> Expression:
        bound_args = BoundUDFArgs(self.bind_func(*args, **kwargs))
        expressions = list(bound_args.expressions().values())
        return Expression.stateful_udf(
            partial=PartialStatefulUDF(self.cls, self.return_dtype, bound_args),
            expressions=expressions,
            return_dtype=self.return_dtype,
        )

    def bind_func(self, *args, **kwargs) -> inspect.BoundArguments:
        sig = inspect.signature(self.cls.__call__)
        bound_args = sig.bind(
            # Placeholder for `self`
            None,
            *args,
            **kwargs,
        )
        bound_args.apply_defaults()
        return bound_args

    def __hash__(self) -> int:
        return hash((self.cls, self.return_dtype))


def udf(
    *,
    return_dtype: DataType,
) -> Callable[[UserProvidedPythonFunction | type], UDF]:
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

    def _udf(f: UserProvidedPythonFunction | type) -> UDF:
        if inspect.isclass(f):
            return StatefulUDF(
                cls=f,
                return_dtype=return_dtype,
            )
        else:
            return StatelessUDF(
                func=f,
                return_dtype=return_dtype,
            )

    return _udf
