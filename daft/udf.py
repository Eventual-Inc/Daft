from __future__ import annotations

import dataclasses
import functools
import inspect
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Union

from daft.daft import PyDataType, ResourceRequest
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


# Marker that helps us differentiate whether a user provided the argument or not
_UnsetMarker: Any = object()


@dataclasses.dataclass
class UDF:
    resource_request: ResourceRequest | None

    @abstractmethod
    def __call__(self, *args, **kwargs) -> Expression: ...

    def override_options(
        self,
        *,
        num_cpus: float | None = _UnsetMarker,
        num_gpus: float | None = _UnsetMarker,
        memory_bytes: int | None = _UnsetMarker,
    ) -> UDF:
        """Replace the resource requests for running each instance of your stateless UDF.

        For instance, if your stateless UDF requires 4 CPUs to run, you can configure it like so:

        >>> import daft
        >>>
        >>> @daft.udf(return_dtype=daft.DataType.string())
        ... def example_stateless_udf(inputs):
        ...     # You will have access to 4 CPUs here if you configure your UDF correctly!
        ...     return inputs
        >>>
        >>> # Parametrize the UDF to run with 4 CPUs
        >>> example_stateless_udf_4CPU = example_stateless_udf.override_options(num_cpus=4)
        >>>
        >>> df = daft.from_pydict({"foo": [1, 2, 3]})
        >>> df = df.with_column("bar", example_stateless_udf_4CPU(df["foo"]))

        Args:
            num_cpus: Number of CPUs to allocate each running instance of your UDF. Note that this is purely used for placement (e.g. if your
                machine has 8 CPUs and you specify num_cpus=4, then Daft can run at most 2 instances of your UDF at a time).
            num_gpus: Number of GPUs to allocate each running instance of your UDF. This is used for placement and also for allocating
                the appropriate GPU to each UDF using `CUDA_VISIBLE_DEVICES`.
            memory_bytes: Amount of memory to allocate each running instance of your UDF in bytes. If your UDF is experiencing out-of-memory errors,
                this parameter can help hint Daft that each UDF requires a certain amount of heap memory for execution.
        """
        result = self

        # Any changes to resource request
        if not all((num_cpus is _UnsetMarker, num_gpus is _UnsetMarker, memory_bytes is _UnsetMarker)):
            new_resource_request = ResourceRequest() if self.resource_request is None else self.resource_request
            if num_cpus is not _UnsetMarker:
                new_resource_request = new_resource_request.with_num_cpus(num_cpus)
            if num_gpus is not _UnsetMarker:
                new_resource_request = new_resource_request.with_num_gpus(num_gpus)
            if memory_bytes is not _UnsetMarker:
                new_resource_request = new_resource_request.with_memory_bytes(memory_bytes)
            result = dataclasses.replace(result, resource_request=new_resource_request)

        return result


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
    name: str
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
            name=self.name,
            partial=PartialStatelessUDF(self.func, self.return_dtype, bound_args),
            expressions=expressions,
            return_dtype=self.return_dtype,
            resource_request=self.resource_request,
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
    name: str
    cls: type
    return_dtype: DataType
    init_args: tuple[tuple[Any, ...], dict[str, Any]] | None = None

    def __post_init__(self):
        """Analogous to the @functools.wraps(self.cls) pattern

        This will swap out identifiers on `self` to match `self.cls`. Most notably, this swaps out
        self.__module__ and self.__qualname__, which is used in `__reduce__` during serialization.
        """
        functools.update_wrapper(self, self.cls)

    def __call__(self, *args, **kwargs) -> Expression:
        # Validate that initialization arguments are provided if the __init__ signature indicates that there are
        # parameters without defaults
        init_sig = inspect.signature(self.cls.__init__)  # type: ignore
        if any(param.default is param.empty for param in init_sig.parameters.values()) and self.init_args is None:
            raise ValueError(
                "Cannot call StatefulUDF without initialization arguments. Please either specify default arguments in your __init__ or provide "
                "initialization arguments using `.with_init_args(...)`."
            )

        bound_args = BoundUDFArgs(self.bind_func(*args, **kwargs))
        expressions = list(bound_args.expressions().values())
        return Expression.stateful_udf(
            name=self.name,
            partial=PartialStatefulUDF(self.cls, self.return_dtype, bound_args),
            expressions=expressions,
            return_dtype=self.return_dtype,
            resource_request=self.resource_request,
            init_args=self.init_args,
        )

    def with_init_args(self, *args, **kwargs) -> StatefulUDF:
        """Replace initialization arguments for the UDF when calling __init__ at runtime
        on each instance of the UDF.
        """
        return dataclasses.replace(self, init_args=(args, kwargs))

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
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    memory_bytes: int | None = None,
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
        >>> import daft
        >>> @daft.udf(return_dtype=daft.DataType.int64())
        ... def add_constant(x: daft.Series, c=10):
        ...     return [v + c for v in x.to_pylist()]
        >>>
        >>> df = daft.from_pydict({"x": [1, 2, 3]})
        >>> df = df.with_column("new_x", add_constant(df["x"], c=20))
        >>> df.show()
        ╭───────┬───────╮
        │ x     ┆ new_x │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int64 │
        ╞═══════╪═══════╡
        │ 1     ┆ 21    │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 22    │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 23    │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    Resource Requests
    -----------------

    You can also hint Daft about the resources that your UDF will require to run. For example, the following UDF requires 4 CPUs to run. On a
    machine/cluster with 8 CPUs, Daft will be able to run up to 2 instances of this UDF at once, giving you a concurrency of 2!

    >>> import daft
    >>> @daft.udf(return_dtype=daft.DataType.int64(), num_cpus=2)
    ... def udf_needs_2_cpus(x: daft.Series):
    ...     return x
    >>>
    >>> df = daft.from_pydict({"x": [1, 2, 3]})
    >>> df = df.with_column("new_x", udf_needs_2_cpus(df["x"]))
    >>> df.show()
    ╭───────┬───────╮
    │ x     ┆ new_x │
    │ ---   ┆ ---   │
    │ Int64 ┆ Int64 │
    ╞═══════╪═══════╡
    │ 1     ┆ 1     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2     ┆ 2     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 3     ┆ 3     │
    ╰───────┴───────╯
    <BLANKLINE>
    (Showing first 3 of 3 rows)

    Your UDFs' resources can also be overridden before you call it like so:

    >>> import daft
    >>> @daft.udf(return_dtype=daft.DataType.int64(), num_cpus=4)
    ... def udf_needs_4_cpus(x: daft.Series):
    ...     return x
    >>>
    >>> # Override the num_cpus to 2 instead
    >>> udf_needs_8_cpus = udf_needs_4_cpus.override_options(num_cpus=2)
    >>>
    >>> df = daft.from_pydict({"x": [1, 2, 3]})
    >>> df = df.with_column("new_x", udf_needs_2_cpus(df["x"]))
    >>> df.show()
    ╭───────┬───────╮
    │ x     ┆ new_x │
    │ ---   ┆ ---   │
    │ Int64 ┆ Int64 │
    ╞═══════╪═══════╡
    │ 1     ┆ 1     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2     ┆ 2     │
    ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 3     ┆ 3     │
    ╰───────┴───────╯
    <BLANKLINE>
    (Showing first 3 of 3 rows)

    Args:
        return_dtype (DataType): Returned type of the UDF
        num_cpus: Number of CPUs to allocate each running instance of your UDF. Note that this is purely used for placement (e.g. if your
            machine has 8 CPUs and you specify num_cpus=4, then Daft can run at most 2 instances of your UDF at a time). The default `None`
            indicates that Daft is free to allocate as many instances of the UDF as it wants to.
        num_gpus: Number of GPUs to allocate each running instance of your UDF. This is used for placement and also for allocating
            the appropriate GPU to each UDF using `CUDA_VISIBLE_DEVICES`.
        memory_bytes: Amount of memory to allocate each running instance of your UDF in bytes. If your UDF is experiencing out-of-memory errors,
            this parameter can help hint Daft that each UDF requires a certain amount of heap memory for execution.

    Returns:
        Callable[[UserProvidedPythonFunction], UDF]: UDF decorator - converts a user-provided Python function as a UDF that can be called on Expressions
    """

    def _udf(f: UserProvidedPythonFunction | type) -> UDF:
        # Grab a name for the UDF. It **should** be unique.
        name = getattr(f, "__module__", "")  # type: ignore[call-overload]
        if name:
            name = name + "."
        name = name + getattr(f, "__qualname__")  # type: ignore[call-overload]

        resource_request = (
            None
            if num_cpus is None and num_gpus is None and memory_bytes is None
            else ResourceRequest(
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                memory_bytes=memory_bytes,
            )
        )

        if inspect.isclass(f):
            return StatefulUDF(
                name=name,
                cls=f,
                return_dtype=return_dtype,
                resource_request=resource_request,
            )
        else:
            return StatelessUDF(
                name=name,
                func=f,
                return_dtype=return_dtype,
                resource_request=resource_request,
            )

    return _udf
