from __future__ import annotations

import dataclasses
import functools
import inspect
from typing import Any, Callable, Dict, Optional, Tuple, Union

import daft
from daft.daft import PyDataType, ResourceRequest
from daft.datatype import DataType, DataTypeLike
from daft.dependencies import np, pa
from daft.expressions import Expression
from daft.series import PySeries, Series

InitArgsType = Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]]
UdfReturnType = Union[Series, list, "np.ndarray", "pa.Array", "pa.ChunkedArray"]
UserDefinedPyFunc = Callable[..., UdfReturnType]
UserDefinedPyFuncLike = Union[UserDefinedPyFunc, type]


@dataclasses.dataclass(frozen=True)
class UninitializedUdf:
    inner: Callable[..., UserDefinedPyFunc]

    def initialize(self, init_args: InitArgsType) -> UserDefinedPyFunc:
        if init_args is None:
            return self.inner()
        else:
            args, kwargs = init_args
            return self.inner(*args, **kwargs)


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
            elif (
                key not in self.bound_args.kwargs
                and signature.parameters[key].kind != inspect.Parameter.VAR_KEYWORD
            ):
                parsed_arg_keys.append(key)

        return parsed_arg_keys

    def __hash__(self) -> int:
        # Make the bound arguments hashable in the basic case when every argument is itself hashable.
        # NOTE: This will fail if any of the arguments are not hashable (e.g. dicts, Python classes that
        # don't implement __hash__).
        return hash(frozenset(self.bound_args.arguments.items()))


def run_scalar_udf(
    func: UserDefinedPyFunc,
    bound_args: BoundUDFArgs,
    evaluated_expressions: list[Series],
    py_return_dtype: PyDataType,
) -> PySeries:
    """API to call from Rust code that will call an UDF (initialized, in the case of actor pool UDFs) on the inputs."""
    return_dtype = DataType._from_pydatatype(py_return_dtype)
    kwarg_keys = list(bound_args.bound_args.kwargs.keys())
    arg_keys = bound_args.arg_keys()
    pyvalues = {
        key: val
        for key, val in bound_args.bound_args.arguments.items()
        if not isinstance(val, Expression)
    }
    expressions = bound_args.expressions()

    assert len(evaluated_expressions) == len(
        expressions
    ), "Computed series must map 1:1 to the expressions that were evaluated"
    function_parameter_name_to_index = {name: i for i, name in enumerate(expressions)}

    # all inputs must have the same lengths for batching
    # not sure this error can possibly be triggered but it's here
    if len(set(len(s) for s in evaluated_expressions)) != 1:
        raise RuntimeError(
            f"User-defined function `{func}` failed: cannot run in batches when inputs are different lengths: {tuple(len(series) for series in evaluated_expressions)}"
        )

    results = []
    errors = []
    for args in zip(*evaluated_expressions):
        try:
            results.append(func(*args))
            errors.append(None)
        except Exception as user_function_exception:
            results.append(None)
            errors.append(user_function_exception)
    # HACK: Series have names and the logic for naming fields/series in a UDF is to take the first
    # Expression's name. Note that this logic is tied to the `to_field` implementation of the Rust PythonUDF
    # and is quite error prone! If our Series naming logic here is wrong, things will break when the UDF is run on a table.
    name = evaluated_expressions[0].name()

    errors_series = Series.from_pylist(errors, name=name, pyobj="force")._series
    if return_dtype == DataType.python():
        return Series.from_pylist(results, name=name, pyobj="force")._series, errors_series
    else:
        return Series.from_pylist(results, name=name).cast(return_dtype)._series, errors_series


# Marker that helps us differentiate whether a user provided the argument or not
_UnsetMarker: Any = object()


@dataclasses.dataclass
class ScalarUDF:
    """A class produced by applying the `@daft.scalar_udf` decorator over a Python function or class.

    Calling this class produces a `daft.Expression` that can be used in a DataFrame function.

    Examples:
        >>> import daft
        >>> @daft.udf(return_dtype=daft.DataType.float64())
        ... def multiply_and_add(x: daft.Series, y: float, z: float):
        ...     return x.to_arrow().to_numpy() * y + z
        >>>
        >>> df = daft.from_pydict({"x": [1, 2, 3]})
        >>> df = df.with_column("result", multiply_and_add(df["x"], 2.0, z=1.5))
        >>> df.show()
        ╭───────┬─────────╮
        │ x     ┆ result  │
        │ ---   ┆ ---     │
        │ Int64 ┆ Float64 │
        ╞═══════╪═════════╡
        │ 1     ┆ 3.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 2     ┆ 5.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ 7.5     │
        ╰───────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """

    inner: UserDefinedPyFuncLike
    name: str
    return_dtype: DataType
    init_args: InitArgsType = None
    concurrency: int | None = None
    resource_request: ResourceRequest | None = None
    batch_size: int | None = None

    def __post_init__(self):
        # Analogous to the @functools.wraps(self.inner) pattern
        # This will swap out identifiers on `self` to match `self.inner`. Most notably, this swaps out
        # self.__module__ and self.__qualname__, which is used in `__reduce__` during serialization.
        functools.update_wrapper(self, self.inner)

        # construct the UninitializedUdf here so that the constructed expressions can maintain equality
        if isinstance(self.inner, type):
            self.wrapped_inner = UninitializedUdf(self.inner)
        else:
            self.wrapped_inner = UninitializedUdf(lambda: self.inner)

    def __call__(self, *args, **kwargs) -> Expression:
        self._validate_init_args()

        bound_args = self._bind_args(*args, **kwargs)
        expressions = list(bound_args.expressions().values())

        return Expression.udf(
            name=self.name,
            inner=self.wrapped_inner,
            bound_args=bound_args,
            expressions=expressions,
            return_dtype=self.return_dtype,
            init_args=self.init_args,
            scalar_udf=True,
            resource_request=self.resource_request,
            batch_size=self.batch_size,
            concurrency=self.concurrency,
        )

    def override_options(
        self,
        *,
        num_cpus: float | None = _UnsetMarker,
        num_gpus: float | None = _UnsetMarker,
        memory_bytes: int | None = _UnsetMarker,
    ) -> ScalarUDF:
        """Replace the resource requests for running each instance of your UDF.

        Args:
            num_cpus: Number of CPUs to allocate each running instance of your UDF. Note that this is purely used for placement (e.g. if your
                machine has 8 CPUs and you specify num_cpus=4, then Daft can run at most 2 instances of your UDF at a time).
            num_gpus: Number of GPUs to allocate each running instance of your UDF. This is used for placement and also for allocating
                the appropriate GPU to each UDF using `CUDA_VISIBLE_DEVICES`.
            memory_bytes: Amount of memory to allocate each running instance of your UDF in bytes. If your UDF is experiencing out-of-memory errors,
                this parameter can help hint Daft that each UDF requires a certain amount of heap memory for execution.

        Examples:
            For instance, if your UDF requires 4 CPUs to run, you can configure it like so:

            >>> import daft
            >>>
            >>> @daft.udf(return_dtype=daft.DataType.string())
            ... def example_udf(inputs):
            ...     # You will have access to 4 CPUs here if you configure your UDF correctly!
            ...     return inputs
            >>>
            >>> # Parametrize the UDF to run with 4 CPUs
            >>> example_udf_4CPU = example_udf.override_options(num_cpus=4)

        """
        new_resource_request = (
            ResourceRequest()
            if self.resource_request is None
            else self.resource_request
        )
        if num_cpus is not _UnsetMarker:
            new_resource_request = new_resource_request.with_num_cpus(num_cpus)
        if num_gpus is not _UnsetMarker:
            new_resource_request = new_resource_request.with_num_gpus(num_gpus)
        if memory_bytes is not _UnsetMarker:
            new_resource_request = new_resource_request.with_memory_bytes(memory_bytes)

        return dataclasses.replace(self, resource_request=new_resource_request)

    def _validate_init_args(self):
        if isinstance(self.inner, type):
            init_sig = inspect.signature(self.inner.__init__)  # type: ignore
            if (
                any(
                    param.default is param.empty
                    for param in init_sig.parameters.values()
                    if param.name != "self"
                )
                and self.init_args is None
            ):
                raise ValueError(
                    "Cannot call class UDF without initialization arguments. Please either specify default arguments in your __init__ or provide "
                    "initialization arguments using `.with_init_args(...)`."
                )
        else:
            if self.init_args is not None:
                raise ValueError("Function UDFs cannot have init args.")

    def _bind_args(self, *args, **kwargs) -> BoundUDFArgs:
        if isinstance(self.inner, type):
            sig = inspect.signature(self.inner.__call__)
            bound_args = sig.bind(
                # Placeholder for `self`
                None,
                *args,
                **kwargs,
            )
        else:
            sig = inspect.signature(self.inner)
            bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        return BoundUDFArgs(bound_args)

    def with_concurrency(self, concurrency: int) -> ScalarUDF:
        """Override the concurrency of this UDF, which tells Daft how many instances of your UDF to run concurrently.

        Examples:
            >>> import daft
            >>>
            >>> @daft.udf(return_dtype=daft.DataType.string(), num_gpus=1)
            ... class MyGpuUdf:
            ...     def __init__(self, text=" world"):
            ...         self.text = text
            ...
            ...     def __call__(self, data):
            ...         return [x + self.text for x in data]
            >>>
            >>> # New UDF that will have 8 concurrent running instances (will require 8 total GPUs)
            >>> MyGpuUdf_8_concurrency = MyGpuUdf.with_concurrency(8)
        """
        return dataclasses.replace(self, concurrency=concurrency)

    def with_init_args(self, *args, **kwargs) -> ScalarUDF:
        """Replace initialization arguments for a class UDF when calling `__init__` at runtime on each instance of the UDF.

        Examples:
            >>> import daft
            >>>
            >>> @daft.udf(return_dtype=daft.DataType.string())
            ... class MyUdfWithInit:
            ...     def __init__(self, text=" world"):
            ...         self.text = text
            ...
            ...     def __call__(self, data):
            ...         return [x + self.text for x in data]
            >>>
            >>> # Create a customized version of MyUdfWithInit by overriding the init args
            >>> MyUdfWithInit_CustomInitArgs = MyUdfWithInit.with_init_args(text=" my old friend")
            >>>
            >>> df = daft.from_pydict({"foo": ["hello", "hello", "hello"]})
            >>> df = df.with_column("bar_world", MyUdfWithInit(df["foo"]))
            >>> df = df.with_column("bar_custom", MyUdfWithInit_CustomInitArgs(df["foo"]))
            >>> df.show()
            ╭───────┬─────────────┬─────────────────────╮
            │ foo   ┆ bar_world   ┆ bar_custom          │
            │ ---   ┆ ---         ┆ ---                 │
            │ Utf8  ┆ Utf8        ┆ Utf8                │
            ╞═══════╪═════════════╪═════════════════════╡
            │ hello ┆ hello world ┆ hello my old friend │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ hello ┆ hello world ┆ hello my old friend │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ hello ┆ hello world ┆ hello my old friend │
            ╰───────┴─────────────┴─────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        if not isinstance(self.inner, type):
            raise ValueError("Function UDFs cannot have init args.")

        init_sig = inspect.signature(self.inner.__init__)  # type: ignore
        init_sig.bind(
            # Placeholder for `self`
            None,
            *args,
            **kwargs,
        )
        return dataclasses.replace(self, init_args=(args, kwargs))

    def __hash__(self) -> int:
        return hash((self.inner, self.return_dtype))


def scalar_udf(
    *,
    return_dtype: DataType,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    memory_bytes: int | None = None,
    concurrency: int | None = None,
) -> Callable[[UserDefinedPyFuncLike], ScalarUDF]:
    batch_size = 1
    inferred_return_dtype = DataType._infer_type(return_dtype)

    def _udf(f: UserDefinedPyFuncLike) -> ScalarUDF:
        # Grab a name for the UDF. It **should** be unique.
        module_name = getattr(f, "__module__", "")  # type: ignore[call-overload]
        qual_name = getattr(f, "__qualname__")  # type: ignore[call-overload]

        if module_name:
            name = f"{module_name}.{qual_name}"
        else:
            name = qual_name

        resource_request = (
            None
            if num_cpus is None and num_gpus is None and memory_bytes is None
            else ResourceRequest(
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                memory_bytes=memory_bytes,
            )
        )
        udf = ScalarUDF(
            inner=f,
            name=name,
            return_dtype=inferred_return_dtype,
            resource_request=resource_request,
            batch_size=batch_size,
            concurrency=concurrency,
        )

        daft.attach_function(udf)
        return udf

    return _udf
