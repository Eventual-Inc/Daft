from __future__ import annotations

import dataclasses
import functools
import inspect
import warnings
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeAlias, Union, cast

import daft
from daft.daft import PyDataType, PySeries, ResourceRequest
from daft.datatype import DataType, DataTypeLike
from daft.dependencies import np, pa
from daft.errors import UDFException
from daft.expressions import Expression
from daft.series import Series

from .udf_v2 import check_serializable

if TYPE_CHECKING:
    from collections.abc import Generator


InitArgsType: TypeAlias = Optional[tuple[tuple[Any, ...], dict[str, Any]]]
UdfReturnType: TypeAlias = Union[Series, list[Any], "np.ndarray[Any, Any]", "pa.Array", "pa.ChunkedArray"]
UserDefinedPyFunc: TypeAlias = Callable[..., UdfReturnType]
UserDefinedPyFuncLike: TypeAlias = Union[UserDefinedPyFunc, type]


@dataclasses.dataclass(frozen=True)
class UninitializedUdf:
    inner: Callable[..., UserDefinedPyFunc]
    name: str

    def initialize(self, init_args: InitArgsType) -> UserDefinedPyFunc:
        try:
            if init_args is None:
                return self.inner()
            else:
                args, kwargs = init_args
                return self.inner(*args, **kwargs)
        except Exception as init_exc:
            error_note = f"User-defined function `{self.name}` failed to initialize"
            raise UDFException(error_note) from init_exc


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


# Assumes there is at least one evaluated expression
def run_udf(
    func: UserDefinedPyFunc,
    bound_args: BoundUDFArgs,
    input_series_list: list[Series],
    py_return_dtype: PyDataType,
    batch_size: int | None,
) -> PySeries:
    """API to call from Rust code that will call an UDF (initialized, in the case of actor pool UDFs) on the inputs."""
    return_dtype = DataType._from_pydatatype(py_return_dtype)
    kwarg_keys = list(bound_args.bound_args.kwargs.keys())
    arg_keys = bound_args.arg_keys()

    # Arguments to the UDF that are not expressions
    py_args = {key: val for key, val in bound_args.bound_args.arguments.items() if not isinstance(val, Expression)}
    # Arguments to the UDF that are expressions
    expression_args = bound_args.expressions()

    assert len(input_series_list) == len(expression_args), "Input series must map 1:1 to the input expressions"

    # Map from the name of the expression to the order of the argument in the function signature
    function_parameter_name_to_arg_order = {name: i for i, name in enumerate(expression_args)}

    input_series_length = len(input_series_list[0])
    assert all(
        len(input_series) == input_series_length for input_series in input_series_list
    ), "All input series must be of the same length"

    # For each call to the UDF, get the arguments to pass to the UDF in the order that they should be passed
    def get_args_for_slice(start: int, end: int) -> tuple[list[Series | Any], dict[str, Any]]:
        needs_slice = start > 0 or end < input_series_length

        # Extract an argument by name
        def extract_argument(name: str) -> Series | Any:
            assert name in py_args or name in function_parameter_name_to_arg_order

            # If the param is not an expression, we can just pass it directly
            if name in py_args:
                return py_args[name]
            # If the param is an expression, we get the Series from the input_series_list and slice it if necessary
            else:
                order_of_argument = function_parameter_name_to_arg_order[name]
                series = input_series_list[order_of_argument]
                if needs_slice:
                    series = series.slice(start, end)
                return series

        # Extract positional arguments, ignoring `self`
        args = [extract_argument(name) for name in arg_keys if name != "self"]

        # Extract keyword arguments
        kwargs = {name: extract_argument(name) for name in kwarg_keys}

        return args, kwargs

    def make_batches(batch_size: int | None) -> Generator[tuple[int, int], None, None]:
        if batch_size is None or input_series_length <= batch_size:
            yield 0, input_series_length
        else:
            for i in range(0, input_series_length, batch_size):
                cur_batch_size = min(batch_size, input_series_length - i)
                yield i, i + cur_batch_size

    results = []
    for start, end in make_batches(batch_size):
        args, kwargs = get_args_for_slice(start, end)
        try:
            results.append(func(*args, **kwargs))
        except Exception as user_function_exception:
            # Remove the call-site `results.append(...)` from the traceback
            tb = user_function_exception.__traceback__
            user_function_exception = user_function_exception.with_traceback(tb.tb_next if tb else None)
            series_info = [
                f"{series.name()} ({series.datatype()}, length={input_series_length})" for series in input_series_list
            ]
            error_note = f"User-defined function `{func}` failed when executing on inputs:\n" + "\n".join(
                f"  - {info}" for info in series_info
            )
            raise UDFException(error_note) from user_function_exception

    # HACK: Series have names and the logic for naming fields/series in a UDF is to take the first
    # Expression's name. Note that this logic is tied to the `to_field` implementation of the Rust PythonUDF
    # and is quite error prone! If our Series naming logic here is wrong, things will break when the UDF is run on a table.
    name = input_series_list[0].name()

    # Post-processing of results into a Series of the appropriate dtype
    if isinstance(results[0], Series):
        result_series = Series.concat(results)  # type: ignore
        return result_series.rename(name).cast(return_dtype)._series
    elif isinstance(results[0], list):
        result_list = [x for res in results for x in res]
        return Series.from_pylist(result_list, name=name, dtype=return_dtype)._series
    elif np.module_available() and isinstance(results[0], np.ndarray):  # type: ignore[attr-defined]
        np_results = cast("list[np.ndarray[Any, Any]]", results)
        result_np = np.concatenate(np_results)
        return Series.from_numpy(result_np, name=name, dtype=return_dtype)._series
    elif pa.module_available() and isinstance(results[0], (pa.Array, pa.ChunkedArray)):
        return Series.from_arrow(_safe_concat_arrays(results), name=name, dtype=return_dtype)._series
    else:
        raise NotImplementedError(
            f"Return type {type(results[0])} not supported for UDF {func}, expected daft.Series, list, np.ndarray, or pa.Array containing {return_dtype}"
        )


def _safe_concat_arrays(pa_arrays: list[pa.Array | pa.ChunkedArray]) -> pa.ChunkedArray:
    if len(pa_arrays) == 0:
        return pa.concat_arrays([])

    data_type = pa_arrays[0].type
    chunks = []
    is_chunked = False

    for arr in pa_arrays:
        if isinstance(arr, pa.Array):
            chunks.append(arr)
        elif isinstance(arr, pa.ChunkedArray):
            is_chunked = True
            chunks.extend(arr.chunks)
        else:
            raise TypeError(f"Unsupported type: {type(arr)}")

    if is_chunked:
        return pa.chunked_array(chunks, type=data_type)
    else:
        return pa.concat_arrays(chunks)


# Marker that helps us differentiate whether a user provided the argument or not
_UnsetMarker: Any = object()


@dataclasses.dataclass
class UDF:
    """A class produced by applying the `@daft.udf` decorator over a Python function or class.

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
    use_process: bool | None = None
    ray_options: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        # Analogous to the @functools.wraps(self.inner) pattern
        # This will swap out identifiers on `self` to match `self.inner`. Most notably, this swaps out
        # self.__module__ and self.__qualname__, which is used in `__reduce__` during serialization.
        functools.update_wrapper(self, self.inner)

        # construct the UninitializedUdf here so that the constructed expressions can maintain equality
        if isinstance(self.inner, type):
            self.wrapped_inner = UninitializedUdf(self.inner, self.name)
        else:
            self.wrapped_inner = UninitializedUdf(lambda: self.inner, self.name)

    def __call__(self, *args: Any, **kwargs: Any) -> Expression:
        self._validate_init_args()

        check_serializable(
            self.inner,
            "`@daft.udf` requires that the UDF is serializable. Please double-check that the function does not use any global variables.\n\nIf it does, please use the legacy `@daft.udf` with a class UDF instead and initialize the global in the `__init__` method.",
        )

        bound_args = self._bind_args(*args, **kwargs)
        expressions = list(bound_args.expressions().values())

        return Expression.udf(
            name=self.name,
            inner=self.wrapped_inner,
            bound_args=bound_args,
            expressions=expressions,
            return_dtype=self.return_dtype,
            init_args=self.init_args,
            resource_request=self.resource_request,
            batch_size=self.batch_size,
            concurrency=self.concurrency,
            use_process=self.use_process,
            ray_options=self.ray_options,
        )

    def override_options(
        self,
        *,
        num_cpus: float | None = _UnsetMarker,
        num_gpus: float | None = _UnsetMarker,
        memory_bytes: int | None = _UnsetMarker,
        ray_options: dict[str, Any] | None = None,
        batch_size: int | None = _UnsetMarker,
    ) -> UDF:
        """Replace the resource requests for running each instance of your UDF.

        Args:
            num_cpus: Number of CPUs to allocate each running instance of your UDF. Note that this is purely used for placement (e.g. if your
                machine has 8 CPUs and you specify num_cpus=4, then Daft can run at most 2 instances of your UDF at a time).
            num_gpus: Number of GPUs to allocate each running instance of your UDF. This is used for placement and also for allocating
                the appropriate GPU to each UDF using `CUDA_VISIBLE_DEVICES`.
            memory_bytes: Amount of memory to allocate each running instance of your UDF in bytes. If your UDF is experiencing out-of-memory errors,
                this parameter can help hint Daft that each UDF requires a certain amount of heap memory for execution.
            ray_options: Ray options to pass to the UDF. see more  https://docs.ray.io/en/latest/ray-core/api/doc/ray.actor.ActorClass.options.html#ray.actor.ActorClass.options
            batch_size: Enables batching of the input into batches of at most this size. Results between batches are concatenated.

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
        new_resource_request = ResourceRequest() if self.resource_request is None else self.resource_request
        if num_cpus is not _UnsetMarker:
            new_resource_request = new_resource_request.with_num_cpus(num_cpus)
        if num_gpus is not _UnsetMarker:
            new_resource_request = new_resource_request.with_num_gpus(num_gpus)
        if memory_bytes is not _UnsetMarker:
            new_resource_request = new_resource_request.with_memory_bytes(memory_bytes)

        new_ray_options = ray_options if ray_options is not None else self.ray_options
        new_batch_size = self.batch_size if batch_size is _UnsetMarker else batch_size

        return dataclasses.replace(
            self, resource_request=new_resource_request, batch_size=new_batch_size, ray_options=new_ray_options
        )

    def _validate_init_args(self) -> None:
        if isinstance(self.inner, type):
            init_sig = inspect.signature(self.inner.__init__)  # type: ignore
            if (
                any(param.default is param.empty for param in init_sig.parameters.values() if param.name != "self")
                and self.init_args is None
            ):
                raise ValueError(
                    "Cannot call class UDF without initialization arguments. Please either specify default arguments in your __init__ or provide "
                    "initialization arguments using `.with_init_args(...)`."
                )
        else:
            if self.init_args is not None:
                raise ValueError("Function UDFs cannot have init args.")

    def _bind_args(self, *args: Any, **kwargs: Any) -> BoundUDFArgs:
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

    def with_concurrency(self, concurrency: int) -> UDF:
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

    def run_on_process(self, use_process: bool) -> UDF:
        """Override whether this UDF should run on a separate process or not.

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
            >>> # New UDF that will run on a separate process
            >>> MyGpuUdf_separate_process = MyGpuUdf.run_on_process(True)
        """
        return dataclasses.replace(self, use_process=use_process)

    def with_init_args(self, *args: Any, **kwargs: Any) -> UDF:
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
            ╭────────┬─────────────┬─────────────────────╮
            │ foo    ┆ bar_world   ┆ bar_custom          │
            │ ---    ┆ ---         ┆ ---                 │
            │ String ┆ String      ┆ String              │
            ╞════════╪═════════════╪═════════════════════╡
            │ hello  ┆ hello world ┆ hello my old friend │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ hello  ┆ hello world ┆ hello my old friend │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ hello  ┆ hello world ┆ hello my old friend │
            ╰────────┴─────────────┴─────────────────────╯
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


def udf(
    *,
    return_dtype: DataTypeLike,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    memory_bytes: int | None = None,
    ray_options: dict[str, Any] | None = None,
    batch_size: int | None = None,
    concurrency: int | None = None,
    use_process: bool | None = None,
) -> Callable[[UserDefinedPyFuncLike], UDF]:
    """(DEPRECATED) `@udf` Decorator to convert a Python function/class into a `UDF`.

    UDFs allow users to run arbitrary Python code on the outputs of Expressions.

    Args:
        return_dtype (DataType): Returned type of the UDF
        num_cpus: Number of CPUs to allocate each running instance of your UDF. Note that this is purely used for placement (e.g. if your
            machine has 8 CPUs and you specify num_cpus=4, then Daft can run at most 2 instances of your UDF at a time). The default `None`
            indicates that Daft is free to allocate as many instances of the UDF as it wants to.
        num_gpus: Number of GPUs to allocate each running instance of your UDF. This is used for placement and also for allocating
            the appropriate GPU to each UDF using `CUDA_VISIBLE_DEVICES`.
        memory_bytes: Amount of memory to allocate each running instance of your UDF in bytes. If your UDF is experiencing out-of-memory errors,
            this parameter can help hint Daft that each UDF requires a certain amount of heap memory for execution.
        ray_options: Extra Ray options, e.g. {"label_selector": {...}}. see more https://docs.ray.io/en/latest/ray-core/api/doc/ray.actor.ActorClass.options.html#ray.actor.ActorClass.options
        batch_size: Enables batching of the input into batches of at most this size. Results between batches are concatenated.
        concurrency: Spin up `N` number of persistent replicas of the UDF to process all partitions. Defaults to `None` which will spin up one
            UDF per partition. This is especially useful for expensive initializations that need to be amortized across partitions such as
            loading model weights for model batch inference.
        use_process: Run the UDF on a separate process.
            This is useful for UDFs that run a lot of Python-only code, since it avoids GIL overhead.
            This is not necessary for UDFs that run C-extension code, like NumPy or PyTorch.
            Defaults to `None` where Daft will automatically choose based on runtime performance.
            Note: Users should generally never set this flag manually.

    Returns:
        Callable[[UserDefinedPyFuncLike], UDF]: UDF decorator - converts a user-provided Python function as a UDF that can be called on Expressions

    Note:
        In most cases, UDFs will be slower than a native kernel/expression because of the required Rust and Python overheads. If
        your computation can be expressed using Daft expressions, you should do so instead of writing a UDF. If your UDF expresses a
        common use-case that isn't already covered by Daft, you should file a ticket or contribute this functionality back to Daft
        as a kernel!

    Examples:
        In the example below, we create a UDF that:

        1. Receives data under the argument name ``x``
        2. Iterates over the ``x`` Daft Series
        3. Adds a Python constant value ``c`` to every element in ``x``
        4. Returns a new list of Python values which will be coerced to the specified return type: ``return_dtype=DataType.int64()``.
        5. We can call our UDF on a dataframe using any of the dataframe projection operations ([df.with_column()](https://docs.daft.ai/en/latest/api/dataframe/#daft.DataFrame.with_column),
        [df.select()](https://docs.daft.ai/en/latest/api/dataframe/#daft.DataFrame.select), etc.)

        >>> import daft
        >>> @daft.udf(return_dtype=daft.DataType.int64())
        ... def add_constant(x: daft.Series, c=10):
        ...     return [v + c for v in x]
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

        **Resource Requests:**

        You can also hint Daft about the resources that your UDF will require to run. For example, the following UDF requires 2 CPUs to run. On a
        machine/cluster with 8 CPUs, Daft will be able to run up to 4 instances of this UDF at once!

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

        Your UDF's resources can also be overridden before you call it like so:

        >>> import daft
        >>> @daft.udf(return_dtype=daft.DataType.int64(), num_cpus=4)
        ... def udf_needs_4_cpus(x: daft.Series):
        ...     return x
        >>>
        >>> # Override the num_cpus to 2 instead
        >>> udf_needs_2_cpus = udf_needs_4_cpus.override_options(num_cpus=2)
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

        **Concurrency:**

        With the `concurrency` parameter, you can tell Daft how many instances of your UDF you want to run at the same time.
        If `concurrency` is set with a class UDF, only that many instances of the class will be run at a time, and each instance will reused for different batches.

        This is especially useful if your UDF has a costly initialization step, for example, if you are loading a ML model into memory.

        >>> import daft
        >>> @daft.udf(
        ...     return_dtype=daft.DataType.string(),
        ...     concurrency=4,  # only create 4 instances of this UDF
        ... )
        ... class MLModelUDF:
        ...     def __init__(self):
        ...         self.model = some_slow_initialization_step()
        ...
        ...     def __call__(self, data):
        ...         return self.model(data.to_pylist())

    """
    warnings.warn(
        "The `@daft.udf` decorator is deprecated since Daft version >= 0.7.0 and will be removed in >= 0.8.0. Please use `@daft.func` and `@daft.cls` instead.\nSee the migration guide for more details: https://docs.daft.ai/en/stable/custom-code/migration/",
        category=DeprecationWarning,
        stacklevel=2,
    )

    inferred_return_dtype = DataType._infer(return_dtype)

    def _udf(f: UserDefinedPyFuncLike) -> UDF:
        # Grab a name for the UDF. It **should** be unique.
        module_name = getattr(f, "__module__", "")
        qual_name = getattr(f, "__qualname__")

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
        udf = UDF(
            inner=f,
            name=name,
            return_dtype=inferred_return_dtype,
            resource_request=resource_request,
            batch_size=batch_size,
            concurrency=concurrency,
            use_process=use_process,
            ray_options=ray_options,
        )

        daft.attach_function(udf)
        return udf

    return _udf
