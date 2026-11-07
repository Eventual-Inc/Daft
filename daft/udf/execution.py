from __future__ import annotations

import abc
import collections
import collections.abc
import inspect
import typing
from typing import TYPE_CHECKING, Any, Concatenate, TypeVar, get_origin
from weakref import WeakKeyDictionary

from daft import DataType
from daft.dependencies import np, pa
from daft.expressions.expressions import Expression
from daft.series import Series
from daft.udf.metrics import _metrics_context
from daft.udf.udf_v2 import IS_CLS_METHOD_ATTR

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from daft.daft import OperatorMetrics, PyDataType, PySeries

    from .udf_v2 import ClsBase

C = TypeVar("C")


def replace_expressions_with_evaluated_args(
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    args, kwargs = original_args
    new_args = tuple(evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args)
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}
    return new_args, new_kwargs


class _MapConversionPlan:
    """Pre-resolved plan describing which UDF parameters should be converted to dict.

    ``dict_params`` is the set of parameter names annotated as ``dict[...]``.
    ``parameters`` holds the wrapped function's ``inspect.Parameter`` objects
    (excluding ``self``/``cls`` for instance methods) so that arguments can be
    bound according to Python's calling rules.
    """

    __slots__ = ("dict_params", "parameters")

    def __init__(self, dict_params: frozenset[str], parameters: list[inspect.Parameter]):
        self.dict_params = dict_params
        self.parameters = parameters

    def is_empty(self) -> bool:
        return not self.dict_params


_map_conversion_plan_cache: WeakKeyDictionary[Callable[..., Any], _MapConversionPlan] = WeakKeyDictionary()


def _resolve_cls_method(method: Callable[..., Any]) -> Callable[..., Any] | None:
    """Return the original ``@daft.cls`` method if ``method`` is one, else ``None``.

    ``@daft.method`` marks the class method with ``IS_CLS_METHOD_ATTR``. The
    generator wrapper built in ``Func.__call__`` wraps that method with
    ``functools.wraps``, which copies the marker onto the wrapper too (via
    ``__dict__``), so resolve through ``__wrapped__`` first to return the original
    method rather than the wrapper.
    """
    wrapped = getattr(method, "__wrapped__", None)
    if wrapped is not None and getattr(wrapped, IS_CLS_METHOD_ATTR, False):
        return wrapped
    if getattr(method, IS_CLS_METHOD_ATTR, False):
        return method
    return None


def _build_map_conversion_plan(method: Callable[..., Any]) -> _MapConversionPlan:
    # `@daft.cls` methods include a bound `self`/`cls` param to skip; `@daft.func`
    # passes a wraps-wrapped method whose `__wrapped__` is the original function.
    cls_method = _resolve_cls_method(method)
    if cls_method is not None:
        sig_source = cls_method
        skip_first = True
    else:
        sig_source = inspect.unwrap(method)
        skip_first = False

    try:
        sig = inspect.signature(sig_source, follow_wrapped=False)
    except Exception:
        return _MapConversionPlan(frozenset(), [])

    dict_params = _resolve_dict_annotated_params(sig_source, sig)

    parameters = list(sig.parameters.values())
    if skip_first:
        parameters = parameters[1:]

    return _MapConversionPlan(dict_params, parameters)


def _resolve_dict_annotated_params(sig_source: Callable[..., Any], sig: inspect.Signature) -> frozenset[str]:
    """Return the names of parameters annotated as ``dict[...]``.

    Only parameter annotations are resolved; the return annotation is ignored so
    that generator UDFs (whose ``typing.Iterator`` return annotation can't be
    resolved after a cloudpickle round-trip) still work. Resolving each parameter
    independently also means one unresolvable annotation doesn't disable the rest.
    """
    # Builtins and some callables have no `__globals__`; use an empty namespace so
    # they get their arguments unchanged instead of raising.
    globalns = dict(getattr(sig_source, "__globals__", None) or {})
    # Names that commonly appear only in annotations and are dropped by cloudpickle.
    globalns.setdefault("typing", typing)
    globalns.setdefault("collections", collections)
    globalns.setdefault("abc", abc)
    globalns.setdefault("collections.abc", collections.abc)

    dict_params = set()
    for param in sig.parameters.values():
        ann = param.annotation
        if ann is inspect.Parameter.empty:
            continue
        # Keep evaluating string annotations (postponed or ForwardRef) until we
        # reach a real type object, matching `get_type_hints`.
        seen: set[str] = set()
        while isinstance(ann, str):
            if ann in seen:  # guard against self-referential strings
                ann = None
                break
            seen.add(ann)
            try:
                ann = eval(ann, globalns, {})
            except Exception:
                ann = None
                break
        if get_origin(ann) is dict:
            dict_params.add(param.name)
    return frozenset(dict_params)


def _get_map_conversion_plan(method: Callable[..., Any]) -> _MapConversionPlan:
    """Return a cached map-conversion plan for ``method``, resolving it once."""
    plan = _map_conversion_plan_cache.get(method)
    if plan is not None:
        return plan
    plan = _build_map_conversion_plan(method)
    _map_conversion_plan_cache[method] = plan
    return plan


def _convert_annotated_map_args(
    method: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Convert params annotated as ``dict[...]`` from list-of-tuples to dict (lossy).

    Daft represents Arrow map values passed into row-wise UDFs as a list of
    ``(key, value)`` tuples (see ``Literal::into_pyobject`` for ``Map``). If a
    parameter is annotated with a ``dict`` type, convert that value with
    ``dict(...)`` before invoking the user function. Duplicate keys keep the last
    value (``lossy``), matching ``dict([("a", 1), ("a", 2)])``.

    Arguments are bound according to Python's calling rules so that a
    ``**kwargs`` parameter annotated as ``dict[...]`` converts each keyword
    argument it captures, and a keyword sharing a positional-only parameter's
    name does not prevent that positional argument from being converted.
    """
    plan = _get_map_conversion_plan(method)
    if plan.is_empty():
        return args, kwargs

    parameters = plan.parameters
    dict_params = plan.dict_params

    def _convert(value: Any, name: str) -> Any:
        if name not in dict_params or value is None or isinstance(value, dict):
            return value
        return dict(value)

    positional_params = [
        param
        for param in parameters
        if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    var_positional = next((param for param in parameters if param.kind is inspect.Parameter.VAR_POSITIONAL), None)
    var_keyword = next((param for param in parameters if param.kind is inspect.Parameter.VAR_KEYWORD), None)

    new_args = list(args)
    arg_index = 0
    for param in positional_params:
        if arg_index >= len(args):
            break
        new_args[arg_index] = _convert(args[arg_index], param.name)
        arg_index += 1
    if var_positional is not None:
        for i in range(arg_index, len(args)):
            new_args[i] = _convert(args[i], var_positional.name)

    # Names of parameters already bound by a positional argument. A keyword with
    # the same name (e.g. for a positional-only parameter alongside ``**kwargs``)
    # must not be mistaken for that positional argument.
    bound_positionally = {param.name for param in positional_params[:arg_index]}

    new_kwargs = {}
    for key, value in kwargs.items():
        target = next(
            (
                param
                for param in parameters
                if param.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
                and param.name == key
                and param.name not in bound_positionally
            ),
            var_keyword,
        )
        new_kwargs[key] = _convert(value, target.name) if target is not None else value

    return tuple(new_args), new_kwargs


async def call_async_func_batched(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args_list: list[list[Any]],
) -> tuple[PySeries, OperatorMetrics]:
    import asyncio

    bound_method = cls._daft_bind_method(method)

    tasks = []
    for evaluated_args in evaluated_args_list:
        args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args)
        args, kwargs = _convert_annotated_map_args(method, args, kwargs)
        coroutine = bound_method(*args, **kwargs)
        tasks.append(coroutine)

    dtype = DataType._from_pydatatype(return_dtype)
    with _metrics_context() as metrics:
        outputs = await asyncio.gather(*tasks)

    series = Series.from_pylist(outputs, dtype=dtype)._series
    return series, metrics


def call_func(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> tuple[Any, OperatorMetrics]:
    """Called from Rust to evaluate a Python scalar UDF. Returns a list of Python objects."""
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args)
    args, kwargs = _convert_annotated_map_args(method, args, kwargs)

    bound_method = cls._daft_bind_method(method)

    with _metrics_context() as metrics:
        output = bound_method(*args, **kwargs)

    return output, metrics


def call_batch_func(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Series],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[PySeries],
) -> tuple[PySeries, OperatorMetrics]:
    """Called from Rust to evaluate a Python batch UDF. Returns a PySeries."""
    evaluated_args_series = [Series._from_pyseries(arg) for arg in evaluated_args]
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args_series)

    bound_method = cls._daft_bind_method(method)

    with _metrics_context() as metrics:
        output = bound_method(*args, **kwargs)

    if isinstance(output, Series):
        output_series = output
    elif isinstance(output, list):
        output_series = Series.from_pylist(output)
    elif np.module_available() and isinstance(output, np.ndarray):
        output_series = Series.from_numpy(output)
    elif pa.module_available() and isinstance(output, (pa.Array, pa.ChunkedArray)):
        output_series = Series.from_arrow(output)
    else:
        raise ValueError(f"Expected output to be a Series, list, numpy array, or pyarrow array, got {type(output)}")

    return output_series._series, metrics


async def call_batch_async(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Coroutine[Any, Any, Series]],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[PySeries],
) -> tuple[PySeries, OperatorMetrics]:
    """Called from Rust to evaluate a Python batch UDF. Returns a PySeries."""
    evaluated_args_series = [Series._from_pyseries(arg) for arg in evaluated_args]
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args_series)

    bound_coroutine: Callable[..., Coroutine[Any, Any, Series]] = cls._daft_bind_coroutine_method(method)

    with _metrics_context() as metrics:
        output = await bound_coroutine(*args, **kwargs)

    if isinstance(output, Series):
        output_series = output
    elif isinstance(output, list):
        output_series = Series.from_pylist(output)
    elif np.module_available() and isinstance(output, np.ndarray):
        output_series = Series.from_numpy(output)
    elif pa.module_available() and isinstance(output, (pa.Array, pa.ChunkedArray)):
        output_series = Series.from_arrow(output)
    else:
        raise ValueError(f"Expected output to be a Series, list, numpy array, or pyarrow array, got {type(output)}")

    return output_series._series, metrics
