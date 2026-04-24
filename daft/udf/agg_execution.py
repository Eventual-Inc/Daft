from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.series import Series
from daft.udf.execution import replace_expressions_with_evaluated_args

if TYPE_CHECKING:
    from daft.daft import PyDataType, PySeries

_local = threading.local()


def _get_instance(cls_factory: Any, init_args: tuple[tuple[Any, ...], dict[str, Any]]) -> Any:
    cache = getattr(_local, "_udaf_instances", None)
    if cache is None:
        cache = {}
        _local._udaf_instances = cache

    key = id(cls_factory)
    instance = cache.get(key)
    if instance is None:
        args, kwargs = init_args
        instance = cls_factory(*args, **kwargs)
        cache[key] = instance
    return instance


def call_agg_block(
    cls_factory: Any,
    init_args: tuple[tuple[Any, ...], dict[str, Any]],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    input_pyseries_list: list[PySeries],
    state_field_names: list[str],
    state_field_dtypes: list[PyDataType],
) -> list[PySeries]:
    instance = _get_instance(cls_factory, init_args)

    input_series = [Series._from_pyseries(ps) for ps in input_pyseries_list]
    args, kwargs = replace_expressions_with_evaluated_args(original_args, input_series)

    result = instance.agg_block(*args, **kwargs)

    return _pack_state_result(result, state_field_names, state_field_dtypes)


def call_agg_combine(
    cls_factory: Any,
    init_args: tuple[tuple[Any, ...], dict[str, Any]],
    state_pyseries_list: list[PySeries],
    state_field_names: list[str],
    state_field_dtypes: list[PyDataType],
) -> list[PySeries]:
    instance = _get_instance(cls_factory, init_args)

    if len(state_field_names) == 1 and state_field_names[0] == "_state":
        state_series = Series._from_pyseries(state_pyseries_list[0])
        result = instance.combine(state_series)
    else:
        states_dict = {
            name: Series._from_pyseries(ps)
            for name, ps in zip(state_field_names, state_pyseries_list)
        }
        result = instance.combine(states_dict)

    return _pack_state_result(result, state_field_names, state_field_dtypes)


def call_agg_finalize(
    cls_factory: Any,
    init_args: tuple[tuple[Any, ...], dict[str, Any]],
    state_values: list[Any],
    state_field_names: list[str],
    return_dtype: PyDataType,
) -> PySeries:
    instance = _get_instance(cls_factory, init_args)

    if len(state_field_names) == 1 and state_field_names[0] == "_state":
        result = instance.finalize(state_values[0])
    else:
        state_dict = dict(zip(state_field_names, state_values))
        result = instance.finalize(state_dict)

    dtype = DataType._from_pydatatype(return_dtype)
    return Series.from_pylist([result], dtype=dtype)._series


def _pack_state_result(
    result: Any,
    state_field_names: list[str],
    state_field_dtypes: list[PyDataType],
) -> list[PySeries]:
    if len(state_field_names) == 1 and state_field_names[0] == "_state":
        dtype = DataType._from_pydatatype(state_field_dtypes[0])
        return [Series.from_pylist([result], dtype=dtype)._series]
    else:
        out = []
        for name, py_dtype in zip(state_field_names, state_field_dtypes):
            dtype = DataType._from_pydatatype(py_dtype)
            out.append(Series.from_pylist([result[name]], dtype=dtype)._series)
        return out
