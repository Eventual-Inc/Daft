"""HDF5 Functions."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

import daft
from daft.datatype import DataTypeLike
from daft.file.hdf5 import Hdf5File
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    from daft import Expression
    from daft.dependencies import np


def _field_name_from_dataset(dataset: str) -> str:
    name = re.sub(r"\W+", "_", dataset.strip("/")).strip("_")
    return name or "dataset"


def _normalize_datasets(datasets: str | Sequence[str] | Mapping[str, str]) -> dict[str, str]:
    if isinstance(datasets, str):
        return {_field_name_from_dataset(datasets): datasets}
    if isinstance(datasets, Mapping):
        normalized = dict(datasets)
    else:
        pairs = [(_field_name_from_dataset(dataset), dataset) for dataset in datasets]
        names = [name for name, _ in pairs]
        if len(names) != len(set(names)):
            raise ValueError(f"datasets produced duplicate output field names: {names}")
        normalized = dict(pairs)

    if not normalized:
        raise ValueError("datasets must contain at least one HDF5 dataset")
    if any(not isinstance(name, str) or not isinstance(dataset, str) for name, dataset in normalized.items()):
        raise TypeError("datasets must be a string, a sequence of strings, or a mapping of string aliases to strings")
    return normalized


def _tensor_return_dtype(dtype: DataTypeLike | None) -> daft.DataType:
    if dtype is None:
        return daft.DataType.python()

    resolved = daft.DataType._infer(dtype)
    if resolved.is_tensor():
        return resolved
    return daft.DataType.tensor(resolved)


def _read_many_return_dtype(
    datasets: Mapping[str, str],
    dtypes: Mapping[str, DataTypeLike] | None,
) -> daft.DataType:
    if dtypes is None:
        return daft.DataType.struct({name: daft.DataType.python() for name in datasets})

    missing = set(datasets) - set(dtypes)
    extra = set(dtypes) - set(datasets)
    if missing or extra:
        raise ValueError(f"dtypes must match dataset output names; missing={sorted(missing)}, extra={sorted(extra)}")
    return daft.DataType.struct({name: _tensor_return_dtype(dtypes[name]) for name in datasets})


def hdf5_keys_impl(file: Hdf5File, group: str = "/") -> list[str]:
    return file.keys(group)


hdf5_keys_fn = Func._from_func(
    hdf5_keys_impl,
    return_dtype=daft.DataType.list(daft.DataType.string()),
    unnest=False,
    use_process=False,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="hdf5_keys",
)


def hdf5_keys(file_expr: Expression, group: str = "/") -> Expression:
    """List member names directly under an HDF5 group.

    Expression wrapper for ``Hdf5File.keys()``, mirroring h5py
    ``Group.keys()`` while returning a concrete list of strings.

    Args:
        file_expr: ``Hdf5File`` expression.
        group: HDF5 group within the file. Defaults to the root group ``/``.

    Returns:
        Expression containing a list of child names under the group.
    """
    return cast("Expression", hdf5_keys_fn(file_expr, group=group))


def hdf5_read_impl(file: Hdf5File, dataset: str) -> np.ndarray[Any, Any]:
    return file.read(dataset)


def hdf5_read(file_expr: Expression, dataset: str, dtype: DataTypeLike | None = None) -> Expression:
    """Read an HDF5 dataset into a tensor column.

    Expression wrapper for ``Hdf5File.read(dataset)``. The read follows h5py's
    full-dataset access pattern, equivalent to ``h5[dataset][()]`` for each
    input file. If ``dtype`` is omitted, the result is a Python object column
    so h5py's NumPy dtype is preserved. Pass a metadata-derived dtype to get a
    typed tensor column.

    Args:
        file_expr: ``Hdf5File`` expression.
        dataset: Dataset path within the file (for example ``action/proprio``).
        dtype: Optional Daft dtype for the dataset values. This is the tensor
            inner dtype, such as ``DataType.float32()`` or
            ``DataType.from_numpy_dtype(np.dtype("int64"))``.

    Returns:
        Expression containing the dataset values as a Python object when
        ``dtype`` is omitted, or as a tensor when ``dtype`` is provided.
    """
    hdf5_read_fn = Func._from_func(
        hdf5_read_impl,
        return_dtype=_tensor_return_dtype(dtype),
        unnest=False,
        use_process=False,
        is_batch=False,
        batch_size=None,
        max_retries=None,
        on_error=None,
        name_override="hdf5_read",
    )
    return cast("Expression", hdf5_read_fn(file_expr, dataset=dataset))


def hdf5_read_many_impl(file: Hdf5File, datasets: dict[str, str]) -> dict[str, np.ndarray[Any, Any]]:
    return file.read(datasets)


def hdf5_read_many(
    file_expr: Expression,
    datasets: str | Sequence[str] | Mapping[str, str],
    dtypes: Mapping[str, DataTypeLike] | None = None,
) -> Expression:
    """Read multiple HDF5 datasets with one open per input file.

    Expression wrapper for ``Hdf5File.read(datasets)``. Passing a mapping is
    the most stable DataFrame form because the mapping keys become struct field
    names. If ``dtypes`` is omitted, each struct field is a Python object so
    h5py's NumPy dtype is preserved. Pass metadata-derived dtypes keyed by
    output field name to get typed tensor fields.

    Args:
        file_expr: ``Hdf5File`` expression.
        datasets: Either a dataset path, a sequence of dataset paths, or a mapping
            of output field names to dataset paths. The mapping form is preferred
            for stable DataFrame schemas.
        dtypes: Optional mapping from output field names to tensor inner dtypes.
            Keys must match the output names after normalizing ``datasets``.

    Returns:
        Expression containing a struct with one field per requested dataset.
    """
    normalized = _normalize_datasets(datasets)
    read_many_fn = Func._from_func(
        hdf5_read_many_impl,
        return_dtype=_read_many_return_dtype(normalized, dtypes),
        unnest=False,
        use_process=False,
        is_batch=False,
        batch_size=None,
        max_retries=None,
        on_error=None,
        name_override="hdf5_read_many",
    )
    return cast("Expression", read_many_fn(file_expr, datasets=normalized))
