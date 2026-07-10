"""HDF5 Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import daft
from daft.file.hdf5 import Hdf5File
from daft.file.typing import Hdf5ObjectMetadata
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    from daft import Expression


# Hdf5File.keys() as a function hdf5_keys()


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


# Hdf5File.metadata() as a function hdf5_metadata()
def hdf5_metadata_impl(file: Hdf5File, group: str = "/") -> list[Hdf5ObjectMetadata]:
    return file.metadata(group)


hdf5_metadata_fn = Func._from_func(
    hdf5_metadata_impl,
    return_dtype=daft.DataType.list(
        daft.DataType.struct(
            {
                "h5path": daft.DataType.string(),
                "kind": daft.DataType.string(),
                "shape": daft.DataType.list(daft.DataType.int64()),
                "dtype": daft.DataType.string(),
                "chunks": daft.DataType.list(daft.DataType.int64()),
                "compression": daft.DataType.string(),
            }
        )
    ),
    unnest=False,
    use_process=False,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="hdf5_metadata",
)


def hdf5_metadata(file_expr: Expression, group: str = "/") -> Expression:
    """Collect metadata for groups and datasets under an HDF5 group.

    Expression wrapper for ``Hdf5File.metadata(group)``.

    Args:
        file_expr: ``Hdf5File`` expression.
        group: HDF5 group within the file. Defaults to the root group ``/``.

    Returns:
        Expression containing a list of object metadata structs.
    """
    return cast("Expression", hdf5_metadata_fn(file_expr, group=group))


# Hdf5File.attrs() as a function hdf5_attrs()
def hdf5_attrs_impl(file: Hdf5File, h5path: str = "/") -> dict[str, Any]:
    return file.attrs(h5path)


hdf5_attrs_fn = Func._from_func(
    hdf5_attrs_impl,
    return_dtype=daft.DataType.python(),
    unnest=False,
    use_process=False,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="hdf5_attrs",
)


def hdf5_attrs(file_expr: Expression, h5path: str = "/") -> Expression:
    """Read HDF5 attributes for a group or dataset.

    Expression wrapper for ``Hdf5File.attrs(h5path)``.

    Args:
        file_expr: ``Hdf5File`` expression.
        h5path: Group or dataset path. Defaults to the root group ``/``.

    Returns:
        Expression containing a Python dictionary of attribute names to values.
    """
    return cast("Expression", hdf5_attrs_fn(file_expr, h5path=h5path))
