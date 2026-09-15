from __future__ import annotations

from daft_lance.namespace import (
    DatasetOpenContext,
    validate_uri_or_namespace,
)
from lance_namespace.errors import TableNotFoundError

#: Raised when the addressed Lance table does not exist yet. Namespace implementations
#: signal this with `TableNotFoundError`; plain URI targets surface it as a filesystem
#: or object-store error instead. Kept narrow on purpose: a permission or connectivity
#: failure must not be mistaken for "the table is not there".
TABLE_NOT_FOUND_ERRORS: tuple[type[Exception], ...] = (
    TableNotFoundError,
    ValueError,
    FileNotFoundError,
    OSError,
)

__all__ = ["TABLE_NOT_FOUND_ERRORS", "DatasetOpenContext", "validate_uri_or_namespace"]
