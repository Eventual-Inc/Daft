# We ban importing from daft.unity_catalog as a module level import because it is expensive despite
# not always being needed. Within the daft.unity_catalog module itself we ignore this restriction.
from .unity_catalog import UnityCatalog, UnityCatalogTable  # noqa: TID253

__all__ = ["UnityCatalog", "UnityCatalogTable"]
