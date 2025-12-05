from __future__ import annotations

import importlib
import pkgutil
import sys
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar
from warnings import warn

if TYPE_CHECKING:
    from daft import DataFrame

NS = TypeVar("NS")

_discovered_packages: dict[str, str] = {
    name.removeprefix("daft_"): name for _, name, _ in pkgutil.iter_modules() if name.startswith("daft_")
}
_registered_namespaces: dict[str, ModuleType] = {}
_dataframe_extensions_registry: dict[str, type] = {}
_applied_dataframe_extensions: set[str] = set()


class NameSpace(Generic[NS]):
    """Establish property-like namespace object for user-defined functionality."""

    def __init__(self, name: str, namespace: type[NS]) -> None:
        self._accessor = name
        self._ns = namespace

    def __get__(self, instance: NS | None, cls: type[NS]) -> NS | type[NS]:
        if instance is None:
            return self._ns

        ns_instance = self._ns(instance)  # type: ignore[call-arg]
        setattr(instance, self._accessor, ns_instance)
        return ns_instance


def _load_daft_packages() -> None:
    """Load all daft_* packages to trigger their extension registrations."""
    for short_name, full_name in _discovered_packages.items():
        if short_name not in _registered_namespaces:
            _registered_namespaces[short_name] = importlib.import_module(full_name)


def _create_module(name: str, obj: Any) -> ModuleType:
    module_name = f"daft.extensions.{name}"
    if module_name in sys.modules:
        return sys.modules[module_name]

    instance = obj() if isinstance(obj, type) else obj
    mod = ModuleType(module_name)
    for attr in dir(instance):
        if not attr.startswith("_"):
            setattr(mod, attr, getattr(instance, attr))
    sys.modules[module_name] = mod
    return mod


def _create_extension(name: str, cls: type[DataFrame]) -> Callable[[type[NS]], type[NS]]:
    """Register custom extension against the underlying class."""

    def namespace(ns_class: type[NS]) -> type[NS]:
        if hasattr(cls, name):
            warn(
                f"Overriding existing custom namespace {name!r} (on {cls.__name__!r})",
                UserWarning,
                stacklevel=2,
            )

        setattr(cls, name, NameSpace(name, ns_class))
        cls._accessors.add(name)
        return ns_class

    return namespace


def _auto_register_dataframe_extensions() -> None:
    """Auto-register DataFrame extensions from daft_* packages."""
    from daft import DataFrame

    _load_daft_packages()

    for name, extension_class in _dataframe_extensions_registry.items():
        if name not in _applied_dataframe_extensions:
            _create_extension(name, DataFrame)(extension_class)
            _applied_dataframe_extensions.add(name)


def __getattr__(name: str) -> ModuleType:
    if name in _registered_namespaces:
        return _create_module(name, _registered_namespaces[name])

    if name in _discovered_packages:
        full_name = _discovered_packages[name]
        _registered_namespaces[name] = importlib.import_module(full_name)
        return _create_module(name, _registered_namespaces[name])

    raise AttributeError(f"module 'daft.extensions' has no attribute '{name}'")


def register_dataframe_extension(name: str) -> Callable[[type[NS]], type[NS]]:
    """Register a custom namespace on DataFrame instances.

    This allows third-party packages to add methods to DataFrame through a namespace accessor.
    Registered extensions are accessed as `df.<name>.<method>()`.

    Args:
        name: The namespace name for the extension (e.g., "geo", "timeseries")

    Returns:
        A decorator that registers the class as a DataFrame extension

    Examples:
        Register a DataFrame extension with custom methods:

        >>> from daft.extensions import register_dataframe_extension
        >>> import daft
        >>>
        >>> @register_dataframe_extension("capitalizer")
        ... class CapitalizeExtension:
        ...     def __init__(self, df: daft.DataFrame):
        ...         self._df = df
        ...
        ...     def capitalize_column_names(self) -> daft.DataFrame:
        ...         return self._df.select(*[daft.col(c).alias(c.capitalize()) for c in self._df.column_names])
        >>>
        >>> df = daft.from_pydict({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        >>> df.capitalizer.capitalize_column_names().collect()
        ╭───────┬───────╮
        │ Foo   ┆ Bar   │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int64 │
        ╞═══════╪═══════╡
        │ 1     ┆ 4     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """

    def decorator(ns_class: type[NS]) -> type[NS]:
        _dataframe_extensions_registry[name] = ns_class

        if "daft.dataframe" in sys.modules:
            from daft import DataFrame

            if name not in _applied_dataframe_extensions:
                _create_extension(name, DataFrame)(ns_class)
                _applied_dataframe_extensions.add(name)

        return ns_class

    return decorator


__path__ = []
_auto_register_dataframe_extensions()
