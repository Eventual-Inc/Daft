from __future__ import annotations

import sys
from types import ModuleType
from typing import Callable, Generic, TypeVar, Any, TYPE_CHECKING
from warnings import warn

if TYPE_CHECKING:
    from daft import DataFrame

NS = TypeVar("NS")


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


_registered_namespaces = {}


def register_extension(name: str) -> Any:
    """Register a module namespace under daft.extensions.*.

    This allows third-party packages to add functionality to Daft through a plugin system.
    Registered extensions can be accessed as `daft.extensions.<name>` or imported directly.

    Args:
        name: The namespace name for the extension (e.g., "mylib", "analytics")

    Returns:
        A decorator that registers the class or module as an extension

    Examples:
        Register a class with static methods:

        >>> from daft.extensions import register_extension
        >>>
        >>> @register_extension("mylib")
        ... class MyLibExtension:
        ...     @staticmethod
        ...     def hello(name: str):
        ...         return f"Hello, {name}!"
        >>>
        >>> # Use via attribute access
        >>> daft.extensions.mylib.hello("World")
        'Hello, World!'
        >>>
        >>> # Or import directly
        >>> from daft.extensions.mylib import hello
        >>> hello("World")
        'Hello, World!'
    """

    def decorator(obj: Any) -> Any:
        _registered_namespaces[name] = obj

        module_name = f"daft.extensions.{name}"
        if module_name not in sys.modules:
            instance = obj() if isinstance(obj, type) else obj
            mod = ModuleType(module_name)
            for attr in dir(instance):
                if not attr.startswith("_"):
                    setattr(mod, attr, getattr(instance, attr))
            sys.modules[module_name] = mod

        return obj

    return decorator


def __getattr__(name: str) -> ModuleType:
    if name in _registered_namespaces:
        namespace = _registered_namespaces[name]
        obj = namespace() if isinstance(namespace, type) else namespace

        module_name = f"daft.extensions.{name}"
        if module_name not in sys.modules:
            mod = ModuleType(module_name)
            for attr in dir(obj):
                if not attr.startswith("_"):
                    setattr(mod, attr, getattr(obj, attr))
            sys.modules[module_name] = mod

        return sys.modules[module_name]
    raise AttributeError(f"module 'daft.extensions' has no attribute '{name}'")


def _create_extension(name: str, cls: type[DataFrame]) -> Callable[[type[NS]], type[NS]]:
    """Register custom extension against the underlying class."""

    def namespace(ns_class: type[NS]) -> type[NS]:
        if hasattr(cls, name):
            warn(
                f"Overriding existing custom namespace {name!r} (on {cls.__name__!r})",
                UserWarning,
            )

        setattr(cls, name, NameSpace(name, ns_class))
        cls._accessors.add(name)
        return ns_class

    return namespace


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
    from daft import DataFrame

    return _create_extension(name, DataFrame)


__path__ = []
__all__ = ["register_dataframe_extension", "register_extension"]
