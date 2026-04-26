from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.source import DataSource

_builtin_list = list


def register(data_source: type[DataSource], *, name: str | None = None, replace: bool = False) -> None:
    """Register a Python ``DataSource`` class on the current session."""
    from daft.session import current_session

    current_session().register_data_source(data_source, name=name, replace=replace)


def unregister(name: str) -> None:
    """Remove a registered Python ``DataSource`` from the current session."""
    from daft.session import current_session

    current_session().unregister_data_source(name)


def get(name: str) -> type[DataSource]:
    """Return a registered Python ``DataSource`` class."""
    from daft.session import current_session

    return current_session().get_data_source(name)


def list() -> _builtin_list[str]:
    """List registered Python ``DataSource`` names."""
    from daft.session import current_session

    return current_session().list_data_sources()


def read(name: str, **options: Any) -> DataFrame:
    """Read a registered Python ``DataSource`` by name."""
    from daft.session import current_session

    return current_session().read_source(name, **options)


__all__ = [
    "get",
    "list",
    "read",
    "register",
    "unregister",
]
