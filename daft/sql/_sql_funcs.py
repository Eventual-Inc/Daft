"""This module is used for Sphinx documentation only. We procedurally generate Python functions to allow
Sphinx to generate documentation pages for every SQL function.
"""

from __future__ import annotations

from inspect import Parameter as _Parameter
from inspect import Signature as _Signature

from daft.daft import list_sql_functions as _list_sql_functions


def _create_sql_function(func_name: str, docstring: str, arg_names: list[str]):
    def sql_function(*args, **kwargs):
        raise NotImplementedError("This function is for documentation purposes only and should not be called.")

    sql_function.__name__ = func_name
    sql_function.__qualname__ = func_name
    sql_function.__doc__ = docstring
    sql_function.__signature__ = _Signature([_Parameter(name, _Parameter.POSITIONAL_OR_KEYWORD) for name in arg_names])  # type: ignore[attr-defined]

    # Register the function in the current module
    globals()[func_name] = sql_function


__all__ = []

for sql_function_stub in _list_sql_functions():
    _create_sql_function(sql_function_stub.name, sql_function_stub.docstring, sql_function_stub.arg_names)
    __all__.append(sql_function_stub.name)
