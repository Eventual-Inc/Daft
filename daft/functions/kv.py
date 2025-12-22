"""KV Store functions for Daft.

This module provides key-value store operations as standalone functions,
following the session-based architecture pattern similar to daft.ai.provider.

The module includes functions for putting, getting, and checking existence of
key-value pairs in KV store backends (currently supporting Lance).

Functions:
    kv_put: Put key-value pairs into a KV store.
    kv_get: Get data from KV store using keys.
    kv_batch_get: Batch get operation for KV store.
    kv_exists: Check if keys exist in KV store.

Examples:
    >>> import daft
    >>> from daft.expressions import col
    >>> from daft.kv import load_kv
    >>> from daft.functions.kv import kv_put, kv_get, kv_get_with_name, kv_batch_get_with_name, kv_exists_with_name
    >>>
    >>> # Lance KV basic flow
    >>> # Assuming we have a lance dataset at 's3://bucket/dataset' with a key column 'id'
    >>> lance_kv = load_kv("lance", name="my_store", uri="s3://bucket/dataset", key_column="id")  # doctest: +SKIP
    >>> daft.attach_kv(lance_kv, alias="my_store")  # doctest: +SKIP
    >>> daft.set_kv("my_store")  # doctest: +SKIP
    >>>
    >>> # Build base DataFrame
    >>> df = daft.from_pydict({"id": [1, 2, 3]})
    >>>
    >>> # Get data from the attached store
    >>> df_out = df.with_column("data", kv_get(col("id"), columns=["value"]))  # doctest: +SKIP
    >>>
    >>> # Using specific store name
    >>> df_out = df.with_column("data", kv_get_with_name("my_store", col("id"), columns=["value"]))  # doctest: +SKIP
    >>>
    >>> # Batch get
    >>> df_out = df.with_column("data", kv_batch_get_with_name("my_store", col("id"), batch_size=100))  # doctest: +SKIP
    >>>
    >>> # Check existence
    >>> df_out = df.with_column("exists", kv_exists_with_name("my_store", col("id")))  # doctest: +SKIP
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from types import ModuleType

    class _NativeModule(ModuleType):
        def kv_get_with_name(self, store_name: Any, keys: Any, on_error: Any, columns: Any = ...) -> Any: ...
        def kv_batch_get_with_name(
            self, store_name: Any, keys: Any, batch_size: Any, on_error: Any, columns: Any = ...
        ) -> Any: ...
        def kv_exists_with_name(self, store_name: Any, keys: Any) -> Any: ...
        def kv_put_with_name(self, store_name: Any, key: Any, value: Any) -> Any: ...
        def kv_get_with_config(self, config: str, keys: Any, on_error: Any, columns: Any = ...) -> Any: ...
        def kv_batch_get_with_config(
            self, config: str, keys: Any, batch_size: Any, on_error: Any, columns: Any = ...
        ) -> Any: ...

    native: _NativeModule
else:
    import daft.daft as native

if TYPE_CHECKING:
    from daft.expressions import Expression

from daft.expressions import Expression, col

if TYPE_CHECKING:
    from daft.kv import KVStore


def kv_put(kv_store: KVStore, key: Expression | str, value: Expression | Any) -> Expression:
    """Put key-value pairs into a KV store.

    Args:
        kv_store (KVStore): KV store instance registered via daft.attach_kv
        key (Expression | str): Key column/expression or column name
        value (Expression | Any): Value column/expression or Python object

    Returns:
        Expression: Struct with operation status and key
    """
    store_name = getattr(kv_store, "name", "")
    return kv_put_with_name(store_name, key, value)


def kv_get(
    keys: Expression | str,
    columns: list[str] | str | None = None,
    on_error: Literal["raise", "null"] = "raise",
    store: str | None = None,
) -> Expression:
    """Get data from a single KV store by name (or current default).

    - columns=None: return all fields for the store
    - columns=[...]: return only requested fields
    - store: store name/alias; if None, use current_kv()
    """
    from daft.session import current_kv

    # Resolve target store name
    if store is not None:
        store_name = store
    else:
        kv_store_optional = current_kv()
        if kv_store_optional is None:
            raise ValueError("No KV store is currently attached to the session.")
        store_name = getattr(kv_store_optional, "name", "")

    return kv_get_with_name(store_name, keys, columns, on_error)


def kv_batch_get(
    keys: Expression | str,
    columns: list[str] | str | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
    store: str | None = None,
) -> Expression:
    """Batch get operation for KV store."""
    from daft.session import current_kv, get_kv

    if store is not None:
        try:
            # Verify store exists
            kv_store = get_kv(store)
            store_name = getattr(kv_store, "name", "")
        except ValueError as e:
            raise ValueError(
                f"KV store '{store}' not found in session. "
                f"Please attach the KV store using daft.attach() first. Original error: {e}"
            )
    else:
        kv_store_optional = current_kv()
        if kv_store_optional is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a store parameter."
            )
        store_name = getattr(kv_store_optional, "name", "")

    return kv_batch_get_with_name(store_name, keys, columns, batch_size, on_error)


def kv_exists(
    keys: Expression | str,
    on_error: Literal["raise", "null"] = "raise",
    store: str | None = None,
) -> Expression:
    """Check if keys exist in KV store."""
    from daft.session import current_kv, get_kv

    if store is not None:
        try:
            kv_store = get_kv(store)
            store_name = getattr(kv_store, "name", "")
        except ValueError as e:
            raise ValueError(
                f"KV store '{store}' not found in session. "
                f"Please attach the KV store using daft.attach() first. Original error: {e}"
            )
    else:
        kv_store_optional = current_kv()
        if kv_store_optional is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a store parameter."
            )
        store_name = getattr(kv_store_optional, "name", "")

    return kv_exists_with_name(store_name, keys, on_error)


def kv_get_with_name(
    name: str | Expression,
    keys: Expression | str,
    columns: list[str] | str | Expression | None = None,
    on_error: Literal["raise", "null"] | Expression = "raise",
) -> Expression:
    """Get data from KV store using row IDs and store name.

    This function retrieves data from a KV store specified by name. If columns is specified,
    it returns a struct containing only the requested columns as top-level keys.
    """
    from daft.expressions import lit

    # Convert keys to Expression if it's a string
    if isinstance(keys, str):
        keys = col(keys)

    # Create expression for the store name
    if isinstance(name, str):
        name_expr = lit(name)
    else:
        name_expr = name

    if isinstance(on_error, str):
        on_error_expr = lit(on_error)
    else:
        on_error_expr = on_error

    # Try to resolve KV config early
    config_json = None
    if isinstance(name, str):
        from daft.context import get_context  # noqa: F401

        # We can't access session directly here easily without circular imports or context access
        # But we can try via context if available
        try:
            # Assuming get_session() exists or we can access it.
            # Actually Daft context doesn't expose session publicly like this usually.
            # But let's check if we can access the global session via daft.session.current_session()
            from daft.session import current_session

            # This returns a PySession wrapper
            sess = current_session()
            if sess:
                # get_kv returns a PyKVStore wrapper or specific store
                # We need to handle potential errors if store doesn't exist
                try:
                    store = sess.get_kv(name)
                    # Check if it's a Lance store
                    if hasattr(store, "backend_type") and store.backend_type == "lance":
                        # Get config
                        if hasattr(store, "get_config"):
                            # This returns a LanceConfig object or similar, we need JSON string
                            # The Rust PyLanceKVStore.to_config() returns a JSON string now
                            # But the Python wrapper might return something else.
                            # Let's check daft/kv/lance.py
                            # LanceKVStore.get_config calls self._inner.to_config() which returns string.
                            # Wait, in python wrapper it returns self._inner.to_config().
                            # And I changed Rust to_config to return String.
                            config_json = store.get_config()
                except Exception:
                    # If store not found or any error, fallback to name-based lookup
                    pass
        except Exception:
            pass

    # Optional columns selection
    columns_expr = None
    if columns is not None:
        if isinstance(columns, Expression):
            columns_expr = columns
        else:
            import json

            cols = columns if isinstance(columns, list) else [columns]
            columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            columns_expr = lit(columns_bytes)

    if config_json is not None:
        if columns_expr is not None:
            return Expression._from_pyexpr(
                native.kv_get_with_config(str(config_json), keys._expr, on_error_expr._expr, columns_expr._expr)
            )
        return Expression._from_pyexpr(native.kv_get_with_config(str(config_json), keys._expr, on_error_expr._expr))

    if columns_expr is not None:
        return Expression._from_pyexpr(
            native.kv_get_with_name(name_expr._expr, keys._expr, on_error_expr._expr, columns_expr._expr)
        )

    # All backends: use ScalarUDF path with name parameter
    return Expression._from_pyexpr(native.kv_get_with_name(name_expr._expr, keys._expr, on_error_expr._expr))


def kv_batch_get_with_name(
    name: str,
    keys: Expression | str,
    columns: list[str] | str | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Batch get operation for KV store using store name."""
    from daft.expressions import lit

    if isinstance(keys, str):
        keys = col(keys)

    # Create expression for the store name
    name_expr = lit(name)
    batch_size_expr = lit(batch_size)
    on_error_expr = lit(on_error)

    # Try to resolve KV config early
    config_json = None
    if isinstance(name, str):
        try:
            from daft.session import current_session

            sess = current_session()
            if sess:
                try:
                    store = sess.get_kv(name)
                    if hasattr(store, "backend_type") and store.backend_type == "lance":
                        if hasattr(store, "get_config"):
                            config_json = store.get_config()
                except Exception:
                    pass
        except Exception:
            pass

    columns_expr = None
    if columns is not None:
        import json

        cols = columns if isinstance(columns, list) else [columns]
        columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        columns_expr = lit(columns_bytes)

    if config_json is not None:
        if columns_expr is not None:
            return Expression._from_pyexpr(
                native.kv_batch_get_with_config(
                    str(config_json),
                    keys._expr,
                    batch_size_expr._expr,
                    on_error_expr._expr,
                    columns_expr._expr,
                )
            )
        return Expression._from_pyexpr(
            native.kv_batch_get_with_config(str(config_json), keys._expr, batch_size_expr._expr, on_error_expr._expr)
        )

    if columns_expr is not None:
        return Expression._from_pyexpr(
            native.kv_batch_get_with_name(
                name_expr._expr,
                keys._expr,
                batch_size_expr._expr,
                on_error_expr._expr,
                columns_expr._expr,
            )
        )

    # All backends: use ScalarUDF path with name parameter
    return Expression._from_pyexpr(
        native.kv_batch_get_with_name(name_expr._expr, keys._expr, batch_size_expr._expr, on_error_expr._expr)
    )


def kv_exists_with_name(
    name: str,
    keys: Expression | str,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Check if row IDs exist in KV store using store name."""
    from daft.expressions import lit

    if isinstance(keys, str):
        keys = col(keys)

    # Create expression for the store name
    name_expr = lit(name)

    # All backends: use ScalarUDF path with name parameter
    return Expression._from_pyexpr(native.kv_exists_with_name(name_expr._expr, keys._expr))


def kv_put_with_name(
    name: str,
    key: Expression | str,
    value: Expression | Any,
) -> Expression:
    """Put key-value pairs into a KV store using store name.

    Args:
        name (str): Name of the KV store
        key (Expression | str): Key column/expression or column name
        value (Expression | Any): Value column/expression or Python object

    Returns:
        Expression: Struct with operation status and key
    """
    from daft.expressions import lit

    if isinstance(key, str):
        key = col(key)
    if not isinstance(value, Expression):
        value = lit(value)

    # Create expression for the store name
    name_expr = lit(name)

    # All backends: use ScalarUDF path with name parameter
    return Expression._from_pyexpr(native.kv_put_with_name(name_expr._expr, key._expr, value._expr))
