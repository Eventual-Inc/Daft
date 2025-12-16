"""KV Store functions for Daft.

This module provides key-value store operations as standalone functions,
following the session-based architecture pattern similar to daft.ai.provider.

The module includes functions for putting, getting, and checking existence of
key-value pairs in various KV store backends such as memory, Lance, and LMDB.

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
    >>> # Memory KV basic flow
    >>> embeddings_kv = load_kv("memory", name="embeddings")  # doctest: +SKIP
    >>> metadata_kv = load_kv("memory", name="metadata")  # doctest: +SKIP
    >>> daft.attach_kv(embeddings_kv, alias="mem_embeddings")  # doctest: +SKIP
    >>> daft.attach_kv(metadata_kv, alias="mem_metadata")  # doctest: +SKIP
    >>> daft.set_kv("mem_embeddings")  # doctest: +SKIP
    >>>
    >>> # Build base DataFrame
    >>> df = daft.from_pydict(
    ...     {
    ...         "item_id": [0, 1, 2],
    ...         "embedding": [[0.0, 0.1], [1.0, 1.1], [2.0, 2.2]],
    ...         "metadata": ["m0", "m1", "m2"],
    ...     }
    ... )
    >>>
    >>> # Put key/value pairs into different stores (returns Struct)
    >>> _ = df.select(kv_put(embeddings_kv, col("item_id"), col("embedding")).alias("put")).collect()  # doctest: +SKIP
    >>> _ = df.select(kv_put(metadata_kv, col("item_id"), col("metadata")).alias("put")).collect()  # doctest: +SKIP
    >>>
    >>> # Get aligned by row: same order as df['item_id']
    >>> e1 = kv_get_with_name("item_id", "mem_embeddings", columns=["embedding"])  # doctest: +SKIP
    >>> e2 = kv_get_with_name("item_id", "mem_metadata", columns=["metadata"])  # doctest: +SKIP
    >>> merge = daft.func(lambda a, b: {**(a or {}), **(b or {})}, return_dtype=daft.DataType.python())
    >>> df_out = df.with_column("data", merge(e1, e2))  # doctest: +SKIP
    >>>
    >>> # Using default store via set_kv(...)
    >>> df_default = df.with_column("embedding_data", kv_get("item_id", columns=["embedding"]))  # doctest: +SKIP
    >>>
    >>> # Join equivalence (explicit join matches merged dicts)
    >>> df_e = df.with_column("embedding_data", e1).select("item_id", "embedding_data")  # doctest: +SKIP
    >>> df_m = df.with_column("metadata_data", e2).select("item_id", "metadata_data")  # doctest: +SKIP
    >>> df_joined = df_e.join(df_m, on="item_id")  # doctest: +SKIP
    >>> # Robust merge for missing keys: (a or {}) / (b or {})
    >>> combined = df_joined.with_column(  # doctest: +SKIP
    ...     "data",
    ...     daft.func(lambda a, b: {**(a or {}), **(b or {})}, return_dtype=daft.DataType.python())(  # doctest: +SKIP
    ...         daft.col("embedding_data"),
    ...         daft.col("metadata_data"),  # doctest: +SKIP
    ...     ),  # doctest: +SKIP
    ... )  # doctest: +SKIP
    >>>
    >>> # Optional: batch get and exists (if supported by the backend)
    >>> _ = df.select(kv_batch_get_with_name(col("item_id"), "mem_embeddings", batch_size=100)).collect()  # doctest: +SKIP
    >>> _ = df.select(kv_exists_with_name(col("item_id"), "mem_embeddings")).collect()  # doctest: +SKIP
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from types import ModuleType

    class _NativeModule(ModuleType):
        def kv_get_with_name(self, keys: Any, store_name: Any, on_error: Any, columns: Any = ...) -> Any: ...
        def kv_batch_get_with_name(
            self, keys: Any, store_name: Any, batch_size: Any, on_error: Any, columns: Any = ...
        ) -> Any: ...
        def kv_exists_with_name(self, keys: Any, store_name: Any) -> Any: ...
        def kv_put_with_name(self, key: Any, value: Any, store_name: Any) -> Any: ...
        def kv_get_with_config(self, keys: Any, kv_config: Any) -> Any: ...
        def kv_batch_get_with_config(self, keys: Any, kv_config: Any, batch_size: Any) -> Any: ...
        def kv_exists_with_config(self, keys: Any, kv_config: Any) -> Any: ...
        def kv_put_with_config(self, key: Any, value: Any, kv_config: Any) -> Any: ...

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
    from daft.expressions import lit

    if isinstance(key, str):
        key = col(key)
    if not isinstance(value, Expression):
        value = lit(value)

    store_name = getattr(kv_store, "name", "")
    name_expr = lit(store_name)
    return Expression._from_pyexpr(native.kv_put_with_name(key._expr, value._expr, name_expr._expr))


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
    from daft.expressions import lit
    from daft.session import current_kv

    # Convert keys to Expression if it's a string
    if isinstance(keys, str):
        keys = col(keys)

    # Resolve target store name
    if store is not None:
        store_name = store
    else:
        kv_store_optional = current_kv()
        if kv_store_optional is None:
            raise ValueError("No KV store is currently attached to the session.")
        store_name = getattr(kv_store_optional, "name", "")

    name_expr = lit(store_name)
    on_error_expr = lit(on_error)
    columns_expr = None
    if columns is not None:
        import json

        cols = columns if isinstance(columns, list) else [columns]
        columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        columns_expr = lit(columns_bytes)

    if columns_expr is None:
        return Expression._from_pyexpr(native.kv_get_with_name(keys._expr, name_expr._expr, on_error_expr._expr))
    return Expression._from_pyexpr(
        native.kv_get_with_name(keys._expr, name_expr._expr, on_error_expr._expr, columns_expr._expr)
    )


def kv_batch_get(
    keys: Expression | str,
    columns: list[str] | str | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
    store: str | None = None,
) -> Expression:
    """Batch get operation for KV store."""
    from daft.expressions import lit
    from daft.session import current_kv, get_kv

    if isinstance(keys, str):
        keys = col(keys)

    if store is not None:
        try:
            kv_store = get_kv(store)
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
        kv_store = kv_store_optional

    batch_size_expr = lit(batch_size)
    on_error_expr = lit(on_error)

    store_name = getattr(kv_store, "name", "")
    name_expr = lit(store_name)

    columns_expr = None
    if columns is not None:
        import json

        cols = columns if isinstance(columns, list) else [columns]
        columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        columns_expr = lit(columns_bytes)

    if columns_expr is None:
        return Expression._from_pyexpr(
            native.kv_batch_get_with_name(keys._expr, name_expr._expr, batch_size_expr._expr, on_error_expr._expr)
        )
    return Expression._from_pyexpr(
        native.kv_batch_get_with_name(
            keys._expr, name_expr._expr, batch_size_expr._expr, on_error_expr._expr, columns_expr._expr
        )
    )


def kv_exists(
    keys: Expression | str,
    on_error: Literal["raise", "null"] = "raise",
    store: str | None = None,
) -> Expression:
    """Check if keys exist in KV store."""
    from daft.expressions import lit
    from daft.session import current_kv, get_kv

    if isinstance(keys, str):
        keys = col(keys)

    if store is not None:
        try:
            kv_store = get_kv(store)
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
        kv_store = kv_store_optional

    store_name = getattr(kv_store, "name", "")
    name_expr = lit(store_name)
    return Expression._from_pyexpr(native.kv_exists_with_name(keys._expr, name_expr._expr))


def kv_get_with_config(
    keys: Expression | str,
    kv_config: dict[str, Any] | str,
    columns: list[str] | str | None = None,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Get data from KV store using row IDs and explicit configuration.

    This function retrieves data from a KV store with explicit configuration. If columns is specified,
    it returns a struct containing only the requested columns as top-level keys.
    """
    from daft.expressions import lit

    # Convert keys to Expression if it's a string
    if isinstance(keys, str):
        keys = col(keys)

    # Process KV configuration
    import json

    if isinstance(kv_config, dict):
        kv_config_dict = kv_config.copy()
    else:
        # Assume it's a JSON string
        kv_config_dict = json.loads(kv_config)

    # Enhance config with return type and requested columns
    kv_config_dict["return_type"] = "struct"
    kv_config_dict["requested_columns"] = (
        None if columns is None else (columns if isinstance(columns, list) else [columns])
    )

    # Serialize enhanced config
    enhanced_kv_config_json = json.dumps(kv_config_dict, ensure_ascii=False, separators=(",", ":"))
    kv_config_bytes = enhanced_kv_config_json.encode("utf-8")
    kv_config_expr = lit(kv_config_bytes)

    return Expression._from_pyexpr(native.kv_get_with_config(keys._expr, kv_config_expr._expr))


def kv_batch_get_with_config(
    keys: Expression | str,
    kv_config: dict[str, Any] | str,
    columns: list[str] | str | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Batch get operation for KV store with explicit configuration."""
    from daft.expressions import lit

    if isinstance(keys, str):
        keys = col(keys)

    # Process KV configuration
    import json

    if isinstance(kv_config, dict):
        kv_config_dict = kv_config.copy()
    else:
        # Assume it's a JSON string
        kv_config_dict = json.loads(kv_config)

    # Enhance config with return type and requested columns
    kv_config_dict["return_type"] = "struct"
    kv_config_dict["requested_columns"] = (
        None if columns is None else (columns if isinstance(columns, list) else [columns])
    )

    # Serialize enhanced config
    enhanced_kv_config_json = json.dumps(kv_config_dict, ensure_ascii=False, separators=(",", ":"))
    kv_config_bytes = enhanced_kv_config_json.encode("utf-8")
    kv_config_expr = lit(kv_config_bytes)
    batch_size_expr = lit(batch_size)

    return Expression._from_pyexpr(
        native.kv_batch_get_with_config(keys._expr, kv_config_expr._expr, batch_size_expr._expr)
    )


def kv_exists_with_config(
    keys: Expression | str,
    kv_config: dict[str, Any] | str,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Check if row IDs exist in KV store with explicit configuration."""
    from daft.expressions import lit

    if isinstance(keys, str):
        keys = col(keys)

    # Process KV configuration
    import json

    if isinstance(kv_config, dict):
        kv_config_bytes = json.dumps(kv_config, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    else:
        # Assume it's already a JSON string
        kv_config_bytes = kv_config.encode("utf-8")

    kv_config_expr = lit(kv_config_bytes)

    return Expression._from_pyexpr(native.kv_exists_with_config(keys._expr, kv_config_expr._expr))


def kv_put_with_config(
    key: Expression | str,
    value: Expression | Any,
    kv_config: dict[str, Any] | str,
) -> Expression:
    """Put key-value pairs into a KV store with explicit configuration.

    Args:
        key (Expression | str): Key column/expression or column name
        value (Expression | Any): Value column/expression or Python object
        kv_config (dict[str, Any] | str): KV configuration as dict or JSON string

    Returns:
        Expression: Binary result of the put operation
    """
    from daft.expressions import lit

    if isinstance(key, str):
        key = col(key)
    if not isinstance(value, Expression):
        value = lit(value)

    # Serialize config to JSON bytes if it's a dict
    if isinstance(kv_config, dict):
        import json

        kv_config_json = json.dumps(kv_config, ensure_ascii=False, separators=(",", ":"))
        kv_config_bytes = kv_config_json.encode("utf-8")
    else:
        # Assume it's already a JSON string
        kv_config_bytes = kv_config.encode("utf-8")

    kv_config_expr = lit(kv_config_bytes)
    return Expression._from_pyexpr(native.kv_put_with_config(key._expr, value._expr, kv_config_expr._expr))


def kv_get_with_store_name(
    keys: Expression | str,
    store_name: str,
    columns: list[str] | str | None = None,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Get data from KV store using row IDs and store name.

    This function retrieves data from a KV store specified by name. If columns is specified,
    it returns a struct containing only the requested columns as top-level keys.
    """
    from daft.expressions import lit
    from daft.session import get_kv

    # Convert keys to Expression if it's a string
    if isinstance(keys, str):
        keys = col(keys)

    try:
        _ = get_kv(store_name)
    except ValueError as e:
        raise ValueError(
            f"KV store '{store_name}' not found in session. "
            f"Please attach the KV store using daft.attach() first. Original error: {e}"
        )

    name_expr = lit(store_name)
    on_error_expr = lit(on_error)
    columns_expr = None
    if columns is not None:
        import json

        cols = columns if isinstance(columns, list) else [columns]
        columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        columns_expr = lit(columns_bytes)

    if columns_expr is None:
        return Expression._from_pyexpr(native.kv_get_with_name(keys._expr, name_expr._expr, on_error_expr._expr))
    return Expression._from_pyexpr(
        native.kv_get_with_name(keys._expr, name_expr._expr, on_error_expr._expr, columns_expr._expr)
    )


def kv_batch_get_with_store_name(
    keys: Expression | str,
    store_name: str,
    columns: list[str] | str | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Batch get operation for KV store using store name."""
    from daft.expressions import lit
    from daft.session import get_kv

    if isinstance(keys, str):
        keys = col(keys)

    batch_size_expr = lit(batch_size)
    try:
        _ = get_kv(store_name)
    except ValueError as e:
        raise ValueError(
            f"KV store '{store_name}' not found in session. "
            f"Please attach the KV store using daft.attach() first. Original error: {e}"
        )

    name_expr = lit(store_name)
    on_error_expr = lit(on_error)

    columns_expr = None
    if columns is not None:
        import json

        cols = columns if isinstance(columns, list) else [columns]
        columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        columns_expr = lit(columns_bytes)

    if columns_expr is None:
        return Expression._from_pyexpr(
            native.kv_batch_get_with_name(keys._expr, name_expr._expr, batch_size_expr._expr, on_error_expr._expr)
        )
    return Expression._from_pyexpr(
        native.kv_batch_get_with_name(
            keys._expr,
            name_expr._expr,
            batch_size_expr._expr,
            on_error_expr._expr,
            columns_expr._expr,
        )
    )


def kv_exists_with_store_name(
    keys: Expression | str,
    store_name: str,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Check if row IDs exist in KV store using store name."""
    from daft.expressions import lit
    from daft.session import get_kv

    if isinstance(keys, str):
        keys = col(keys)

    try:
        _ = get_kv(store_name)
    except ValueError as e:
        raise ValueError(
            f"KV store '{store_name}' not found in session. "
            f"Please attach the KV store using daft.attach() first. Original error: {e}"
        )

    name_expr = lit(store_name)
    return Expression._from_pyexpr(native.kv_exists_with_name(keys._expr, name_expr._expr))


def kv_put_with_store_name(
    key: Expression | str,
    value: Expression | Any,
    store_name: str,
) -> Expression:
    """Put key-value pairs into a KV store using store name.

    Args:
        key (Expression | str): Key column/expression or column name
        value (Expression | Any): Value column/expression or Python object
        store_name (str): Name of the KV store

    Returns:
        Expression: Struct with operation status and key
    """
    from daft.expressions import lit
    from daft.session import get_kv

    if isinstance(key, str):
        key = col(key)
    if not isinstance(value, Expression):
        value = lit(value)

    try:
        _ = get_kv(store_name)
    except ValueError as e:
        raise ValueError(
            f"KV store '{store_name}' not found in session. "
            f"Please attach the KV store using daft.attach() first. Original error: {e}"
        )

    name_expr = lit(store_name)
    return Expression._from_pyexpr(native.kv_put_with_name(key._expr, value._expr, name_expr._expr))


def kv_get_with_name(
    keys: Expression | str,
    name: str,
    columns: list[str] | str | None = None,
    on_error: Literal["raise", "null"] = "raise",
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
    name_expr = lit(name)
    on_error_expr = lit(on_error)

    # Optional columns selection
    if columns is not None:
        import json

        cols = columns if isinstance(columns, list) else [columns]
        columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        columns_expr = lit(columns_bytes)
        return Expression._from_pyexpr(
            native.kv_get_with_name(keys._expr, name_expr._expr, on_error_expr._expr, columns_expr._expr)
        )

    # All backends: use ScalarUDF path with name parameter
    return Expression._from_pyexpr(native.kv_get_with_name(keys._expr, name_expr._expr, on_error_expr._expr))


def kv_batch_get_with_name(
    keys: Expression | str,
    name: str,
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

    if columns is not None:
        import json

        cols = columns if isinstance(columns, list) else [columns]
        columns_bytes = json.dumps(cols, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        columns_expr = lit(columns_bytes)
        return Expression._from_pyexpr(
            native.kv_batch_get_with_name(
                keys._expr,
                name_expr._expr,
                batch_size_expr._expr,
                on_error_expr._expr,
                columns_expr._expr,
            )
        )

    # All backends: use ScalarUDF path with name parameter
    return Expression._from_pyexpr(
        native.kv_batch_get_with_name(keys._expr, name_expr._expr, batch_size_expr._expr, on_error_expr._expr)
    )


def kv_exists_with_name(
    keys: Expression | str,
    name: str,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Check if row IDs exist in KV store using store name."""
    from daft.expressions import lit

    if isinstance(keys, str):
        keys = col(keys)

    # Create expression for the store name
    name_expr = lit(name)

    # All backends: use ScalarUDF path with name parameter
    return Expression._from_pyexpr(native.kv_exists_with_name(keys._expr, name_expr._expr))


def kv_put_with_name(
    key: Expression | str,
    value: Expression | Any,
    name: str,
) -> Expression:
    """Put key-value pairs into a KV store using store name.

    Args:
        key (Expression | str): Key column/expression or column name
        value (Expression | Any): Value column/expression or Python object
        name (str): Name of the KV store

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
    return Expression._from_pyexpr(native.kv_put_with_name(key._expr, value._expr, name_expr._expr))
