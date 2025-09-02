"""KV Store functions for Daft.

This module provides key-value store operations as standalone functions,
following the session-based architecture pattern similar to daft.ai.provider.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from daft.expressions import Expression

from daft.expressions import Expression, col


def kv_get(
    row_ids: Expression | str,
    columns: list[str] | None = None,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression:
    """Get data from KV store using row IDs.

    This function retrieves data from a KV store. If source is specified,
    it will use the named KV store; otherwise, it uses the current session's active KV store.
    The KV store must be attached to the session before using this function.

    Args:
        row_ids: Expression or column name containing row IDs to retrieve
        columns: List of column names to retrieve. If None, returns all columns
        on_error: Error handling strategy - "raise" or "null"
        source: Optional name of the KV store to use. If None, uses current active KV store

    Returns:
        Expression: Expression containing the retrieved data

    Examples:
        >>> import daft
        >>> from daft.kv import load_kv
        >>> from daft.functions.kv import kv_get
        >>> # Attach multiple KV stores to session
        >>> embeddings_kv = load_kv("lance", name="embeddings", uri="s3://bucket/embeddings")
        >>> metadata_kv = load_kv("lance", name="metadata", uri="s3://bucket/metadata")
        >>> daft.attach(embeddings_kv, alias="lance_embeddings")
        >>> daft.attach(metadata_kv, alias="lance_metadata")
        >>> daft.set_kv("lance_embeddings")  # Set default
        >>> 
        >>> df = daft.from_pydict({"item_id": [0, 1, 2]})
        >>> # Use default KV store
        >>> df = df.with_column("embeddings", kv_get(df["item_id"]))
        >>> # Use specific KV store via source parameter
        >>> df = df.with_column("metadata", kv_get(df["item_id"], source="lance_metadata"))
    """
    from daft.expressions import lit
    from daft.session import current_kv, get_kv

    # Convert row_ids to Expression if it's a string
    if isinstance(row_ids, str):
        row_ids = col(row_ids)

    # Get KV store from session - either specified source or current active
    if source is not None:
        # Use specified KV source
        try:
            kv_store = get_kv(source)
        except ValueError as e:
            raise ValueError(
                f"KV store '{source}' not found in session. "
                f"Please attach the KV store using daft.attach() first. Original error: {e}"
            )
    else:
        # Use current active KV store
        kv_store = current_kv()
        if kv_store is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a source parameter."
            )

    # Get KV configuration from the selected KV store
    kv_config = kv_store.get_config()

    # Serialize KVConfig to JSON bytes for transmission to Rust
    kv_config_json = kv_config.to_json()
    kv_config_bytes = kv_config_json.encode("utf-8")

    # Create a binary literal expression containing the serialized KVConfig
    kv_config_expr = lit(kv_config_bytes)

    # Create additional parameter expressions (unused in current implementation)
    # columns_expr = lit(columns) if columns is not None else lit(None)
    # on_error_expr = lit(on_error)

    # Use the ScalarUDF convenience function
    import daft.daft as native

    return Expression._from_pyexpr(
        native.kv_get_with_config(row_ids._expr, kv_config_expr._expr)  # type: ignore[attr-defined]
    )


def kv_batch_get(
    row_ids: Expression | str,
    columns: list[str] | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression:
    """Batch get operation for KV store.

    This function retrieves data in batches from a KV store. If source is specified,
    it will use the named KV store; otherwise, it uses the current session's active KV store.
    The KV store must be attached to the session before using this function.

    Args:
        row_ids: Expression or column name containing row IDs to retrieve
        columns: List of column names to retrieve
        batch_size: Batch size for processing optimization
        on_error: Error handling strategy - "raise" or "null"
        source: Optional name of the KV store to use. If None, uses current active KV store

    Returns:
        Expression: Expression containing the retrieved data

    Examples:
        >>> import daft
        >>> from daft.kv import load_kv
        >>> from daft.functions.kv import kv_batch_get
        >>> # Attach multiple KV stores to session
        >>> embeddings_kv = load_kv("lance", name="embeddings", uri="s3://bucket/embeddings")
        >>> metadata_kv = load_kv("lance", name="metadata", uri="s3://bucket/metadata")
        >>> daft.attach(embeddings_kv, alias="lance_embeddings")
        >>> daft.attach(metadata_kv, alias="lance_metadata")
        >>> daft.set_kv("lance_embeddings")  # Set default
        >>> 
        >>> df = daft.from_pydict({"row_ids": [[0, 1], [2, 3]]})
        >>> # Use default KV store
        >>> df = df.with_column("embeddings", kv_batch_get("row_ids"))
        >>> # Use specific KV store via source parameter
        >>> df = df.with_column("metadata", kv_batch_get("row_ids", source="lance_metadata"))
    """
    from daft.expressions import lit
    from daft.session import current_kv, get_kv

    # Convert row_ids to Expression if it's a string
    if isinstance(row_ids, str):
        row_ids = col(row_ids)

    # Get KV store from session - either specified source or current active
    if source is not None:
        # Use specified KV source
        try:
            kv_store = get_kv(source)
        except ValueError as e:
            raise ValueError(
                f"KV store '{source}' not found in session. "
                f"Please attach the KV store using daft.attach() first. Original error: {e}"
            )
    else:
        # Use current active KV store
        kv_store = current_kv()
        if kv_store is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a source parameter."
            )

    # Get KV configuration from the selected KV store
    kv_config = kv_store.get_config()

    # Serialize KVConfig to JSON bytes for transmission to Rust
    kv_config_json = kv_config.to_json()
    kv_config_bytes = kv_config_json.encode("utf-8")

    # Create a binary literal expression containing the serialized KVConfig
    kv_config_expr = lit(kv_config_bytes)

    # Create additional parameter expressions for batch_size
    batch_size_expr = lit(batch_size)

    # Use the ScalarUDF convenience function
    import daft.daft as native

    return Expression._from_pyexpr(
        native.kv_batch_get_with_config(row_ids._expr, kv_config_expr._expr, batch_size_expr._expr)  # type: ignore[attr-defined]
    )


def kv_exists(
    row_ids: Expression | str,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression:
    """Check if row IDs exist in KV store.

    This function checks if row IDs exist in a KV store. If source is specified,
    it will use the named KV store; otherwise, it uses the current session's active KV store.
    The KV store must be attached to the session before using this function.

    Args:
        row_ids: Expression or column name containing row IDs to check
        on_error: Error handling strategy - "raise" or "null"
        source: Optional name of the KV store to use. If None, uses current active KV store

    Returns:
        Expression: Boolean expression indicating if row IDs exist

    Examples:
        >>> import daft
        >>> from daft.kv import load_kv
        >>> from daft.functions.kv import kv_exists
        >>> # Attach multiple KV stores to session
        >>> embeddings_kv = load_kv("lance", name="embeddings", uri="s3://bucket/embeddings")
        >>> metadata_kv = load_kv("lance", name="metadata", uri="s3://bucket/metadata")
        >>> daft.attach(embeddings_kv, alias="lance_embeddings")
        >>> daft.attach(metadata_kv, alias="lance_metadata")
        >>> daft.set_kv("lance_embeddings")  # Set default
        >>> 
        >>> df = daft.from_pydict({"item_id": [0, 1, 999]})
        >>> # Check existence in default KV store
        >>> df = df.with_column("exists_embeddings", kv_exists("item_id"))
        >>> # Check existence in specific KV store via source parameter
        >>> df = df.with_column("exists_metadata", kv_exists("item_id", source="lance_metadata"))
    """
    from daft.expressions import lit
    from daft.session import current_kv, get_kv

    # Convert row_ids to Expression if it's a string
    if isinstance(row_ids, str):
        row_ids = col(row_ids)

    # Get KV store from session - either specified source or current active
    if source is not None:
        # Use specified KV source
        try:
            kv_store = get_kv(source)
        except ValueError as e:
            raise ValueError(
                f"KV store '{source}' not found in session. "
                f"Please attach the KV store using daft.attach() first. Original error: {e}"
            )
    else:
        # Use current active KV store
        kv_store = current_kv()
        if kv_store is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a source parameter."
            )

    # Get KV configuration from the selected KV store
    kv_config = kv_store.get_config()

    # Serialize KVConfig to JSON bytes for transmission to Rust
    kv_config_json = kv_config.to_json()
    kv_config_bytes = kv_config_json.encode("utf-8")

    # Create a binary literal expression containing the serialized KVConfig
    kv_config_expr = lit(kv_config_bytes)

    # Create additional parameter expressions (unused in current implementation)
    # on_error_expr = lit(on_error)

    # Use the ScalarUDF convenience function
    import daft.daft as native

    return Expression._from_pyexpr(
        native.kv_exists_with_config(row_ids._expr, kv_config_expr._expr)  # type: ignore[attr-defined]
    )
