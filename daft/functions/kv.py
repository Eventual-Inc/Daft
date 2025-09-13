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
    columns: list[str] | str | None = None,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression:
    """Get data from KV store using row IDs.

    This function retrieves data from a KV store. If columns is specified,
    it returns a struct containing only the requested columns as top-level keys.
    If source is specified, it will use the named KV store; otherwise,
    it uses the current session's active KV store.

    Args:
        row_ids: Expression or column name containing row IDs to retrieve
        columns: Column names to retrieve. Can be:
            - None: Returns all columns (backward compatibility)
            - str: Single column name, returns struct with this column
            - List[str]: Multiple column names, returns struct with these columns as top-level keys
        on_error: Error handling strategy - "raise" or "null"
        source: Optional name of the KV store to use. If None, uses current active KV store

    Returns:
        Expression: Expression containing the retrieved data as Struct type
            - If columns is None: Returns Struct containing all available fields
            - If columns is specified: Returns Struct containing only the requested fields as top-level keys

    Examples:
        >>> import daft
        >>> from daft.kv import load_kv
        >>> from daft.functions.kv import kv_get
        >>> # Create KV stores with unique names for this doctest
        >>> import time
        >>> timestamp = str(int(time.time() * 1000000))  # Microsecond timestamp for uniqueness
        >>> embeddings_kv = load_kv("memory", name=f"embeddings_{timestamp}")
        >>> metadata_kv = load_kv("memory", name=f"metadata_{timestamp}")
        >>> daft.attach(embeddings_kv, alias=f"lance_embeddings_{timestamp}")
        >>> daft.attach(metadata_kv, alias=f"lance_metadata_{timestamp}")
        >>> daft.set_kv(f"lance_embeddings_{timestamp}")  # Set default
        >>>
        >>> df = daft.from_pydict({"item_id": [0, 1, 2]})
        >>> # All operations now return Struct type
        >>> df_struct_all = df.with_column("data", kv_get("item_id"))  # Returns struct with all fields
        >>> # Get specific columns as struct
        >>> df_struct = df.with_column(
        ...     "embedding_data", kv_get("item_id", ["embedding"], source=f"lance_embeddings_{timestamp}")
        ... )
        >>> # Extract field from struct
        >>> # df_final = df_struct.with_column("embedding", col("embedding_data").struct.get("embedding"))
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
        kv_store_optional = current_kv()
        if kv_store_optional is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a source parameter."
            )
        kv_store = kv_store_optional

    # Get KV configuration from the selected KV store
    kv_config = kv_store.get_config()

    # Process columns parameter and determine return type
    # UNIFIED STRUCT RETURN: Always return struct type for consistency
    return_type = "struct"
    if columns is None:
        # No columns specified: return struct with all available fields
        requested_columns = None  # Will be handled by Rust layer to include all fields
    else:
        # Specific columns requested: return struct with only these fields
        if isinstance(columns, str):
            requested_columns = [columns]
        else:
            requested_columns = list(columns)

    # Convert KV config to dictionary for modification
    import json

    kv_config_json = kv_config.to_json()
    kv_config_dict = json.loads(kv_config_json)

    # Add return type and requested columns to config
    kv_config_dict["return_type"] = return_type
    kv_config_dict["requested_columns"] = requested_columns

    # Enable column projection optimization for Lance backend if columns specified
    if columns is not None and "lance" in kv_config_dict and kv_config_dict["lance"] is not None:
        kv_config_dict["lance"]["columns"] = requested_columns

    # Serialize the enhanced KVConfig to JSON bytes for transmission to Rust
    enhanced_kv_config_json = json.dumps(kv_config_dict, ensure_ascii=False, separators=(",", ":"))
    kv_config_bytes = enhanced_kv_config_json.encode("utf-8")

    # Create a binary literal expression containing the serialized KVConfig
    kv_config_expr = lit(kv_config_bytes)

    # Use the ScalarUDF convenience function
    import daft.daft as native

    # The enhanced KV functions are now available and columns parameter is fully supported
    # The Rust layer (lance_take_rows) already implements column projection optimization
    # The columns parameter is passed through the KV configuration to the Rust layer

    return Expression._from_pyexpr(
        native.kv_get_with_config(row_ids._expr, kv_config_expr._expr)  # type: ignore[attr-defined]
    )


def kv_batch_get(
    row_ids: Expression | str,
    columns: list[str] | str | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression:
    """Batch get operation for KV store.

    This function retrieves data in batches from a KV store. If columns is specified,
    it returns a struct containing only the requested columns as top-level keys.
    If source is specified, it will use the named KV store; otherwise,
    it uses the current session's active KV store.

    Args:
        row_ids: Expression or column name containing row IDs to retrieve
        columns: Column names to retrieve. Can be:
            - None: Returns all columns (backward compatibility)
            - str: Single column name, returns struct with this column
            - List[str]: Multiple column names, returns struct with these columns as top-level keys
        batch_size: Batch size for processing optimization
        on_error: Error handling strategy - "raise" or "null"
        source: Optional name of the KV store to use. If None, uses current active KV store

    Returns:
        Expression: Expression containing the retrieved data as Struct type
            - If columns is None: Returns Struct containing all available fields
            - If columns is specified: Returns Struct containing only the requested fields as top-level keys

    Examples:
        >>> import daft
        >>> from daft.kv import load_kv
        >>> from daft.functions.kv import kv_batch_get
        >>> # Create KV stores with unique names for this doctest
        >>> import time
        >>> timestamp = str(int(time.time() * 1000000))  # Microsecond timestamp for uniqueness
        >>> embeddings_kv = load_kv("memory", name=f"embeddings_{timestamp}")
        >>> metadata_kv = load_kv("memory", name=f"metadata_{timestamp}")
        >>> daft.attach(embeddings_kv, alias=f"lance_embeddings_{timestamp}")
        >>> daft.attach(metadata_kv, alias=f"lance_metadata_{timestamp}")
        >>> daft.set_kv(f"lance_embeddings_{timestamp}")  # Set default
        >>>
        >>> df = daft.from_pydict({"row_ids": [[0, 1], [2, 3]]})
        >>> # Use default KV store
        >>> df = df.with_column("embeddings", kv_batch_get("row_ids"))
        >>> # Use specific KV store via source parameter
        >>> df = df.with_column("metadata", kv_batch_get("row_ids", source=f"lance_metadata_{timestamp}"))
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
        kv_store_optional = current_kv()
        if kv_store_optional is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a source parameter."
            )
        kv_store = kv_store_optional

    # Get KV configuration from the selected KV store
    kv_config = kv_store.get_config()

    # Process columns parameter and determine return type
    # UNIFIED STRUCT RETURN: Always return struct type for consistency
    return_type = "struct"
    if columns is None:
        # No columns specified: return struct with all available fields
        requested_columns = None  # Will be handled by Rust layer to include all fields
    else:
        # Specific columns requested: return struct with only these fields
        if isinstance(columns, str):
            requested_columns = [columns]
        else:
            requested_columns = list(columns)

    # Convert KV config to dictionary for modification
    import json

    kv_config_json = kv_config.to_json()
    kv_config_dict = json.loads(kv_config_json)

    # Add return type and requested columns to config
    kv_config_dict["return_type"] = return_type
    kv_config_dict["requested_columns"] = requested_columns

    # Enable column projection optimization for Lance backend if columns specified
    if columns is not None and "lance" in kv_config_dict and kv_config_dict["lance"] is not None:
        kv_config_dict["lance"]["columns"] = requested_columns

    # Serialize the enhanced KVConfig to JSON bytes for transmission to Rust
    enhanced_kv_config_json = json.dumps(kv_config_dict, ensure_ascii=False, separators=(",", ":"))
    kv_config_bytes = enhanced_kv_config_json.encode("utf-8")

    # Create a binary literal expression containing the serialized KVConfig
    kv_config_expr = lit(kv_config_bytes)

    # Create batch size expression
    batch_size_expr = lit(batch_size)

    # Use the ScalarUDF convenience function
    import daft.daft as native

    # The enhanced KV functions are now available and columns parameter is fully supported
    # The Rust layer (lance_take_rows) already implements column projection optimization
    # The columns parameter is passed through the KV configuration to the Rust layer

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
        >>> # Create KV stores with unique names for this doctest
        >>> import time
        >>> timestamp = str(int(time.time() * 1000000))  # Microsecond timestamp for uniqueness
        >>> embeddings_kv = load_kv("memory", name=f"embeddings_{timestamp}")
        >>> metadata_kv = load_kv("memory", name=f"metadata_{timestamp}")
        >>> daft.attach(embeddings_kv, alias=f"lance_embeddings_{timestamp}")
        >>> daft.attach(metadata_kv, alias=f"lance_metadata_{timestamp}")
        >>> daft.set_kv(f"lance_embeddings_{timestamp}")  # Set default
        >>>
        >>> df = daft.from_pydict({"item_id": [0, 1, 999]})
        >>> # Check existence in default KV store
        >>> df = df.with_column("exists_embeddings", kv_exists("item_id"))
        >>> # Check existence in specific KV store via source parameter
        >>> df = df.with_column("exists_metadata", kv_exists("item_id", source=f"lance_metadata_{timestamp}"))
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
        kv_store_optional = current_kv()
        if kv_store_optional is None:
            raise ValueError(
                "No KV store is currently attached to the session. "
                "Please attach a KV store using daft.attach() and set it as default using daft.set_kv(), "
                "or specify a source parameter."
            )
        kv_store = kv_store_optional

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
