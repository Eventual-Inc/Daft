# Multi-Source KV Store Access Guide

This guide demonstrates how to use Daft's multi-source KV store functionality to access different data sources within a single DataFrame operation.

## Overview

The multi-source expression functionality allows you to:
- Access multiple KV stores in a single DataFrame query
- Specify which KV store to use via the `source` parameter
- Combine data from different sources seamlessly
- Maintain backward compatibility with existing code

## Basic Usage

### Setting Up Multiple KV Stores

```python
import daft
from daft.kv import load_kv
from daft.functions.kv import kv_get, kv_batch_get, kv_exists

# Create multiple KV stores
embeddings_kv = load_kv("lance", name="embeddings", uri="s3://bucket/embeddings")
metadata_kv = load_kv("lance", name="metadata", uri="s3://bucket/metadata")
features_kv = load_kv("memory", name="features", initial_data={"item_1": "feature_data"})

# Attach to session with descriptive aliases
daft.attach(embeddings_kv, alias="lance_embeddings")
daft.attach(metadata_kv, alias="lance_metadata")
daft.attach(features_kv, alias="feature_store")

# Set default KV store (optional)
daft.set_kv("lance_embeddings")
```

### Using the Source Parameter

```python
# Create a DataFrame
df = daft.from_pydict({"item_id": ["item_1", "item_2", "item_3"]})

# Access different KV stores using source parameter
df_enriched = (df
    .with_column("embeddings", kv_get("item_id", source="lance_embeddings"))
    .with_column("metadata", kv_get("item_id", source="lance_metadata"))
    .with_column("features", kv_get("item_id", source="feature_store"))
)
```

## API Reference

### kv_get()

Retrieve data from a KV store using row IDs.

```python
kv_get(
    row_ids: Expression | str,
    columns: list[str] | None = None,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression
```

**Parameters:**
- `row_ids`: Expression or column name containing row IDs to retrieve
- `columns`: List of column names to retrieve. If None, returns all columns
- `on_error`: Error handling strategy - "raise" or "null"
- `source`: Optional name of the KV store to use. If None, uses current active KV store

**Examples:**
```python
# Use default KV store
df.with_column("data", kv_get("item_id"))

# Use specific KV store
df.with_column("embeddings", kv_get("item_id", source="lance_embeddings"))

# With specific columns and error handling
df.with_column("metadata", kv_get("item_id", source="metadata_store",
                                  columns=["name", "category"], on_error="null"))
```

### kv_batch_get()

Batch retrieve data from a KV store.

```python
kv_batch_get(
    row_ids: Expression | str,
    columns: list[str] | None = None,
    batch_size: int = 1000,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression
```

**Parameters:**
- `row_ids`: Expression or column name containing lists of row IDs to retrieve
- `columns`: List of column names to retrieve
- `batch_size`: Batch size for processing optimization
- `on_error`: Error handling strategy - "raise" or "null"
- `source`: Optional name of the KV store to use

**Examples:**
```python
# Batch data from default store
df.with_column("batch_data", kv_batch_get("item_ids"))

# Batch data from specific store with custom batch size
df.with_column("embeddings", kv_batch_get("item_ids", source="lance_embeddings",
                                          batch_size=500))
```

### kv_exists()

Check if row IDs exist in a KV store.

```python
kv_exists(
    row_ids: Expression | str,
    on_error: Literal["raise", "null"] = "raise",
    source: str | None = None,
) -> Expression
```

**Parameters:**
- `row_ids`: Expression or column name containing row IDs to check
- `on_error`: Error handling strategy - "raise" or "null"
- `source`: Optional name of the KV store to use

**Examples:**
```python
# Check existence in default store
df.with_column("exists", kv_exists("item_id"))

# Check existence in specific store
df.with_column("has_embeddings", kv_exists("item_id", source="lance_embeddings"))
```

## Advanced Usage Patterns

### Multi-Modal Data Processing

```python
# Process user and product data from different sources
df = daft.from_pydict({
    "user_id": ["user_001", "user_002"],
    "product_id": ["prod_101", "prod_102"]
})

# Enrich with data from multiple KV stores
df_enriched = (df
    # User data
    .with_column("user_embeddings", kv_get("user_id", source="user_embeddings"))
    .with_column("user_profile", kv_get("user_id", source="user_metadata"))
    .with_column("user_behavior", kv_get("user_id", source="behavioral_data"))

    # Product data
    .with_column("product_embeddings", kv_get("product_id", source="product_embeddings"))
    .with_column("product_info", kv_get("product_id", source="product_catalog"))

    # Existence checks
    .with_column("user_has_behavior", kv_exists("user_id", source="behavioral_data"))
    .with_column("product_available", kv_exists("product_id", source="inventory"))
)
```

### Conditional Data Access

```python
# Access different stores based on conditions
df = daft.from_pydict({
    "item_id": ["user_001", "product_101", "user_002"],
    "item_type": ["user", "product", "user"]
})

# Use different KV stores based on item type
df_conditional = (df
    .with_column("embeddings",
        daft.when(df["item_type"] == "user")
        .then(kv_get("item_id", source="user_embeddings"))
        .otherwise(kv_get("item_id", source="product_embeddings")))

    .with_column("metadata",
        daft.when(df["item_type"] == "user")
        .then(kv_get("item_id", source="user_profiles"))
        .otherwise(kv_get("item_id", source="product_catalog")))
)
```

### Error Handling and Fallbacks

```python
# Graceful error handling with fallbacks
df = daft.from_pydict({"item_id": ["item_1", "item_2", "missing_item"]})

# Try primary source, fall back to secondary if missing
df_with_fallback = (df
    .with_column("primary_data", kv_get("item_id", source="primary_store", on_error="null"))
    .with_column("secondary_data", kv_get("item_id", source="secondary_store", on_error="null"))
    .with_column("final_data",
        daft.when(df["primary_data"].is_not_null())
        .then(df["primary_data"])
        .otherwise(df["secondary_data"]))
)
```

## Best Practices

### 1. Descriptive Alias Names

Use descriptive aliases when attaching KV stores:

```python
# Good
daft.attach(embeddings_kv, alias="user_embeddings_v2")
daft.attach(metadata_kv, alias="product_catalog_latest")

# Avoid
daft.attach(embeddings_kv, alias="kv1")
daft.attach(metadata_kv, alias="store2")
```

### 2. Error Handling Strategy

Choose appropriate error handling based on your use case:

```python
# For critical data - fail fast
critical_data = kv_get("item_id", source="critical_store", on_error="raise")

# For optional data - continue with nulls
optional_data = kv_get("item_id", source="optional_store", on_error="null")
```

### 3. Performance Optimization

Use batch operations for multiple items:

```python
# Efficient for multiple items
df.with_column("batch_embeddings", kv_batch_get("item_ids", source="embeddings"))

# Less efficient for multiple items
df.with_column("single_embeddings", kv_get("item_id", source="embeddings"))
```

### 4. Resource Management

Clean up KV stores when done:

```python
# Detach unused stores
daft.detach_kv("temporary_store")

# Clear current KV if not needed
daft.set_kv(None)
```

## Migration Guide

### From Single KV Store

If you're currently using a single KV store:

```python
# Old approach
daft.set_kv("my_store")
df.with_column("data", kv_get("item_id"))

# New approach (backward compatible)
df.with_column("data", kv_get("item_id"))  # Still works
# Or explicitly specify source
df.with_column("data", kv_get("item_id", source="my_store"))
```

### Adding Multiple Sources

To add multiple KV sources to existing code:

```python
# 1. Attach additional KV stores
daft.attach(new_kv_store, alias="additional_data")

# 2. Use source parameter for new data
df = (df
    .with_column("existing_data", kv_get("item_id"))  # Uses current KV
    .with_column("new_data", kv_get("item_id", source="additional_data"))
)
```

## Troubleshooting

### Common Errors

1. **KV store not found**
   ```
   ValueError: KV store 'my_store' not found in session
   ```
   **Solution**: Ensure the KV store is attached with the correct alias:
   ```python
   daft.attach(my_kv, alias="my_store")
   ```

2. **No current KV store**
   ```
   ValueError: No KV store is currently attached to the session
   ```
   **Solution**: Either set a current KV store or use the source parameter:
   ```python
   daft.set_kv("my_store")  # Set current KV
   # OR
   kv_get("item_id", source="my_store")  # Use source parameter
   ```

3. **Missing data**
   ```
   DaftError: Key not found in KV store
   ```
   **Solution**: Use `on_error="null"` for graceful handling:
   ```python
   kv_get("item_id", source="my_store", on_error="null")
   ```

### Debug Tips

1. **List attached KV stores**:
   ```python
   # Check which stores are attached
   print(daft.has_kv("my_store"))  # Returns True/False
   ```

2. **Check current KV store**:
   ```python
   current = daft.current_kv()
   print(current.name if current else "No current KV set")
   ```

3. **Verify data existence**:
   ```python
   df.with_column("exists", kv_exists("item_id", source="my_store"))
   ```

## Examples Repository

For complete working examples, see:
- `examples/multi_source_kv_demo.py` - Comprehensive demonstration
- `tests/kv_store/test_multi_source_kv.py` - Unit tests with usage patterns

## Performance Considerations

- **Batch Operations**: Use `kv_batch_get()` for multiple items
- **Existence Checks**: Use `kv_exists()` before expensive `kv_get()` operations
- **Error Handling**: Use `on_error="null"` for optional data to avoid exceptions
- **Resource Cleanup**: Detach unused KV stores to free memory

## Conclusion

The multi-source KV store functionality provides a powerful and flexible way to access multiple data sources within Daft DataFrame operations. It maintains full backward compatibility while enabling complex multi-modal data processing workflows.
