from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pathlib

    import lance

    from daft.dependencies import pa

logger = logging.getLogger(__name__)


def distribute_fragments_balanced(fragments: list[Any], concurrency: int) -> list[dict[str, list[int]]]:
    """Distribute fragments across workers using a balanced algorithm considering fragment sizes."""
    if not fragments:
        return [{"fragment_ids": []} for _ in range(concurrency)]

    # Get fragment information (ID and size)
    fragment_info = []
    for fragment in fragments:
        try:
            row_count = fragment.count_rows()
            fragment_info.append({"id": fragment.fragment_id, "size": row_count})
        except Exception as e:
            # If we can't get size info, use fragment_id as a fallback
            logger.warning(
                "Could not get size for fragment %s: %s. Using fragment_id as size estimate.",
                fragment.fragment_id,
                e,
            )
            fragment_info.append({"id": fragment.fragment_id, "size": fragment.fragment_id})

    # Sort fragments by size in descending order (largest first)
    # This helps with better load balancing using the greedy algorithm
    fragment_info.sort(key=lambda x: x["size"], reverse=True)

    # Initialize fragment groups for each worker
    fragment_group_list: list[list[int]] = [[] for _ in range(concurrency)]
    group_size_list = [0] * concurrency

    # Greedy assignment: assign each fragment to the worker with minimum workload
    for frag_info in fragment_info:
        # Find the worker with the minimum current workload
        min_workload_idx = min(range(concurrency), key=lambda i: group_size_list[i])

        # Assign fragment to this worker
        fragment_group_list[min_workload_idx].append(frag_info["id"])
        group_size_list[min_workload_idx] += frag_info["size"]

    # Log distribution statistics for debugging
    total_size = sum(frag_info["size"] for frag_info in fragment_info)
    logger.info(
        "Fragment distribution statistics: Total fragments=%d, Total size=%d, Workers=%d",
        len(fragment_info),
        total_size,
        concurrency,
    )

    for i, (batch, workload) in enumerate(zip(fragment_group_list, group_size_list)):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info("Worker %d: %d fragments, workload: %d (%d%%)", i, len(batch), workload, percentage)

    # Filter out empty batches (shouldn't happen with proper input validation)
    non_empty_batches = [{"fragment_ids": batch} for batch in fragment_group_list if batch]

    return non_empty_batches


def construct_lance_dataset(
    uri: str | pathlib.Path,
    version: object | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,
) -> lance.LanceDataset:
    """Construct a Lance dataset with common options."""
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    ds = lance.dataset(uri, storage_options=storage_options, version=version, **kwargs)

    effective_kwargs = {
        "storage_options": storage_options,
        "version": version,
    }
    effective_kwargs.update(kwargs or {})
    try:
        ds._lance_open_kwargs = effective_kwargs
    except Exception:
        pass
    return ds


def combine_filters_to_arrow(predicates: list[Any] | None) -> pa.compute.Expression | None:
    """Combine a list of Daft PyExpr predicates into a single Arrow Expression.

    Returns None if predicates is empty or None.
    """
    from daft.expressions import Expression

    if not predicates:
        return None
    combined = predicates[0]
    for pred in predicates[1:]:
        combined = combined & pred
    return Expression._from_pyexpr(combined).to_arrow_expr()


def ensure_arrow_schema(obj: Any) -> pa.Schema:
    """Ensure the given object is a PyArrow Schema.

    Accepts pa.Schema directly, or objects with a `.schema` attribute (e.g., pa.Table, pa.RecordBatch).
    Falls back gracefully for Daft Schema objects.
    """
    from daft.dependencies import pa
    from daft.logical.schema import Schema as DaftSchema

    if isinstance(obj, pa.Schema):
        return obj
    # pa.Table / pa.RecordBatch
    schema = getattr(obj, "schema", None)
    if isinstance(schema, pa.Schema):
        return schema
    # Daft Schema wrapper
    if isinstance(obj, DaftSchema):
        # Daft Schema stores underlying pyarrow schema at `_schema`
        pa_schema = getattr(obj, "_schema", None)
        if isinstance(pa_schema, pa.Schema):
            return pa_schema
    # Last resort: build from fields if present
    fields = getattr(obj, "fields", None)
    if fields is not None:
        try:
            return pa.schema(list(fields))
        except Exception:
            pass
    raise TypeError(f"Cannot ensure PyArrow Schema from object of type {type(obj).__name__}")


def select_required_columns(schema: pa.Schema, required_columns: list[str] | None) -> pa.Schema:
    """Return a new schema filtered to required columns, validating presence.

    If required_columns is None, returns the original schema unchanged.
    """
    from daft.dependencies import pa

    if required_columns is None:
        return schema
    missing = [c for c in required_columns if c not in schema.names]
    if missing:
        raise KeyError(f"Required columns missing in schema: {missing}")
    fields = [schema.field(schema.get_field_index(c)) for c in required_columns]
    return pa.schema(fields, metadata=schema.metadata)
