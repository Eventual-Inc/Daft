from __future__ import annotations

import logging
import uuid
from typing import Any

import lance
from lance.dataset import IndexConfig

from daft import DataType, from_pylist
from daft.dependencies import pa
from daft.udf import udf
from daft.udf.legacy import _UnsetMarker

logger = logging.getLogger(__name__)


@udf(
    return_dtype=DataType.struct(
        {
            "status": DataType.string(),
            "fragment_ids": DataType.list(DataType.int32()),
            "fields": DataType.list(DataType.int32()),
            "uuid": DataType.string(),
            "error": DataType.string(),
        }
    ),
    concurrency=1,
)
class FragmentIndexHandler:
    """UDF handler for distributed fragment index creation."""

    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        column: str,
        index_type: str,
        name: str,
        fragment_uuid: str,
        replace: bool,
        train: bool,
        **kwargs: Any,
    ) -> None:
        self.lance_ds = lance_ds
        self.column = column
        self.index_type = index_type
        self.name = name
        self.fragment_uuid = fragment_uuid
        self.replace = replace
        self.train = train
        self.kwargs = kwargs

    def __call__(self, fragment_ids_batch: list[list[int]]) -> list[dict[str, Any]]:
        """Process a batch of fragment IDs for index creation."""
        results: list[dict[str, Any]] = []
        for fragment_ids in fragment_ids_batch:
            try:
                results.append(self._handle_fragment_index(fragment_ids))
            except Exception as e:
                logger.exception("Error creating fragment index for fragment_ids %s: %s", fragment_ids, e)
                results.append({"status": "error", "fragment_ids": fragment_ids, "error": str(e)})
        return results

    def _handle_fragment_index(self, fragment_ids: list[int]) -> dict[str, Any]:
        """Handle index creation for a single fragment."""
        try:
            # Use the distributed index building API - Phase 1: Fragment index creation
            logger.info("Building distributed index for fragments %s using create_scalar_index", fragment_ids)

            # Call create_scalar_index directly - no return value expected
            # After execution, fragment-level indices are automatically built
            self.lance_ds.create_scalar_index(
                column=self.column,
                index_type=self.index_type,
                name=self.name,
                replace=self.replace,
                train=self.train,
                fragment_uuid=self.fragment_uuid,
                fragment_ids=fragment_ids,
                **self.kwargs,
            )

            field_id = self.lance_ds.schema.get_field_index(self.column)
            logger.info("Fragment index created successfully for fragments %s", fragment_ids)
            return {
                "status": "success",
                "fragment_ids": fragment_ids,
                "fields": [field_id],
                "uuid": self.fragment_uuid,
            }
        except Exception as e:
            logger.error("Fragment index task failed for fragments %s: %s", fragment_ids, str(e))
            return {
                "status": "error",
                "fragment_ids": fragment_ids,
                "error": str(e),
            }


def _distribute_fragments_balanced(fragments: list[Any], concurrency: int) -> list[dict[str, list[Any]]]:
    """Distribute fragments across workers using a balanced algorithm considering fragment sizes."""
    if not fragments:
        return [{"fragment_ids": []} for _ in range(concurrency)]

    # Get fragment information (ID and size)
    fragment_info = []
    for fragment in fragments:
        try:
            row_count = fragment.count_rows()
            fragment_info.append(
                {
                    "id": fragment.fragment_id,
                    "size": row_count,
                }
            )
        except Exception as e:
            # If we can't get size info, use fragment_id as a fallback
            logger.warning(
                "Could not get size for fragment %s: %s. " "Using fragment_id as size estimate.",
                fragment.fragment_id,
                e,
            )
            fragment_info.append(
                {
                    "id": fragment.fragment_id,
                    "size": fragment.fragment_id,  # Fallback to fragment_id
                }
            )

    # Sort fragments by size in descending order (largest first)
    # This helps with better load balancing using the greedy algorithm
    fragment_info.sort(key=lambda x: x["size"], reverse=True)

    # Initialize worker batches and their current workloads
    worker_batches: list[list[int]] = [[] for _ in range(concurrency)]
    worker_workloads = [0] * concurrency

    # Greedy assignment: assign each fragment to the worker with minimum workload
    for frag_info in fragment_info:
        # Find the worker with the minimum current workload
        min_workload_idx = min(range(concurrency), key=lambda i: worker_workloads[i])

        # Assign fragment to this worker
        worker_batches[min_workload_idx].append(frag_info["id"])
        worker_workloads[min_workload_idx] += frag_info["size"]

    # Log distribution statistics for debugging
    total_size = sum(frag_info["size"] for frag_info in fragment_info)
    logger.info(
        "Fragment distribution statistics:\n  Total fragments: %d\n  Total size: %d\n  Workers: %d",
        len(fragment_info),
        total_size,
        concurrency,
    )

    for i, (batch, workload) in enumerate(zip(worker_batches, worker_workloads)):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info("  Worker %d: %d fragments, " "workload: %d (%d%%)", i, len(batch), workload, percentage)

    # Filter out empty batches (shouldn't happen with proper input validation)
    non_empty_batches = [
        {
            "fragment_ids": batch,
        }
        for batch in worker_batches
        if batch
    ]

    return non_empty_batches


def _get_available_fragment_ids(
    lance_ds: lance.LanceDataset,
    fragment_ids: list[int] | None = None,
) -> tuple[list[Any], list[int]]:
    """Get available fragment IDs from a list of Lance fragments."""
    fragments = lance_ds.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")

    if fragment_ids is None:
        return fragments, [fragment.fragment_id for fragment in fragments]

    for fragment_id in fragment_ids:
        if fragment_id < 0 or fragment_id > 0xFFFFFFFF:
            raise ValueError(f"Invalid fragment_id: {fragment_id}")

    fragment_ids_set = set(fragment_ids)
    available_fragment_ids = {f.fragment_id for f in fragments}
    invalid_fragments = fragment_ids_set - available_fragment_ids
    if invalid_fragments:
        raise ValueError(f"Fragment IDs {invalid_fragments} do not exist in dataset")

    fragments = [f for f in fragments if f.fragment_id in fragment_ids]
    return fragments, list(fragment_ids_set)


def create_scalar_index_internal(
    lance_ds: lance.LanceDataset,
    uri: str,
    *,
    column: str,
    index_type: str | IndexConfig = "INVERTED",
    name: str | None = None,
    replace: bool = True,
    train: bool = True,
    fragment_ids: list[int] | None = None,
    fragment_uuid: str | None = None,
    storage_options: dict[str, str] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed FTS index creation using Daft UDFs.

    This function implements the 3-phase distributed indexing workflow:
    Phase 1: Fragment parallel processing using Daft UDFs
    Phase 2: Index metadata merging
    Phase 3: Atomic index creation and commit
    """
    if not column:
        raise ValueError("Column name cannot be empty")

    # Handle index_type validation
    if isinstance(index_type, str):
        valid_index_types = ["BTREE", "BITMAP", "LABEL_LIST", "INVERTED", "FTS", "NGRAM", "ZONEMAP"]
        if index_type not in valid_index_types:
            raise ValueError(f"Index type must be one of {valid_index_types}, not '{index_type}'")
        # For distributed indexing, currently only support text-based indexes
        if index_type not in ["INVERTED", "FTS"]:
            raise ValueError(
                f"Distributed indexing currently only supports 'INVERTED' and 'FTS' index types, not '{index_type}'"
            )
    elif not isinstance(index_type, IndexConfig):
        raise ValueError(f"index_type must be a string literal or IndexConfig object, got {type(index_type)}")

    # Validate column exists and has correct type
    try:
        field = lance_ds.schema.field(column)
    except KeyError as e:
        available_columns = [field.name for field in lance_ds.schema]
        raise ValueError(f"Column '{column}' not found. Available: {available_columns}") from e

    # Check column type
    value_type = field.type
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_type = field.type.value_type

    if not pa.types.is_string(value_type) and not pa.types.is_large_string(value_type):
        raise TypeError(f"Column {column} must be string type, got {value_type}")

    # Generate index name if not provided
    if name is None:
        name = f"{column}_idx"

    # Handle replace parameter - check for existing index with same name
    if not replace:
        try:
            existing_indices = lance_ds.list_indices()
            existing_names = {idx["name"] for idx in existing_indices}
            if name in existing_names:
                raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")
        except Exception:
            # If we can't check existing indices, continue
            pass

    # Get available fragment IDs to use
    fragments, fragment_ids_to_use = _get_available_fragment_ids(lance_ds, fragment_ids)

    # Adjust concurrency based on fragment count
    if concurrency is None:
        concurrency = 4

    if concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")

    if concurrency > len(fragment_ids_to_use):
        concurrency = len(fragment_ids_to_use)
        logger.info("Adjusted concurrency to %d to match fragment count", concurrency)

    # Generate unique index ID
    index_id = fragment_uuid or str(uuid.uuid4())

    logger.info(
        "Starting distributed FTS index creation: column=%s, type=%s, name=%s, concurrency=%s",
        column,
        index_type,
        name,
        concurrency,
    )

    # Get fragments to use
    # Phase 1: Fragment parallel processing using Daft UDFs
    logger.info("Phase 1: Starting fragment parallel processing")

    # Create DataFrame with fragment batches
    fragment_data = _distribute_fragments_balanced(fragments, concurrency)
    df = from_pylist(fragment_data)

    daft_remote_args = daft_remote_args or {}
    num_cpus = daft_remote_args.get("num_cpus", _UnsetMarker)
    num_gpus = daft_remote_args.get("num_gpus", _UnsetMarker)
    memory_bytes = daft_remote_args.get("memory_bytes", _UnsetMarker)
    batch_size = daft_remote_args.get("batch_size", _UnsetMarker)

    handler_fragment_udf = (
        FragmentIndexHandler.with_init_args(  # type: ignore[attr-defined]
            lance_ds=lance_ds,
            column=column,
            index_type=index_type,
            name=name,
            fragment_uuid=index_id,
            replace=replace,
            train=train,
            **kwargs,
        )
        .override_options(num_cpus=num_cpus, num_gpus=num_gpus, memory_bytes=memory_bytes, batch_size=batch_size)
        .with_concurrency(concurrency)
    )
    df = df.with_column("index_result", handler_fragment_udf(df["fragment_ids"]))

    results = df.to_pandas()["index_result"]

    # Check for failures
    failed_results = [r for r in results if r["status"] == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results]
        raise RuntimeError(
            f"Index building failed on {len(failed_results)} fragment batches: {'; '.join(error_messages)}"
        )

    successful_results = [r for r in results if r["status"] == "success"]
    if not successful_results:
        raise RuntimeError("No successful index building results")

    logger.info("Phase 1 completed successfully: %d fragment batches processed", len(successful_results))

    # Phase 2: Index metadata merging
    logger.info("Phase 2: Starting index metadata merging")

    # Reload dataset to get latest state
    lance_ds = lance.LanceDataset(uri, storage_options=storage_options)
    try:
        lance_ds.merge_index_metadata(index_id, index_type)
    except TypeError as e:
        # Handle older versions of Lance that don't support index type
        logger.warning("Lance version maybe not support index type, merging index metadata without type: %s", e)
        lance_ds.merge_index_metadata(index_id)

    logger.info("Phase 2 completed: Index metadata merged")

    # Phase 3: Atomic index creation and commit
    logger.info("Phase 3: Starting atomic index creation and commit")

    # Get field information from successful results
    fields = successful_results[0]["fields"]

    # Create index object
    index = lance.Index(
        uuid=index_id,
        name=name,
        fields=fields,
        dataset_version=lance_ds.version,
        fragment_ids=set(fragment_ids_to_use),
        index_version=0,
    )

    # Create and commit the index operation
    create_index_op = lance.LanceOperation.CreateIndex(
        new_indices=[index],
        removed_indices=[],
    )

    # Commit the index operation atomically
    lance.LanceDataset.commit(
        uri,
        create_index_op,
        read_version=lance_ds.version,
        storage_options=storage_options,
    )

    logger.info("Phase 3 completed: Index '%s' created successfully with ID %s", name, index_id)
