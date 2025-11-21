from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pathlib

import lance

from daft import DataType, from_pylist
from daft.dependencies import pa
from daft.io.lance.utils import distribute_fragments_balanced
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
    )
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
        **kwargs: Any,
    ) -> None:
        self.lance_ds = lance_ds
        self.column = column
        self.index_type = index_type
        self.name = name
        self.fragment_uuid = fragment_uuid
        self.replace = replace
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
            logger.info("Building distributed index for fragments %s using create_scalar_index", fragment_ids)

            self.lance_ds.create_scalar_index(
                column=self.column,
                index_type=self.index_type,
                name=self.name,
                replace=self.replace,
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


def create_scalar_index_internal(
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = True,
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
    if index_type not in ["INVERTED", "FTS"]:
        raise ValueError(
            f"Distributed indexing currently only supports 'INVERTED' and 'FTS' index types, not '{index_type}'"
        )

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
        name = f"{column}_{index_type.lower()}_idx"
    # Handle replace parameter - check for existing index with same name
    if not replace:
        try:
            existing_indices = lance_ds.list_indices()
        except Exception:
            # If we can't check existing indices, continue
            pass
        existing_names = {idx["name"] for idx in existing_indices}
        if name in existing_names:
            raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")

    # Get available fragment IDs to use
    fragments = lance_ds.get_fragments()
    fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]

    # Adjust concurrency based on fragment count
    if concurrency is None:
        concurrency = 4

    if concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")

    if concurrency > len(fragment_ids_to_use):
        concurrency = len(fragment_ids_to_use)
        logger.info("Adjusted concurrency to %d to match fragment count", concurrency)

    # Generate unique index ID
    index_id = str(uuid.uuid4())

    logger.info(
        "Starting distributed FTS index creation: column=%s, type=%s, name=%s, concurrency=%s",
        column,
        index_type,
        name,
        concurrency,
    )

    logger.info("Starting fragment parallel processing. And create DataFrame with fragment batches")
    fragment_data = distribute_fragments_balanced(fragments, concurrency)
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

    logger.info("Starting index metadata merging by reloading dataset to get latest state")
    lance_ds = lance.LanceDataset(uri, storage_options=storage_options)

    lance_ds.merge_index_metadata(index_id, index_type)

    logger.info("Starting atomic index creation and commit")
    fields = successful_results[0]["fields"]
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

    logger.info("Index %s created successfully with ID %s", name, index_id)
