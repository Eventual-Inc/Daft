from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

import daft
from daft import execution_config_ctx, from_pylist

if TYPE_CHECKING:
    import pathlib

import lance

from daft.dependencies import pa
from daft.io.lance.utils import distribute_fragments_balanced

logger = logging.getLogger(__name__)


class FragmentIndexHandler:
    """Handler for distributed scalar index creation on fragment batches."""

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

    def __call__(self, fragment_ids: list[int]) -> bool:
        """Process a batch of fragment IDs for scalar index creation."""
        logger.info(
            "Building distributed scalar index for fragments %s using create_scalar_index",
            fragment_ids,
        )

        self.lance_ds.create_scalar_index(
            column=self.column,
            index_type=self.index_type,
            name=self.name,
            replace=self.replace,
            fragment_uuid=self.fragment_uuid,
            fragment_ids=fragment_ids,
            **self.kwargs,
        )
        return True


def create_scalar_index_internal(
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = True,
    storage_options: dict[str, Any] | None = None,
    fragment_group_size: int | None = None,
    num_partitions: int | None = None,
    max_concurrency: int | None = None,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed scalar index creation.

    Supports INVERTED, FTS, and BTREE index types and runs as a 3-phase workflow:
    Phase 1: Fragment-parallel processing using Daft distributed execution
    Phase 2: Index metadata merging
    Phase 3: Atomic index creation and commit
    """
    if not column:
        raise ValueError("Column name cannot be empty")

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

    match index_type:
        case "INVERTED" | "FTS":
            if not pa.types.is_string(value_type) and not pa.types.is_large_string(value_type):
                raise TypeError(f"Column {column} must be string type for INVERTED or FTS index, got {value_type}")
        case "BTREE":
            if (
                not pa.types.is_integer(value_type)
                and not pa.types.is_floating(value_type)
                and not pa.types.is_string(value_type)
            ):
                raise TypeError(f"Column {column} must be numeric or string type for BTREE index, got {value_type}")
        case _:
            logger.warning(
                "Distributed indexing currently only supports 'INVERTED', 'FTS', and 'BTREE' index types, not '%s'. So we are falling back to single-threaded index creation.",
                index_type,
            )
            lance_ds.create_scalar_index(
                column=column,
                index_type=index_type,
                name=name,
                replace=replace,
                **kwargs,
            )
            return

    # Generate index name if not provided
    if name is None:
        name = f"{column}_{index_type.lower()}_idx"
    # Handle replace parameter - check for existing index with same name
    if not replace:
        existing_indices = []
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

    # Adjust fragment grouping size
    if fragment_group_size is None:
        fragment_group_size = 10
    elif fragment_group_size <= 0:
        raise ValueError("fragment_group_size must be positive")

    if fragment_group_size > len(fragment_ids_to_use) and fragment_ids_to_use:
        fragment_group_size = len(fragment_ids_to_use)
        logger.info(
            "Adjusted fragment_group_size to %d to match fragment count",
            fragment_group_size,
        )

    logger.info("Starting fragment-parallel processing and creating DataFrame with fragment batches")
    fragment_data = distribute_fragments_balanced(fragments, fragment_group_size)

    # Configure maximum concurrency for fragment batches
    if not fragment_data:
        logger.info("No fragments found for dataset at %s; skipping scalar index creation.", uri)
        return

    # Generate unique index ID
    index_id = str(uuid.uuid4())

    logger.info(
        "Starting distributed scalar index creation: column=%s, type=%s, name=%s, fragment_group_size=%s, max_concurrency=%s",
        column,
        index_type,
        name,
        fragment_group_size,
        max_concurrency,
    )

    handler_cls = daft.cls(
        FragmentIndexHandler,
        max_concurrency=max_concurrency,
    )
    handler = handler_cls(
        lance_ds=lance_ds,
        column=column,
        index_type=index_type,
        name=name,
        fragment_uuid=index_id,
        replace=replace,
        **kwargs,
    )

    with execution_config_ctx(maintain_order=False):
        if num_partitions is not None and num_partitions > 1:
            df = from_pylist(fragment_data).repartition(num_partitions)
        else:
            df = from_pylist(fragment_data)

        df = df.select(handler(df["fragment_ids"]))
        df.collect()

    logger.info("Starting index metadata merging by reloading dataset to get latest state")
    lance_ds = lance.LanceDataset(uri, storage_options=storage_options)
    lance_ds.merge_index_metadata(index_id, index_type)

    logger.info("Starting atomic index creation and commit")
    field_id = lance_ds.schema.get_field_index(column)
    index = lance.Index(
        uuid=index_id,
        name=name,
        fields=[field_id],
        dataset_version=lance_ds.version,
        fragment_ids=set(fragment_ids_to_use),
        index_version=0,
    )
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
