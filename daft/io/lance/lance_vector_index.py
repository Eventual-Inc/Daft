from __future__ import annotations

import logging
import math
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pathlib
import lance
from lance.dataset import Index, LanceDataset
from lance.indices import IndicesBuilder

import daft
from daft import DataType, execution_config_ctx, from_pylist
from daft.io.lance.utils import distribute_fragments_balanced
from daft.udf import cls as daft_cls

if TYPE_CHECKING:
    from daft.dependencies import pa

logger = logging.getLogger(__name__)
# Vector index types supported by the distributed merge pipeline.
_VECTOR_INDEX_TYPES = {
    "IVF_FLAT",
    "IVF_PQ",
    "IVF_SQ",
    "IVF_HNSW_FLAT",
    "IVF_HNSW_PQ",
    "IVF_HNSW_SQ",
}
_PQ_INDEX_TYPES = {"IVF_PQ", "IVF_HNSW_PQ"}


def _normalize_index_type(index_type: Any) -> str:
    """Normalize index type to upper-case string and validate support.

    Parameters
    ----------
    index_type : str or enum-like
        Vector index type. Must be one of the precise distributed vector
        types supported by Lance.
    """
    if hasattr(index_type, "value") and isinstance(index_type.value, str):
        index_type_name = index_type.value.upper()
    elif isinstance(index_type, str):
        index_type_name = index_type.upper()
    else:  # pragma: no cover - defensive
        raise TypeError(
            "index_type must be a string or an enum-like object with a string 'value' "
            f"attribute, got {type(index_type)}"
        )
    if index_type_name not in _VECTOR_INDEX_TYPES:
        raise ValueError(
            "Distributed vector indexing only supports the following index types: "
            f"{sorted(_VECTOR_INDEX_TYPES)}, not '{index_type_name}'. "
            "For scalar indices (e.g., INVERTED, FTS, BTREE), please use create_scalar_index instead."
        )
    return index_type_name


def _validate_metric(metric: str) -> str:
    """Normalize and validate the distance metric string."""
    if not isinstance(metric, str):
        raise TypeError(f"Metric must be a string, got {type(metric)}")
    metric_lower = metric.lower()
    valid_metrics = {"l2", "cosine", "euclidean", "dot", "hamming"}
    if metric_lower not in valid_metrics:
        raise ValueError(f"Metric {metric} not supported. Valid: {sorted(valid_metrics)}")
    return metric_lower


def _merge_index_metadata_compat(
    dataset: LanceDataset,
    index_id: str,
    index_type: str,
    **kwargs: Any,
) -> None:
    """Call ``merge_index_metadata`` with backwards compatible signature."""
    try:
        return dataset.merge_index_metadata(index_id, index_type, batch_readhead=kwargs.get("batch_readhead"))
    except TypeError:
        return dataset.merge_index_metadata(index_id)


class VectorFragmentIndexHandler:
    """UDF handler for distributed vector index creation.

    This class encapsulates the worker-side logic for building vector indices
    on a subset of fragments. It mirrors the behavior of the Ray-based
    implementation in lance-ray, but is executed via Daft's distributed
    UDF engine instead.
    """

    def __init__(
        self,
        lance_ds: LanceDataset,
        column: str,
        index_type: str,
        name: str,
        index_uuid: str,
        replace: bool,
        metric: str,
        num_partitions: int | None,
        num_sub_vectors: int | None,
        ivf_centroids: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray,
        pq_codebook: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None,
        storage_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.lance_ds = lance_ds
        self.column = column
        self.index_type = index_type
        self.name = name
        self.index_uuid = index_uuid
        self.replace = replace
        self.metric = metric
        self.num_partitions = num_partitions
        self.num_sub_vectors = num_sub_vectors
        self.ivf_centroids = ivf_centroids
        self.pq_codebook = pq_codebook
        self.storage_options = storage_options
        self.kwargs = kwargs

    @daft.method(
        return_dtype=DataType.struct(
            {
                "status": DataType.string(),
                "fragment_ids": DataType.list(DataType.int64()),
                "fields": DataType.list(DataType.int32()),
                "uuid": DataType.string(),
                "error": DataType.string(),
            }
        )
    )
    def __call__(self, fragment_ids: list[int]) -> dict[str, Any]:
        """Create a vector index for a single fragment batch."""
        try:
            return self._handle_fragment_index(fragment_ids)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(
                "Error creating vector index for fragment_ids %s: %s",
                fragment_ids,
                exc,
            )
            return {
                "status": "error",
                "fragment_ids": fragment_ids or [],
                "fields": [],
                "uuid": self.index_uuid,
                "error": str(exc),
            }

    def _handle_fragment_index(self, fragment_ids: list[int]) -> dict[str, Any]:
        """Handle index creation for a single fragment batch."""
        if not fragment_ids:
            raise ValueError("fragment_ids cannot be empty")
        for fragment_id in fragment_ids:
            if fragment_id < 0 or fragment_id > 0xFFFFFFFF:
                raise ValueError(f"Invalid fragment_id: {fragment_id}")
        dataset = self.lance_ds
        available_fragments = {f.fragment_id for f in dataset.get_fragments()}
        invalid_fragments = set(fragment_ids) - available_fragments
        if invalid_fragments:
            raise ValueError(f"Fragment IDs {invalid_fragments} do not exist")
        logger.info(
            "Building distributed vector index for fragments %s using create_index",
            fragment_ids,
        )
        dataset.create_index(
            column=self.column,
            index_type=self.index_type,
            name=self.name,
            metric=self.metric,
            replace=self.replace,
            num_partitions=self.num_partitions,
            ivf_centroids=self.ivf_centroids,
            pq_codebook=self.pq_codebook,
            num_sub_vectors=self.num_sub_vectors,
            storage_options=self.storage_options,
            train=True,
            fragment_ids=fragment_ids,
            index_uuid=self.index_uuid,
            **self.kwargs,
        )
        # Prefer Lance's internal field IDs when available, but fall back to
        # the public schema-based field index for older versions.
        field_id: int
        try:
            lance_field = dataset.lance_schema.field(self.column)
            if lance_field is None:
                raise KeyError(self.column)
            field_id = lance_field.id()
        except Exception:  # pragma: no cover - defensive fallback
            field_id = dataset.schema.get_field_index(self.column)
        logger.info(
            "Fragment vector index created successfully for fragments %s",
            fragment_ids,
        )
        return {
            "status": "success",
            "fragment_ids": fragment_ids,
            "fields": [field_id],
            "uuid": self.index_uuid,
            "error": "",
        }


def create_vector_index_internal(
    lance_ds: LanceDataset,
    uri: str | pathlib.Path,
    *,
    column: str,
    index_type: str,
    name: str | None = None,
    replace: bool = True,
    metric: str = "l2",
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    ivf_num_partitions: int | None = None,
    num_sub_vectors: int | None = None,
    pq_codebook: pa.Array | pa.FixedSizeListArray | pa.FixedShapeTensorArray | None = None,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed vector index creation.

    This function implements a multi-phase distributed vector indexing
    workflow using Daft's UDF engine:
    Phase 1: Global IVF / PQ training (driver side)
    Phase 2: Fragment-parallel index building via UDFs
    Phase 3: Index metadata merging
    Phase 4: Atomic index creation and commit

    Note: The index_type parameter should already be normalized by the caller.
    """
    if not column:
        raise ValueError("Column name cannot be empty")
    index_type_name = index_type
    metric_lower = _validate_metric(metric)
    if name is None:
        name = f"{column}_idx"
    if not replace:
        try:
            existing_indices = lance_ds.list_indices()
        except Exception:
            existing_indices = []
        existing_names = {idx.get("name") for idx in existing_indices if isinstance(idx, dict)}
        if name in existing_names:
            raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")
    # Get available fragments.
    fragments = lance_ds.get_fragments()
    if not fragments:
        raise ValueError("Dataset contains no fragments")
    fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]
    # Adjust concurrency based on fragment count.
    if concurrency is None:
        concurrency = min(4, len(fragment_ids_to_use))
    if concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")
    if concurrency > len(fragment_ids_to_use):
        concurrency = len(fragment_ids_to_use)
        logger.info("Adjusted concurrency to %d to match fragment count", concurrency)
    index_id = str(uuid.uuid4())
    logger.info(
        "Starting distributed vector index creation: column=%s, type=%s, name=%s, metric=%s, concurrency=%s, ivf_num_partitions=%s",
        column,
        index_type_name,
        name,
        metric_lower,
        concurrency,
        ivf_num_partitions,
    )
    # Phase 1: global IVF / PQ training on the driver.
    logger.info("[Phase 1] Training IVF / PQ models on driver")
    builder = IndicesBuilder(lance_ds, column)
    num_rows = lance_ds.count_rows()
    dimension = builder.dimension
    computed_num_partitions = builder._determine_num_partitions(
        ivf_num_partitions,
        num_rows,
    )

    train_kwargs = {}
    if "sample_rate" in kwargs:
        train_kwargs["sample_rate"] = kwargs["sample_rate"]

    ivf_model = builder.train_ivf(
        distance_type=metric_lower,
        num_partitions=computed_num_partitions,
        **train_kwargs,
    )
    ivf_centroids_artifact = ivf_model.centroids
    num_partitions = ivf_model.num_partitions
    pq_codebook_artifact = pq_codebook
    if index_type_name in _PQ_INDEX_TYPES:
        computed_num_sub_vectors = builder._normalize_pq_params(
            num_sub_vectors,
            dimension,
        )
        pq_model = builder.train_pq(
            ivf_model,
            computed_num_sub_vectors,
            **train_kwargs,
        )
        pq_codebook_artifact = pq_model.codebook
        num_sub_vectors = computed_num_sub_vectors
    if ivf_centroids_artifact is None:
        raise ValueError("ivf_centroids must be trainable for IVF-based distributed vector indices")
    if index_type_name in _PQ_INDEX_TYPES and pq_codebook_artifact is None:
        raise ValueError("pq_codebook must be trainable for PQ-based distributed vector indices")
    # Phase 2: fragment-parallel index building via Daft UDFs.
    logger.info("[Phase 2] Distributing fragments to workers")
    fragment_group_size = max(1, math.ceil(len(fragment_ids_to_use) / concurrency))
    fragment_data = distribute_fragments_balanced(fragments, fragment_group_size)
    if not fragment_data:
        raise RuntimeError("No fragment batches were generated for vector indexing")
    df = from_pylist(fragment_data)
    daft_remote_args = daft_remote_args or {}
    num_gpus = daft_remote_args.get("num_gpus", 0)
    handler_cls = daft_cls(
        VectorFragmentIndexHandler,
        max_concurrency=concurrency,
        gpus=num_gpus or 0,
    )
    handler = handler_cls(
        lance_ds=lance_ds,
        column=column,
        index_type=index_type_name,
        name=name,
        index_uuid=index_id,
        replace=replace,
        metric=metric_lower,
        num_partitions=num_partitions,
        num_sub_vectors=num_sub_vectors,
        ivf_centroids=ivf_centroids_artifact,
        pq_codebook=pq_codebook_artifact,
        storage_options=storage_options,
        **kwargs,
    )
    logger.info("[Phase 2] Launching vector index UDF over fragment batches")
    target_partitions = min(concurrency, len(fragment_data))
    with execution_config_ctx(maintain_order=False):
        if target_partitions > 1:
            df = df.repartition(target_partitions)
        df = df.with_column("index_result", handler(df["fragment_ids"]))
        results = df.to_pandas()["index_result"]
    # Phase 3: check for failures and merge index metadata.
    failed_results = [r for r in results if r["status"] == "error"]
    if failed_results:
        error_messages = [r["error"] for r in failed_results]
        raise RuntimeError(
            f"Vector index building failed on {len(failed_results)} fragment batches: " + "; ".join(error_messages)
        )
    successful_results = [r for r in results if r["status"] == "success"]
    if not successful_results:
        raise RuntimeError("No successful vector index building results")
    logger.info("[Phase 3] Merging vector index metadata by reloading dataset to get latest state")
    lance_ds = LanceDataset(uri, storage_options=storage_options)
    _merge_index_metadata_compat(
        lance_ds,
        index_id,
        index_type=index_type_name,
        **kwargs,
    )
    # Phase 4: create and commit the final index.
    logger.info("[Phase 4] Creating and committing vector index '%s'", name)
    fields = successful_results[0]["fields"]
    index = Index(
        uuid=index_id,
        name=name,
        fields=fields,
        dataset_version=lance_ds.version,
        fragment_ids=set(fragment_ids_to_use),
        index_version=0,
    )
    create_index_op = lance.LanceOperation.CreateIndex(
        new_indices=[index],
        removed_indices=[],
    )
    LanceDataset.commit(
        uri,
        create_index_op,
        read_version=lance_ds.version,
        storage_options=storage_options,
    )
    logger.info("Vector index %s created successfully with ID %s", name, index_id)
