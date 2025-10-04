from __future__ import annotations

import logging
from typing import Any

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
        "Fragment distribution statistics:\n  Total fragments: %d\n  Total size: %d\n  Workers: %d",
        len(fragment_info),
        total_size,
        concurrency,
    )

    for i, (batch, workload) in enumerate(zip(fragment_group_list, group_size_list)):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info("  Worker %d: %d fragments, " "workload: %d (%d%%)", i, len(batch), workload, percentage)

    # Filter out empty batches (shouldn't happen with proper input validation)
    non_empty_batches = [
        {
            "fragment_ids": batch,
        }
        for batch in fragment_group_list
        if batch
    ]

    return non_empty_batches
