from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.series import Series
from daft.udf import func

from .constants import ASYNC_AWAIT_TIMEOUT_SECONDS

if TYPE_CHECKING:
    from collections.abc import Callable

    from ray.actor import ActorHandle

    from daft.dependencies import np
    from daft.expressions import Expression

    from .key_spec import KeySpec


def _group_row_indices_by_worker(input: Series, num_workers: int) -> list[tuple[int, "np.ndarray"]]:  # noqa: UP037
    from daft.dependencies import np

    hash_arr = input.hash().to_arrow()
    hash_np = hash_arr.to_numpy(zero_copy_only=False).astype(np.uint64, copy=False)
    worker_ids = (hash_np % np.uint64(num_workers)).astype(np.int64, copy=False)
    del hash_arr
    del hash_np

    row_order = np.argsort(worker_ids, kind="stable")
    worker_sorted = worker_ids[row_order]
    run_starts = np.flatnonzero(np.r_[True, worker_sorted[1:] != worker_sorted[:-1]])
    run_ends = np.r_[run_starts[1:], len(worker_sorted)]
    workers_present = worker_sorted[run_starts]
    return [
        (int(worker_id), row_order[int(start) : int(end)])
        for worker_id, start, end in zip(workers_present, run_starts, run_ends)
    ]


class KeyFilterActor:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.key_set: set[Any] = set()

    def add_keys(self, input_keys: list[Any]) -> None:
        self.key_set.update(input_keys)

    def filter(self, input_keys: "np.ndarray") -> "np.ndarray":  # noqa: UP037
        from daft.dependencies import np

        input_list = input_keys.tolist()
        bool_result = np.array([input_key not in self.key_set for input_key in input_list], dtype=bool)
        return np.packbits(bool_result)


def build_key_filter_predicate(
    num_workers: int,
    actor_handles: list[ActorHandle],
    key_spec: KeySpec,
) -> Callable[[Expression], Expression]:
    @func.batch(return_dtype=DataType.bool())
    async def key_filter(input: Series) -> Series:
        import asyncio
        import os

        from daft.dependencies import np

        num_rows = len(input)
        if num_rows > 0 and np.random.random() < 0.01:
            print(f"[PID={os.getpid()}] KeyFilter Batch Size: {num_rows}")

        if num_rows == 0:
            return Series.from_numpy(np.empty(0, dtype=bool))

        keys_np = key_spec.to_numpy(input)

        futures = []
        row_indices_list: list[np.ndarray] = []

        for worker_id, row_indices in _group_row_indices_by_worker(input, num_workers):
            actor = actor_handles[worker_id]
            keys_subset = keys_np[row_indices]

            futures.append(actor.filter.remote(keys_subset))
            row_indices_list.append(row_indices)

        try:
            packed_results = await asyncio.wait_for(
                asyncio.gather(*[asyncio.wrap_future(ref.future()) for ref in futures]),
                timeout=ASYNC_AWAIT_TIMEOUT_SECONDS,
            )
        except Exception as e:
            raise RuntimeError(f"KeyFilterActor filter failed: {e}") from e
        finally:
            del futures
            del keys_np

        final_result = np.full(num_rows, True, dtype=bool)

        for row_indices, packed_subset in zip(row_indices_list, packed_results):
            subset_len = len(row_indices)
            subset_mask = np.unpackbits(packed_subset)[:subset_len].astype(bool)
            final_result[row_indices] = subset_mask

        return Series.from_numpy(final_result)

    return key_filter
