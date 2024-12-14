import contextlib
import dataclasses
import logging
import os
from typing import Iterator
from unittest import mock

import memray
from memray._memray.stats import compute_statistics

import daft
from daft.context import get_context
from daft.execution.physical_plan import MaterializedPhysicalPlan

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class LazyMemrayStats:
    memray_stats: memray._stats.Stats | None

    def unwrap(self) -> memray._stats.Stats:
        assert self.memray_stats is not None
        return self.memray_stats


@contextlib.contextmanager
def track_memory(tmpdir) -> Iterator[LazyMemrayStats]:
    tracked = LazyMemrayStats(None)
    tmpfile = os.path.join(tmpdir, "memray.bin")

    with memray.Tracker(tmpfile):
        yield tracked

    stats = compute_statistics(tmpfile)
    tracked.memray_stats = stats


def df_to_tasks(df: daft.DataFrame) -> MaterializedPhysicalPlan:
    cfg = get_context().daft_execution_config
    physical_plan = df._builder.to_physical_plan_scheduler(cfg)

    return physical_plan.to_partition_tasks(
        psets={
            k: v.values()
            for k, v in get_context().get_or_create_runner()._part_set_cache.get_all_partition_sets().items()
        },
        actor_pool_manager=mock.Mock(),
        results_buffer_size=None,
    )
