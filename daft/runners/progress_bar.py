from __future__ import annotations

import os
from typing import Any

from tqdm.auto import tqdm

from daft.execution.execution_step import PartitionTask


class ProgressBar:
    def __init__(self, use_ray_tqdm: bool, disable: bool = False) -> None:
        print("use ray tqdm", use_ray_tqdm)
        self.use_ray_tqdm = use_ray_tqdm
        self.tqdm_mod = tqdm
        self.pbars: dict[int, tqdm] = dict()
        self.disable = (
            disable
            or not bool(int(os.environ.get("RAY_TQDM", "1")))
            or not bool(int(os.environ.get("DAFT_PROGRESS_BAR", "1")))
        )

    def _make_new_bar(self, stage_id: int, name: str):
        if self.use_ray_tqdm:
            self.pbars[stage_id] = self.tqdm_mod(total=1, desc=name, position=len(self.pbars))
        else:
            self.pbars[stage_id] = self.tqdm_mod(total=1, desc=name, position=len(self.pbars), leave=False)

    def mark_task_start(self, step: PartitionTask[Any]) -> None:
        if self.disable:
            return
        if len(self.pbars) == 0:
            self._make_new_bar(-1, "Tasks")
        else:
            task_pbar = self.pbars[-1]
            task_pbar.total += 1
            if self.use_ray_tqdm:
                task_pbar.refresh()

        stage_id = step.stage_id

        if stage_id not in self.pbars:
            name = "-".join(i.__class__.__name__ for i in step.instructions)
            self._make_new_bar(stage_id, name)

        else:
            pb = self.pbars[stage_id]
            pb.total += 1
            if self.use_ray_tqdm:
                pb.refresh()

    def mark_task_done(self, step: PartitionTask[Any]) -> None:
        if self.disable:
            return

        stage_id = step.stage_id
        self.pbars[stage_id].update(1)
        self.pbars[-1].update(1)

    def close(self) -> None:
        for p in self.pbars.values():
            p.close()
            del p
