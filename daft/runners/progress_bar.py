from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.execution.execution_step import PartitionTask


def get_tqdm(use_ray_tqdm: bool) -> Any:
    # Choose the appropriate tqdm module depending on whether we need to use Ray's tqdm
    if use_ray_tqdm:
        from ray.experimental.tqdm_ray import tqdm
    else:
        from tqdm.auto import tqdm as _tqdm

        try:
            import sys

            from IPython import get_ipython

            ipython = get_ipython()

            # write to sys.stdout if in jupyter notebook
            # source: https://github.com/tqdm/tqdm/blob/74722959a8626fd2057be03e14dcf899c25a3fd5/tqdm/autonotebook.py#L14
            if ipython is not None and "IPKernelApp" in ipython.config:

                class tqdm(_tqdm):  # type: ignore[no-redef]
                    def __init__(self, *args, **kwargs):
                        kwargs = kwargs.copy()
                        if "file" not in kwargs:
                            kwargs["file"] = sys.stdout  # avoid the red block in IPython

                        super().__init__(*args, **kwargs)
            else:
                tqdm = _tqdm
        except ImportError:
            tqdm = _tqdm

    return tqdm


class ProgressBar:
    def __init__(self, use_ray_tqdm: bool, show_tasks_bar: bool = False, disable: bool = False) -> None:
        self.show_tasks_bar = show_tasks_bar
        self._maxinterval = 5.0

        self.use_ray_tqdm = use_ray_tqdm
        self.tqdm_mod = get_tqdm(use_ray_tqdm)

        self.pbars: dict[int, Any] = dict()
        self.disable = (
            disable
            or not bool(int(os.environ.get("RAY_TQDM", "1")))
            or not bool(int(os.environ.get("DAFT_PROGRESS_BAR", "1")))
        )

    def _make_new_bar(self, stage_id: int, name: str):
        if self.use_ray_tqdm:
            self.pbars[stage_id] = self.tqdm_mod(total=1, desc=name, position=len(self.pbars))
        else:
            self.pbars[stage_id] = self.tqdm_mod(
                total=1,
                desc=name,
                position=len(self.pbars),
                leave=False,
                mininterval=1.0,
                maxinterval=self._maxinterval,
            )

    def mark_task_start(self, step: PartitionTask[Any]) -> None:
        if self.disable:
            return
        if self.show_tasks_bar:
            if len(self.pbars) == 0:
                self._make_new_bar(-1, "Tasks")
            else:
                task_pbar = self.pbars[-1]
                task_pbar.total += 1

        stage_id = step.stage_id

        if stage_id not in self.pbars:
            name = step.name()
            self._make_new_bar(stage_id, name)
        else:
            pb = self.pbars[stage_id]
            pb.total += 1
            if hasattr(pb, "last_print_t"):
                dt = time.time() - pb.last_print_t
                if dt >= self._maxinterval:
                    pb.refresh()

    def mark_task_done(self, step: PartitionTask[Any]) -> None:
        if self.disable:
            return
        stage_id = step.stage_id
        self.pbars[stage_id].update(1)
        if self.show_tasks_bar:
            self.pbars[-1].update(1)

    def close(self) -> None:
        for p in self.pbars.values():
            if not self.use_ray_tqdm:
                p.clear()
            p.close()
            del p


# Progress Bar for local execution, should only be used in the native executor
class SwordfishProgressBar:
    def __init__(self) -> None:
        self._maxinterval = 5.0
        self.tqdm_mod = get_tqdm(False)
        self.pbars: dict[int, Any] = dict()
        self.bar_configs: dict[int, str] = dict()
        self.next_id = 0

    def make_new_bar(self, bar_format: str) -> int:
        pbar_id = self.next_id
        self.next_id += 1
        self.bar_configs[pbar_id] = bar_format
        return pbar_id

    def update_bar(self, pbar_id: int, message: str) -> None:
        if pbar_id not in self.pbars:
            if pbar_id not in self.bar_configs:
                raise ValueError(f"No bar configuration found for id {pbar_id}")
            bar_format = self.bar_configs[pbar_id]
            self.pbars[pbar_id] = self.tqdm_mod(
                bar_format=bar_format,
                position=pbar_id,
                leave=False,
                mininterval=1.0,
                maxinterval=self._maxinterval,
            )
            del self.bar_configs[pbar_id]
        self.pbars[pbar_id].set_description_str(message)

    def close_bar(self, pbar_id: int) -> None:
        if pbar_id in self.pbars:
            self.pbars[pbar_id].close()
            del self.pbars[pbar_id]

    def close(self) -> None:
        for p in self.pbars.values():
            p.close()
            del p
