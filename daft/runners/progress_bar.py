from __future__ import annotations

import os
from typing import Any


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

                class tqdm(_tqdm):  # type: ignore
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
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

    def _make_new_bar(self, bar_id: int, bar_name: str) -> None:
        if self.use_ray_tqdm:
            self.pbars[bar_id] = self.tqdm_mod(total=1, desc=bar_name, position=len(self.pbars))
        else:
            self.pbars[bar_id] = self.tqdm_mod(
                total=1,
                desc=bar_name,
                position=len(self.pbars),
                leave=False,
                mininterval=1.0,
                maxinterval=self._maxinterval,
            )

    def make_bar_or_update_total(self, bar_id: int, bar_name: str) -> None:
        if self.disable:
            return
        if self.show_tasks_bar:
            if len(self.pbars) == 0:
                self._make_new_bar(-1, "Tasks")
            else:
                task_pbar = self.pbars[-1]
                task_pbar.total += 1

        if bar_id not in self.pbars:
            self._make_new_bar(bar_id, bar_name)
            pb = self.pbars[bar_id]
        else:
            pb = self.pbars[bar_id]
            pb.total += 1

        if self.use_ray_tqdm and hasattr(pb, "_dump_state"):
            pb._dump_state(True)
        else:
            pb.refresh()

    def update_bar(self, bar_id: int) -> None:
        if self.disable:
            return
        self.pbars[bar_id].update(1)
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
        self.pbar: Any = None  # Single combined progress bar
        self.pbars: dict[int, str] = dict()  # pbar_id -> latest message
        self.bar_configs: dict[int, str] = dict()  # pbar_id -> name
        self.next_id = 0

    def make_new_bar(self, bar_format: str) -> int:
        pbar_id = self.next_id
        self.next_id += 1
        # Extract name from bar_format (e.g., "🗡️ 🐟 Name: {elapsed} {desc}")
        self.bar_configs[pbar_id] = bar_format.split(":")[0] if ":" in bar_format else bar_format
        return pbar_id

    def update_bar(self, pbar_id: int, message: str) -> None:
        self.pbars[pbar_id] = message

        # Create combined bar on first update
        if self.pbar is None:
            self.pbar = self.tqdm_mod(
                bar_format="[{elapsed}] {desc}",
                # leave=True keeps the progress bar visible after completion.
                # This avoids output corruption in Jupyter notebooks where clearing
                # the bar doesn't work reliably.
                leave=True,
                mininterval=1.0,
                maxinterval=self._maxinterval,
            )

        # Combine all messages
        combined = " | ".join(f"{self.bar_configs[pid]}: {msg}" for pid, msg in self.pbars.items() if msg)
        self.pbar.set_description_str(combined)

    def close_bar(self, pbar_id: int) -> None:
        # Don't remove - keep showing in combined display until close() is called
        pass

    def close(self) -> None:
        if self.pbar is not None:
            self.pbar.close()
            self.pbar = None
            print()  # Empty line for cleaner separation
