# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING

from daft import runners

if TYPE_CHECKING:
    from daft.checkpoint import CheckpointConfig
    from daft.logical.builder import LogicalPlanBuilder


def attach_checkpoint(
    builder: "LogicalPlanBuilder",
    checkpoint: "CheckpointConfig | None",
) -> "LogicalPlanBuilder":
    """Attach a checkpoint config to the current scan's `Source` node.

    Shared by every reader that exposes ``checkpoint=``. The checkpoint
    rewrite rule operates on any `Source` node, so this attach point is
    reader-agnostic — `read_parquet`, `read_csv`, `read_iceberg`, etc.
    all use the same wiring.

    No-op when ``checkpoint`` is ``None``.

    Raises:
        ValueError: if called under the native runner — checkpoint
            filtering requires the Ray runner's `KeyFilteringJoin` actors.
    """
    if checkpoint is None:
        return builder
    if runners.get_or_infer_runner_type() == "native":
        raise ValueError(
            "checkpoint= is not supported on the native runner "
            "(single-process, no distributed actor infrastructure). "
            "Use the Ray runner: call daft.context.set_runner_ray() "
            "or set DAFT_RUNNER=ray."
        )
    return builder.with_checkpoint(checkpoint._inner)
