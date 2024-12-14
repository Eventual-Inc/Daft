import os
import tempfile
import uuid
from unittest import mock

import memray

from daft.execution.execution_step import Instruction
from daft.runners.ray_runner import build_partitions
from daft.table import MicroPartition


def run_wrapper_build_partitions(
    input_partitions: list[dict], instructions: list[Instruction]
) -> tuple[list[MicroPartition], str]:
    inputs = [MicroPartition.from_pydict(p) for p in input_partitions]
    tmpdir = tempfile.gettempdir()
    memray_path = os.path.join(tmpdir, f"memray-{uuid.uuid4()}.bin")
    with memray.Tracker(memray_path, native_traces=True, follow_fork=True):
        results = build_partitions(
            instructions,
            [mock.Mock() for _ in range(len(input_partitions))],
            *inputs,
        )
    return results[1:], memray_path
