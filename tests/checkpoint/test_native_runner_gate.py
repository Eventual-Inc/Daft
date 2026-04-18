"""Gate: checkpoint= on native runner must fail at plan-build time.

Checkpoint filtering depends on `KeyFilteringJoin` actors, which only exist
on the Ray runner. Using `checkpoint=` on the native runner is therefore
rejected early with a clear message.
"""

from __future__ import annotations

import os
import tempfile

import pytest

import daft

# Only meaningful on the native runner — when DAFT_RUNNER=ray, the gate
# does not fire (Ray supports checkpointing).
pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") == "ray",
    reason="Native-runner gate does not fire on the Ray runner",
)


def test_read_parquet_with_checkpoint_rejects_native_runner():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input")
        os.makedirs(input_path)
        daft.from_pydict({"file_id": ["a"], "value": [1]}).write_parquet(input_path)

        with pytest.raises(ValueError, match="not supported on the native runner"):
            daft.read_parquet(
                input_path,
                checkpoint=daft.CheckpointStore("s3://dummy/ckpt"),
                on="file_id",
            )
