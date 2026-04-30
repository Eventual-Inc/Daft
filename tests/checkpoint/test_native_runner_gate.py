"""Gate: checkpoint= on native runner must fail at plan-build time.

Checkpoint filtering depends on `KeyFilteringJoin` actors, which only exist
on the Ray runner. Using `checkpoint=` on the native runner is therefore
rejected early with a clear message.

Every reader that exposes ``checkpoint=`` routes through the shared helper
``daft.io._checkpoint.attach_checkpoint``, so testing the helper directly
plus a few representative readers gives us confidence the gate fires
across the whole reader surface.
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


def _make_checkpoint_config():
    return daft.CheckpointConfig(
        store=daft.CheckpointStore("s3://dummy/ckpt"),
        on="file_id",
    )


@pytest.mark.parametrize(
    ("write_method", "read_fn"),
    [
        ("write_parquet", daft.read_parquet),
        ("write_csv", daft.read_csv),
        ("write_json", daft.read_json),
    ],
)
def test_file_readers_reject_checkpoint_on_native_runner(write_method, read_fn):
    """File-based readers reject `checkpoint=` on the native runner.

    Parametrized over parquet/csv/json — all share the same
    `get_tabular_files_scan` + `attach_checkpoint` wiring.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input")
        os.makedirs(input_path)
        df = daft.from_pydict({"file_id": ["a"], "value": [1]})
        getattr(df, write_method)(input_path)

        with pytest.raises(ValueError, match="not supported on the native runner"):
            read_fn(input_path, checkpoint=_make_checkpoint_config())


def test_attach_checkpoint_helper_rejects_native_runner():
    """Direct-helper test, covering iceberg/hudi/lance/warc by proxy.

    All readers route through `attach_checkpoint`, which is what raises.
    Building real fixtures for catalog-based readers (iceberg/hudi/lance)
    is expensive and the gate behavior is identical, so we exercise the
    helper directly to pin the contract for those readers.
    """
    from daft.io._checkpoint import attach_checkpoint

    # Builder doesn't matter — the helper short-circuits on runner check
    # before touching it. Pass a sentinel that would crash if used.
    sentinel_builder = object()
    with pytest.raises(ValueError, match="not supported on the native runner"):
        attach_checkpoint(sentinel_builder, _make_checkpoint_config())


def test_attach_checkpoint_helper_passthrough_when_none():
    """Helper is a no-op when `checkpoint` is None.

    Must return the input builder unchanged regardless of runner.
    """
    from daft.io._checkpoint import attach_checkpoint

    sentinel_builder = object()
    assert attach_checkpoint(sentinel_builder, None) is sentinel_builder
