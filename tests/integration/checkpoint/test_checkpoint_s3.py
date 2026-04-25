"""S3-backed checkpoint integration tests against MinIO."""

from __future__ import annotations

import os

import pytest

import daft
from daft import CheckpointStore
from daft.daft import CheckpointStatus

from .conftest import minio_create_bucket

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("DAFT_RUNNER") != "ray",
        reason="Checkpoint tests require Ray runner (KFJ actors)",
    ),
]


def test_checkpoint_s3_first_run(minio_io_config):
    """First run with S3-backed checkpoint should process all rows."""
    with minio_create_bucket(minio_io_config) as bucket:
        # Input data
        input_path = f"s3://{bucket}/input"
        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        checkpoint = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=checkpoint, on="file_id", io_config=minio_io_config)
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")

        row_count = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert row_count == 3, f"Expected 3 rows, got {row_count}"


def test_checkpoint_s3_second_run_skips(minio_io_config):
    """Second run with same S3 checkpoint should skip all previously-processed rows."""
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        # First run
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df1 = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config)
        df1.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count1 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count1 == 3

        # Second run — same checkpoint store, all keys already checkpointed
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count2 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count2 == 0, f"Expected 0 rows on re-run, got {count2}"


def test_checkpoint_s3_incremental_append(minio_io_config):
    """New data appended between runs should be processed; old data skipped.

    Run 1: input=[a,b,c] (1 file) → output 3 rows, all staged.
    Run 2: input=[a,b,c]+[d,e] (2 files, multi-partition) → output 2 rows (only d,e).
    Run 3: input=[a,b,c]+[d,e] (2 files) → output 0 rows (all already checkpointed).
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        # Run 1
        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config)
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        assert daft.read_parquet(output_path, io_config=minio_io_config).count_rows() == 3

        # Run 2: append [d,e] as a second file — source now has two
        # partitions ([a,b,c] + [d,e]), exercising multi-partition anti-join
        # and per-input checkpointing.
        daft.from_pydict({"file_id": ["d", "e"], "value": [4, 5]}).write_parquet(
            input_path, io_config=minio_io_config, write_mode="append"
        )
        source_df = daft.read_parquet(input_path, io_config=minio_io_config).sort("file_id")
        total_input = source_df.count_rows()
        assert total_input == 5, f"Expected 5 input rows, got {total_input}"
        source_keys = source_df.select("file_id").to_pydict()["file_id"]
        assert source_keys == ["a", "b", "c", "d", "e"], f"Expected all 5 keys in source, got {source_keys}"

        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        output2 = daft.read_parquet(output_path, io_config=minio_io_config).sort("file_id")
        count2 = output2.count_rows()
        assert count2 == 2, f"Run 2: expected 2 new rows, got {count2}"
        output2_keys = output2.select("file_id").to_pydict()["file_id"]
        assert output2_keys == ["d", "e"], f"Run 2: expected ['d', 'e'], got {output2_keys}"

        # Run 3: no new data
        ckpt3 = CheckpointStore(ckpt_prefix, minio_io_config)
        df3 = daft.read_parquet(input_path, checkpoint=ckpt3, on="file_id", io_config=minio_io_config)
        df3.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count3 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count3 == 0, f"Run 3: expected 0 rows, got {count3}"


def test_checkpoint_s3_duplicate_keys_filtered(minio_io_config):
    """Duplicate keys in source should all be filtered when already checkpointed."""
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        # Input has duplicate "a" rows
        daft.from_pydict({"file_id": ["a", "a", "b"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        # Run 1: all 3 rows processed (both "a" rows are new)
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config)
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count1 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count1 == 3, f"Run 1: expected 3 rows, got {count1}"

        # Run 2: both "a" rows and "b" should be filtered
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count2 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count2 == 0, f"Run 2: expected 0 rows (all dupes filtered), got {count2}"


def test_checkpoint_s3_lifecycle(minio_io_config):
    """Verify checkpoint lifecycle: checkpointed after pipeline, committed after mark_committed."""
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        daft.from_pydict({"file_id": ["a", "b"], "value": [1, 2]}).write_parquet(input_path, io_config=minio_io_config)

        checkpoint = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=checkpoint, on="file_id", io_config=minio_io_config)
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")

        # After pipeline: checkpoints should be in Checkpointed state
        ckpts = checkpoint.list_checkpoints()
        assert len(ckpts) > 0, "Expected at least one checkpoint"
        for c in ckpts:
            assert c.status == CheckpointStatus.Checkpointed, f"Expected Checkpointed, got {c.status}"

        # Files should be available
        files = checkpoint.get_checkpointed_files()
        assert len(files) > 0, "Expected at least one staged file"

        # Mark committed
        checkpoint.mark_committed([c.id for c in ckpts])

        # After commit: status should be Committed
        ckpts_after = checkpoint.list_checkpoints()
        for c in ckpts_after:
            assert c.status == CheckpointStatus.Committed, f"Expected Committed, got {c.status}"

        # Committed files are no longer returned by get_checkpointed_files
        files_after = checkpoint.get_checkpointed_files()
        assert len(files_after) == 0, f"Expected 0 files after commit, got {len(files_after)}"

        # But checkpointed keys should still be visible to anti-join (re-run skips)
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count == 0, f"Expected 0 rows after commit + re-run, got {count}"


def test_checkpoint_s3_empty_source(minio_io_config):
    """Empty source with checkpoint should produce 0 rows without error."""
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        # Write empty parquet
        daft.from_pydict({"file_id": [], "value": []}).write_parquet(input_path, io_config=minio_io_config)

        checkpoint = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=checkpoint, on="file_id", io_config=minio_io_config)
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count == 0, f"Expected 0 rows from empty source, got {count}"


def test_checkpoint_s3_int_key_column(minio_io_config):
    """Checkpoint with non-string (Int64) key column."""
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        daft.from_pydict({"record_id": [100, 200, 300], "data": ["x", "y", "z"]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        # Run 1: all rows
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="record_id", io_config=minio_io_config)
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count1 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count1 == 3, f"Run 1: expected 3 rows, got {count1}"

        # Run 2: all skipped
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="record_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count2 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count2 == 0, f"Run 2: expected 0 rows, got {count2}"


def test_checkpoint_s3_sinkless_collect(minio_io_config):
    """Sink-less plan checkpointing via the CheckpointTerminusNode root wrapper.

    Plans that don't end in a Daft-native write sink (e.g. just `.to_pydict()`
    after reading) should still stage keys and call `store.checkpoint()` so
    that re-runs skip already-processed rows.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        # Run 1: collect all rows, keys staged and checkpoint finalized on task completion.
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        result1 = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).to_pydict()
        assert len(result1["file_id"]) == 3, f"Run 1: expected 3 rows, got {len(result1['file_id'])}"
        assert sorted(result1["file_id"]) == ["a", "b", "c"]

        # Run 2: same checkpoint, anti-join skips all rows.
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        result2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config).to_pydict()
        assert len(result2["file_id"]) == 0, (
            f"Run 2: expected 0 rows (all already checkpointed from sink-less run), got {len(result2['file_id'])}"
        )


def test_checkpoint_s3_sinkless_stages_once_per_morsel(minio_io_config):
    """Regression: CheckpointTerminusNode must not re-stage what SCKO already staged.

    SCKO (the optimizer-inserted StageCheckpointKeys node) stages keys on every
    morsel. CheckpointTerminusNode wraps sink-less plans and its sole job is to
    seal via store.checkpoint(id) on Flush. An earlier version also called
    store.stage_keys on every morsel, duplicating SCKO's work and producing 2x
    the keys/*.parquet files. This guards against that recurring.
    """
    import s3fs

    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        _ = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).to_pydict()

        fs = s3fs.S3FileSystem(
            key=minio_io_config.s3.key_id,
            password=minio_io_config.s3.access_key,
            client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
        )
        checkpoints = ckpt.list_checkpoints()
        assert len(checkpoints) > 0, "Expected >=1 checkpoint entry"
        for c in checkpoints:
            key_files = fs.ls(f"{bucket}/checkpoints/{c.id}/keys/")
            assert len(key_files) == 1, (
                f"Expected 1 keys/*.parquet per id (SCKO stages once, Terminus does not stage); "
                f"got {len(key_files)} for id={c.id}. Double-staging regression?"
            )


def test_checkpoint_s3_unknown_key_column(minio_io_config):
    """Fail at plan build time if `on=` names a column that doesn't exist.

    Early failure (FieldNotFound) is the contract — we don't silently accept
    a bogus key and then blow up mid-pipeline.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b"], "value": [1, 2]}).write_parquet(input_path, io_config=minio_io_config)

        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="nonexistent", io_config=minio_io_config)
        with pytest.raises(Exception, match="not found"):
            df.to_pydict()


def test_checkpoint_s3_rejects_shuffle_downstream(minio_io_config):
    """Reject checkpoint plans with shuffle ops between source and sink.

    Shuffle/materialization operators (aggregate, sort, distinct, etc.) would
    re-order or drop the key column before it reaches SCKO or the terminus, so
    the optimizer rejects them with a clear error.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = (
            daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config)
            .groupby("file_id")
            .agg(daft.col("value").sum())
        )
        with pytest.raises(Exception, match="map-only"):
            df.to_pydict()


def test_checkpoint_s3_rejects_limit_downstream(minio_io_config):
    """Limit between source and sink is rejected.

    After run 1 checkpoints the first N keys, run 2's anti-join drops them; Limit then
    picks a different set of rows, producing a non-deterministic cumulative
    output across runs. Reject at optimizer time.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).limit(2)
        with pytest.raises(Exception, match="Limit"):
            df.to_pydict()


def test_checkpoint_s3_rejects_sample_downstream(minio_io_config):
    """Sample between source and sink is rejected (non-deterministic)."""
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [1, 2, 3]}).write_parquet(
            input_path, io_config=minio_io_config
        )
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).sample(0.5)
        with pytest.raises(Exception, match="Sample"):
            df.to_pydict()


def test_checkpoint_s3_rejects_sort_downstream(minio_io_config):
    """Sort between source and sink should also be rejected (another map-breaker)."""
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [3, 1, 2]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).sort("value")
        with pytest.raises(Exception, match="map-only"):
            df.to_pydict()


def test_checkpoint_s3_filter_between_source_and_sink(minio_io_config):
    """Filter between source and sink must not drop keys from the checkpoint.

    Per the Checkpoint V2 design, task completion marks the source keys as
    processed — regardless of whether any rows survived a downstream filter.
    If keys were only staged for surviving rows, expensive filters would
    re-evaluate the dropped rows every run.

    Run 1: read 3 rows, filter keeps 1, write 1 row. All 3 keys should be checkpointed.
    Run 2: re-run with same input — anti-join should skip all 3 rows.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [10, 200, 30]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        # Run 1: only 'b' (value=200) survives the filter
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).filter(
            daft.col("value") > 100
        )
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count1 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count1 == 1, f"Run 1: expected 1 row, got {count1}"

        # Run 2: same pipeline, all 3 source keys should already be checkpointed
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config).filter(
            daft.col("value") > 100
        )
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count2 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count2 == 0, (
            f"Run 2: expected 0 rows (all 3 source keys checkpointed, including filtered-out 'a' and 'c'), got {count2}"
        )


def test_checkpoint_s3_filtered_out_keys_are_checkpointed(minio_io_config):
    """Filtered-out rows' keys must be checkpointed, even when the predicate is pushdownable.

    Distinguishes "keys actually checkpointed" from "predicate drops them again on re-run":

    Run 1: filter `value > 100`, only 'b' survives — write 1 row.
    Run 2: *no filter* — if all 3 keys were checkpointed in Run 1, anti-join
           drops everything and we write 0 rows. If only the surviving key 'b'
           was checkpointed (i.e. PushDownFilter had shoved `value > 100` into
           the Parquet scan and 'a'/'c' never reached SCKO), Run 2 would write
           2 rows.

    PushDownFilter must skip checkpointed sources so SCKO sees every row
    and checkpoints every source key.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [10, 200, 30]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        # Run 1: only 'b' passes `value > 100`, but all 3 keys should be checkpointed.
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).filter(
            daft.col("value") > 100
        )
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count1 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count1 == 1, f"Run 1: expected 1 row, got {count1}"

        # Run 2: remove the filter. If all 3 keys are checkpointed, anti-join returns empty.
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count2 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count2 == 0, (
            f"Run 2 (no filter): expected 0 rows (all 3 keys checkpointed in Run 1), got {count2}. "
            f"If count2 == 2, the pushdownable filter dropped 'a'/'c' at the Parquet scan "
            f"before SCKO could stage their keys — PushDownFilter needs to skip checkpointed sources."
        )


def test_checkpoint_s3_select_without_key_column(minio_io_config):
    """Projection pushdown must not strip the `on=` column from a checkpointed scan.

    When the user's query doesn't reference the key (e.g. `.select('value')`),
    naive `PushDownProjection` would set `Source.pushdowns.columns = ['value']`,
    stripping `file_id` from the output schema. Then `RewriteCheckpointSource`
    calls `source.output_schema.get_field('file_id')` and errors.

    Run 1: `.select('value')` writes all 3 `value`s. Query must not error.
    Run 2: no project — all 3 keys should be checkpointed, anti-join returns empty,
           we write 0 rows.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [10, 20, 30]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).select("value")
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count1 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count1 == 3, f"Run 1: expected 3 rows written, got {count1}"

        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count2 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count2 == 0, f"Run 2 (no project): expected 0 rows (all 3 keys checkpointed in Run 1), got {count2}"


def test_checkpoint_s3_select_with_key_column(minio_io_config):
    """Projection pushdown should still prune non-key columns on a checkpointed scan.

    Complement of `test_checkpoint_s3_select_without_key_column`: when the user's
    query keeps the `on=` column, projection pushdown is free to fire — it prunes
    `extra` from the scan but keeps `file_id` for the anti-join. Verify the query
    runs correctly, the pruned output has only the user-requested columns, and
    all 3 source keys are checkpointed.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input_path = f"s3://{bucket}/input"
        ckpt_prefix = f"s3://{bucket}/checkpoints"
        output_path = f"s3://{bucket}/output"

        daft.from_pydict({"file_id": ["a", "b", "c"], "value": [10, 20, 30], "extra": ["x", "y", "z"]}).write_parquet(
            input_path, io_config=minio_io_config
        )

        # Run 1: select ['file_id', 'value'] — prunes 'extra'. Pushdown should fire.
        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df = daft.read_parquet(input_path, checkpoint=ckpt, on="file_id", io_config=minio_io_config).select(
            "file_id", "value"
        )
        df.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        written = daft.read_parquet(output_path, io_config=minio_io_config)
        assert written.count_rows() == 3
        assert set(written.schema().column_names()) == {"file_id", "value"}, (
            f"Run 1: expected output schema {{'file_id', 'value'}}, got {written.schema().column_names()}"
        )

        # Run 2: no project — all 3 keys checkpointed, anti-join empty, write 0 rows.
        ckpt2 = CheckpointStore(ckpt_prefix, minio_io_config)
        df2 = daft.read_parquet(input_path, checkpoint=ckpt2, on="file_id", io_config=minio_io_config)
        df2.write_parquet(output_path, io_config=minio_io_config, write_mode="overwrite")
        count2 = daft.read_parquet(output_path, io_config=minio_io_config).count_rows()
        assert count2 == 0, f"Run 2: expected 0 rows, got {count2}"


def test_checkpoint_s3_rejects_concat_with_checkpoint(minio_io_config):
    """Reject concat/union pipelines with a checkpointed source.

    Concat is streaming (not a shuffle), but multi-source checkpointing is
    not yet designed — one SCKO at the sink can't tell which rows belong to
    which source's key space. Block it at optimizer time until the API is
    designed.
    """
    with minio_create_bucket(minio_io_config) as bucket:
        input1 = f"s3://{bucket}/input1"
        input2 = f"s3://{bucket}/input2"
        ckpt_prefix = f"s3://{bucket}/checkpoints"

        daft.from_pydict({"file_id": ["a", "b"], "value": [1, 2]}).write_parquet(input1, io_config=minio_io_config)
        daft.from_pydict({"file_id": ["c", "d"], "value": [3, 4]}).write_parquet(input2, io_config=minio_io_config)

        ckpt = CheckpointStore(ckpt_prefix, minio_io_config)
        df1 = daft.read_parquet(input1, checkpoint=ckpt, on="file_id", io_config=minio_io_config)
        df2 = daft.read_parquet(input2, io_config=minio_io_config)
        with pytest.raises(Exception, match="map-only"):
            df1.concat(df2).to_pydict()
