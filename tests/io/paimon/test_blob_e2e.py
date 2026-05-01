"""End-to-end test: Paimon BLOB column → Daft File objects → open/metadata.

Creates a Paimon table with a BLOB column, writes small video files into it,
reads back via daft.read_paimon(), and exercises Daft's File APIs (open,
file_path, video_metadata, etc.).
"""

from __future__ import annotations

import io
import tempfile

import av
import numpy as np
import pyarrow as pa
import pypaimon

import daft
from daft.functions import file_path


def _make_video_bytes(width: int = 64, height: int = 64, num_frames: int = 3) -> bytes:
    """Create a minimal MP4 video in memory."""
    buf = io.BytesIO()
    container = av.open(buf, mode="w", format="mp4")
    stream = container.add_stream("h264", rate=24)
    stream.width = width
    stream.height = height
    stream.pix_fmt = "yuv420p"

    for i in range(num_frames):
        arr = np.full((height, width, 3), fill_value=(i * 40) % 256, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(arr, format="rgb24")
        for packet in stream.encode(frame):
            container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()
    return buf.getvalue()


def main() -> None:
    video_a = _make_video_bytes(64, 64, 3)
    video_b = _make_video_bytes(32, 32, 5)
    print(f"Created video_a ({len(video_a)} bytes), video_b ({len(video_b)} bytes)")

    # ── 1. Create Paimon catalog & table with BLOB column ─────────────
    with tempfile.TemporaryDirectory() as warehouse:
        catalog = pypaimon.CatalogFactory.create({"warehouse": warehouse})
        catalog.create_database("test_db", ignore_if_exists=True)

        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("video", pa.large_binary()),
        ])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(
            pa_schema,
            options={
                "bucket": "1",
                "file.format": "parquet",
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "true",
            },
        )
        catalog.create_table("test_db.video_table", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.video_table")
        print("✓ Created Paimon table with BLOB column")

        # ── 2. Write video bytes via daft.write_paimon() ──────────────
        from daft.io.paimon.paimon_write import _patch_pypaimon_stats_for_complex_types
        _patch_pypaimon_stats_for_complex_types()

        df = daft.from_pydict({"id": [1, 2], "video": [video_a, video_b]})
        result = df.write_paimon(table, mode="append")
        print("✓ write_paimon() succeeded")
        result.show()

        # ── 3. Read back via daft.read_paimon() ───────────────────────
        read_df = daft.read_paimon(table).sort("id")

        # 3a. Schema check
        video_dtype = read_df.schema()["video"].dtype
        print(f"\n  video column dtype: {video_dtype}")
        assert str(video_dtype) == "File[Unknown]", f"Expected File[Unknown], got {video_dtype}"
        print("✓ BLOB column maps to File[Unknown]")

        # 3b. Materialize
        result_dict = read_df.to_pydict()
        assert result_dict["id"] == [1, 2]
        blob_refs = result_dict["video"]
        assert len(blob_refs) == 2
        print(f"✓ Got {len(blob_refs)} File objects")

        # ── 4. Inspect File objects ───────────────────────────────────
        for i, ref in enumerate(blob_refs):
            print(f"\n  File[{i}]:")
            print(f"    type     = {type(ref).__name__}")
            print(f"    path     = {ref.path}")
            print(f"    offset   = {ref.offset}")
            print(f"    length   = {ref.length}")
            assert isinstance(ref, daft.File), f"Expected daft.File, got {type(ref)}"
            assert ".blob" in ref.path, f"Expected .blob in path, got {ref.path}"
            assert ref.offset is not None
            assert ref.length is not None
        print("✓ File objects have valid path/offset/length")

        # ── 5. file_path() expression ─────────────────────────────────
        paths_df = read_df.select("id", file_path(daft.col("video")).alias("video_path")).to_pydict()
        print(f"\n  file_path() results: {paths_df['video_path']}")
        assert all(".blob" in p for p in paths_df["video_path"])
        print("✓ file_path() expression works")

        # ── 6. File.open() — read back raw bytes ─────────────────────
        ref_a = blob_refs[0]  # id=1 → video_a
        ref_b = blob_refs[1]  # id=2 → video_b
        with ref_a.open() as f:
            data_a = f.read()
        with ref_b.open() as f:
            data_b = f.read()

        print(f"\n  Read back: ref_a={len(data_a)} bytes, ref_b={len(data_b)} bytes")
        assert data_a == video_a, f"video_a mismatch: expected {len(video_a)}, got {len(data_a)}"
        assert data_b == video_b, f"video_b mismatch: expected {len(video_b)}, got {len(data_b)}"
        print("✓ File.open().read() returns exact original video bytes")

        # ── 7. Video metadata via daft.File.as_video().metadata() ─────
        print("\n--- Video metadata (per-object API) ---")
        vf_a = ref_a.as_video()
        meta_a = vf_a.metadata()
        print(f"  video_a metadata: {meta_a}")
        assert meta_a["width"] == 64
        assert meta_a["height"] == 64
        print("✓ as_video().metadata() works with byte-range File")

        vf_b = ref_b.as_video()
        meta_b = vf_b.metadata()
        print(f"  video_b metadata: {meta_b}")
        assert meta_b["width"] == 32
        assert meta_b["height"] == 32
        print("✓ as_video().metadata() works for second video")

        # ── 8. Video keyframes via daft.File.as_video().keyframes() ───
        print("\n--- Video keyframes ---")
        keyframes_a = list(vf_a.keyframes())
        print(f"  video_a keyframes: {len(keyframes_a)} frames")
        assert len(keyframes_a) > 0
        first_frame = keyframes_a[0]
        print(f"  first keyframe size: {first_frame.size}")
        print("✓ as_video().keyframes() works with byte-range File")

        # ── 9. Column-level video functions ───────────────────────────
        # Note: video_file() cast from File[Unknown] → File[Video] is not yet
        # supported at the expression level, so column-level video_metadata()
        # cannot be used directly on BLOB-sourced File columns. The per-object
        # API (steps 7-8) works because as_video() is a Python-level cast.
        print("\n--- Column-level video functions ---")
        print("  (skipped: File[Unknown] → File[Video] column cast not yet supported)")

    print("\n" + "=" * 60)
    print("ALL E2E CHECKS PASSED")
    print("=" * 60)


if __name__ == "__main__":
    main()
