#!/usr/bin/env python3
"""Upload DROID scene classifications to Hugging Face.

Creates a public dataset repo with ``scene_classifications.parquet`` (written by
Daft) and a dataset card that includes CC-BY 4.0 attribution to the DROID
authors.

Usage:
    export HF_TOKEN=hf_...   # https://huggingface.co/settings/tokens
    python scripts/upload_droid_scene_classifications.py

    # Custom org/repo:
    python scripts/upload_droid_scene_classifications.py \\
        --repo-id my-org/droid-scene-classifications

    # Preview files without uploading:
    python scripts/upload_droid_scene_classifications.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import daft
from daft.datatype import DataType
from daft.expressions import col

DEFAULT_SOURCE_ZIP_URL = (
    "https://github.com/droid-dataset/droid/files/15068147/DROID_scene_classification.zip"
)
DEFAULT_REPO_ID = "Eventual-Inc/droid-scene-classifications"
DEFAULT_FILENAME = "scene_classifications.parquet"

README = """\
---
license: cc-by-4.0
task_categories:
- robotics
language:
- en
tags:
- droid
- robotics
- scene-classification
- manipulation
pretty_name: DROID Scene Classifications
size_categories:
- 1K<n<10K
---

# DROID Scene Classifications

GPT-4V scene classification labels for the [DROID](https://droid-dataset.github.io/) robot manipulation dataset.

This is a mirror of the supplemental classification table released by the DROID authors, converted to Parquet by Daft for efficient reads. It is intended for joining onto DROID episode metadata via ``scene_id``.

## Dataset

| Column | Type | Description |
|--------|------|-------------|
| ``scene_id`` | int64 | Unique robot workspace identifier from DROID metadata |
| ``scene_classification`` | string | GPT-4V scene label (e.g. ``Industrial office``, ``Home kitchen``) |

## Usage with Daft

```python
import daft
from daft.datasets.droid import filter_scenes, raw

kitchen = filter_scenes(raw(), "Home kitchen").limit(5)
kitchen.select("uuid", "scene_id", "scene_classification").show()
```

Or read the Parquet directly:

```python
import daft

classifications = daft.read_parquet(
    "hf://datasets/{repo_id}/{filename}"
)
```

## Attribution

This dataset is derived from the **DROID** robot manipulation dataset:

> Alexander Khazatsky, Karl Pertsch, Suraj Nair, et al. **DROID: A Large-Scale In-the-Wild Robot Manipulation Dataset.** *Robotics: Science and Systems (RSS)*, 2024.

- **License:** [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Original source:** [DROID_scene_classification.zip](https://github.com/droid-dataset/droid/files/15068147/DROID_scene_classification.zip) (GitHub issue [#6](https://github.com/droid-dataset/droid/issues/6))
- **DROID homepage:** https://droid-dataset.github.io/
- **DROID visualizer:** https://droid-dataset.github.io/dataset.html

When using this data, please cite the DROID paper and link to the original release above.

## Scene classification labels

The dataset contains the following label vocabulary:

- Bathroom
- Bedroom
- Hallway / closet / doorway
- Home dining room
- Home kitchen
- Home office
- Industrial dining room
- Industrial kitchen
- Industrial office
- Laundry
- Living room
- Unknown

## Hosting note

This mirror is maintained by [Eventual Inc.](https://www.eventual.ai/) for convenient access from Daft. The authoritative source remains the DROID authors' supplemental release linked above.
"""


def _build_readme(repo_id: str, filename: str) -> str:
    return README.format(repo_id=repo_id, filename=filename)


def _download_source_csv(dest_dir: Path, zip_url: str) -> Path:
    zip_path = dest_dir / "DROID_scene_classification.zip"
    print(f"Downloading source archive from {zip_url}")
    urllib.request.urlretrieve(zip_url, zip_path)

    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        if not csv_names:
            raise FileNotFoundError(f"No CSV found in archive: {zip_url}")

        csv_name = csv_names[0]
        extracted = dest_dir / Path(csv_name).name
        extracted.write_bytes(archive.read(csv_name))
        return extracted


def _resolve_source_csv(source_csv: Path | None, zip_url: str, work_dir: Path) -> Path:
    if source_csv is not None:
        if not source_csv.is_file():
            raise FileNotFoundError(f"Source CSV not found: {source_csv}")
        return source_csv

    return _download_source_csv(work_dir, zip_url)


def _write_parquet(source_csv: Path, output_path: Path) -> None:
    (
        daft.read_csv(str(source_csv))
        .select(
            col("scene_id").cast(DataType.int64()),
            col("scene_classification"),
        )
        .write_parquet(str(output_path), write_mode="overwrite")
    )


def _stage_dataset(source_csv: Path, repo_id: str, filename: str, staging_dir: Path) -> None:
    _write_parquet(source_csv, staging_dir / filename)
    (staging_dir / "README.md").write_text(_build_readme(repo_id, filename), encoding="utf-8")


def upload(
    *,
    repo_id: str,
    source_csv: Path | None,
    zip_url: str,
    filename: str,
    token: str | None,
    private: bool,
    dry_run: bool,
) -> None:
    with tempfile.TemporaryDirectory(prefix="droid-scene-classifications-") as tmp:
        work_dir = Path(tmp)
        resolved_csv = _resolve_source_csv(source_csv, zip_url, work_dir)
        staging_dir = work_dir / "staging"
        staging_dir.mkdir()
        _stage_dataset(resolved_csv, repo_id, filename, staging_dir)

        print(f"Staged files in {staging_dir}:")
        for path in sorted(staging_dir.rglob("*")):
            if path.is_file():
                print(f"  {path.relative_to(staging_dir)} ({path.stat().st_size:,} bytes)")

        hf_url = f"hf://datasets/{repo_id}/{filename}"
        print(f"\nDaft path after upload: {hf_url}")
        print(f"Hugging Face URL: https://huggingface.co/datasets/{repo_id}")

        if dry_run:
            print("\nDry run — skipping upload.")
            return

        try:
            from huggingface_hub import HfApi
        except ImportError:
            print(
                "Install huggingface_hub first: pip install huggingface_hub",
                file=sys.stderr,
            )
            raise SystemExit(1) from None

        api = HfApi(token=token)
        api.create_repo(
            repo_id=repo_id,
            repo_type="dataset",
            private=private,
            exist_ok=True,
        )
        api.upload_folder(
            folder_path=str(staging_dir),
            repo_id=repo_id,
            repo_type="dataset",
            commit_message="Add DROID scene classifications with attribution",
        )
        print("\nUpload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload DROID scene classifications to Hugging Face.",
    )
    parser.add_argument(
        "--repo-id",
        default=DEFAULT_REPO_ID,
        help=f"Hugging Face dataset repo (default: {DEFAULT_REPO_ID})",
    )
    parser.add_argument(
        "--source-csv",
        type=Path,
        default=None,
        help="Optional local CSV to convert and upload as Parquet",
    )
    parser.add_argument(
        "--source-zip-url",
        default=DEFAULT_SOURCE_ZIP_URL,
        help="Official DROID scene classification zip URL used when --source-csv is omitted",
    )
    parser.add_argument(
        "--filename",
        default=DEFAULT_FILENAME,
        help=f"Filename in the HF repo (default: {DEFAULT_FILENAME})",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="HF token (default: HF_TOKEN or HUGGING_FACE_HUB_TOKEN env var)",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Create a private dataset repo (default: public)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Stage files and print paths without uploading",
    )
    args = parser.parse_args()

    import os

    token = args.token or os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if not args.dry_run and not token:
        print(
            "Set HF_TOKEN (or pass --token). Create one at:\n"
            "  https://huggingface.co/settings/tokens\n"
            "The token needs write access to the target org/repo.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    upload(
        repo_id=args.repo_id,
        source_csv=args.source_csv,
        zip_url=args.source_zip_url,
        filename=args.filename,
        token=token,
        private=args.private,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
