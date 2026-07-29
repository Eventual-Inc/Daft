from __future__ import annotations

import pytest

import daft
from daft import IOConfig
from daft.daft import io_glob
from daft.io import HuggingFaceConfig
from tests._hf_retry import call_with_hf_retry

VLA_JEPA_REVISION = "735d9f692981e286ade093b5046627eda876e5d0"
VLA_JEPA_MODEL_ID = "lerobot/VLA-JEPA-LIBERO"

DEFAULT_REPO_ID = "zhouxueyang/LIBERO-Pro"
DEFAULT_REVISION = "c86fc3b8293185a6f373677018ff3e37f8391602"

BUCKET_ID = "the-hf-stack/zenml-experiments"


def _read_prefix(path: str, io_config: IOConfig, size: int = 16) -> bytes:
    with daft.File(path, io_config=io_config).open() as file:
        return file.read(size)


@pytest.mark.integration()
def test_revision_pinned_model_glob_and_read():
    """Glob an immutable model revision and read a small Xet-backed file with Xet enabled."""
    io_config = IOConfig(hf=HuggingFaceConfig(use_xet=True))
    filename = "policy_postprocessor_step_2_unnormalizer_processor.safetensors"
    path = f"hf://models/{VLA_JEPA_MODEL_ID}@{VLA_JEPA_REVISION}/{filename}"

    files = call_with_hf_retry(io_glob, path.replace("step_2", "step_*"), io_config=io_config)

    assert [file["path"] for file in files] == [path]
    assert len(call_with_hf_retry(_read_prefix, files[0]["path"], io_config)) == 16


@pytest.mark.integration()
def test_explicit_main_model_glob():
    """Canonicalize an explicit @main so listing results still match the glob."""
    pattern = f"hf://models/{VLA_JEPA_MODEL_ID}@main/config.*"
    expected = f"hf://models/{VLA_JEPA_MODEL_ID}/config.json"

    files = call_with_hf_retry(io_glob, pattern)

    assert [file["path"] for file in files] == [expected]


@pytest.mark.integration()
def test_revision_pinned_dataset_glob():
    """Glob dataset files while retaining the immutable revision in every result."""
    pattern = f"hf://datasets/{DEFAULT_REPO_ID}@{DEFAULT_REVISION}/metadata/*index.json"
    expected = f"hf://datasets/{DEFAULT_REPO_ID}@{DEFAULT_REVISION}/metadata/dataset_index.json"

    files = call_with_hf_retry(io_glob, pattern)

    assert [file["path"] for file in files] == [expected]


@pytest.mark.integration()
def test_bucket_glob_remains_revisionless():
    """HF Buckets do not expose revisions, so their canonical paths must not gain one."""
    pattern = f"hf://buckets/{BUCKET_ID}/trackio/data/*.parquet"
    expected = f"hf://buckets/{BUCKET_ID}/trackio/data/train-00000-of-00001.parquet"

    files = call_with_hf_retry(io_glob, pattern)

    paths = [file["path"] for file in files]
    assert expected in paths
    assert all(path.startswith(f"hf://buckets/{BUCKET_ID}/") and "@" not in path for path in paths)
