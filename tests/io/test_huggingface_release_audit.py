from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import daft
from daft.exceptions import DaftTransientError
from tests._hf_retry import call_with_hf_retry

_SCRIPT = Path(__file__).resolve().parents[2] / ".github/ci-scripts/huggingface_release_audit.py"
_SPEC = importlib.util.spec_from_file_location("hf_audit", _SCRIPT)
audit = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(audit)


def test_release_catalog_has_five_immutable_cases_per_format():
    assert len(audit.load_catalog()) == 45


@pytest.mark.parametrize("mutation", ["unpinned", "partial", "duplicate", "missing", "unsafe_id", "same_repo"])
def test_release_catalog_rejects_invalid_coverage(tmp_path, mutation):
    cases = audit.load_catalog()
    if mutation == "unpinned":
        cases[0]["kwargs"]["revision"] = "main"
    elif mutation == "partial":
        cases[0]["kwargs"]["allow_partial"] = True
    elif mutation == "duplicate":
        cases[1]["id"] = cases[0]["id"]
    elif mutation == "unsafe_id":
        cases[0]["id"] = "../oops"
    elif mutation == "same_repo":
        cases[1]["repo"] = cases[0]["repo"]
    else:
        cases.pop()
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(cases))
    with pytest.raises(ValueError):
        audit.load_catalog(path)


def test_release_retry_exhaustion_raises_instead_of_skipping():
    fn = Mock(side_effect=DaftTransientError("unavailable"))
    with pytest.raises(DaftTransientError):
        call_with_hf_retry(fn, retries=2, backoff_seconds=0, on_exhausted="raise")
    assert fn.call_count == 2


def test_existing_retry_default_still_skips_transient_failures():
    with pytest.raises(pytest.skip.Exception):
        call_with_hf_retry(Mock(side_effect=DaftTransientError("unavailable")), retries=1)


def test_retry_does_not_retry_format_errors():
    fn = Mock(side_effect=ValueError("malformed schema"))
    with pytest.raises(ValueError, match="malformed schema"):
        call_with_hf_retry(fn, on_exhausted="raise")
    assert fn.call_count == 1


def test_release_retry_rejects_unknown_policy():
    with pytest.raises(ValueError, match="on_exhausted"):
        call_with_hf_retry(lambda: None, on_exhausted="pass")


@pytest.mark.parametrize("size", [None, audit.MAX_REPOSITORY_BYTES + 1])
def test_eager_source_budget_is_checked_before_reading(size):
    case = audit.load_catalog()[0]
    info = SimpleNamespace(sha=case["kwargs"]["revision"], siblings=[SimpleNamespace(size=size)])
    with patch("huggingface_hub.HfApi") as api, patch("daft.read_huggingface") as read:
        api.return_value.dataset_info.return_value = info
        with pytest.raises(AssertionError, match="budget"):
            audit.audit_case(case, require_wheel=False)
        read.assert_not_called()


def test_native_directory_budget_does_not_include_unselected_train_files():
    case = next(case for case in audit.load_catalog() if case["id"] == "wds-mnist")
    case.pop("media")
    case["columns"] = ["__key__"]
    info = SimpleNamespace(
        sha=case["kwargs"]["revision"],
        siblings=[
            SimpleNamespace(rfilename="train/0.tar", size=audit.MAX_REPOSITORY_BYTES + 1),
            SimpleNamespace(rfilename="test/0.tar", size=100),
        ],
    )
    with (
        patch("huggingface_hub.HfApi") as api,
        patch("daft.read_huggingface", return_value=daft.from_pydict({"__key__": ["sample"]})),
        patch("daft.set_execution_config"),
    ):
        api.return_value.dataset_info.return_value = info
        result = audit.audit_case(case, require_wheel=False)
    assert result["repo_bytes"] == 100
    assert result["webdataset_parallelism"] == 1


def test_candidate_wheel_check_rejects_editable_installs():
    with patch.object(audit, "distribution") as metadata:
        metadata.return_value.read_text.return_value = '{"dir_info": {"editable": true}}'
        with pytest.raises(AssertionError, match="candidate wheel"):
            audit.audit_case(audit.load_catalog()[0], require_wheel=True)
