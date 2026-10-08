from __future__ import annotations

import warnings
from unittest.mock import patch

import pytest

import daft
from daft import IOConfig
from daft.exceptions import ThrottleError
from daft.io.huggingface._lerobot import warn_if_lerobot


def _repo(tags=None):
    return {"tags": tags or [], "siblings": [{"rfilename": "meta/info.json"}]}


@pytest.mark.parametrize("version", ["v2.0", "v2.1", "v3.0"])
def test_lerobot_detected_from_layout_metadata(version):
    config = IOConfig()
    metadata = {"codebase_version": version, "features": {}, "data_path": "data/{episode_index}.parquet", "fps": 30}
    with (
        patch("daft.io.huggingface._lerobot.repo_info", return_value=_repo()) as repo,
        patch("daft.io.huggingface._lerobot.read_json", return_value=metadata) as read,
        pytest.warns(UserWarning, match="daft.datasets.lerobot.read") as recorded,
    ):
        warn_if_lerobot("org/repo", "v1", config)
    assert len(recorded) == 1
    repo.assert_called_once_with("org/repo", "v1", config)
    read.assert_called_once_with("hf://datasets/org/repo@v1/meta/info.json", config)


def test_lerobot_tag_and_layout_warn_without_reading_info():
    with (
        patch("daft.io.huggingface._lerobot.repo_info", return_value=_repo(["LeRobot"])),
        patch("daft.io.huggingface._lerobot.read_json") as read,
        patch("daft.io.huggingface.parquet_files", return_value=["data.parquet"]),
        patch("daft.io.huggingface.read_parquet", return_value="native result"),
        pytest.warns(UserWarning, match="timestamp alignment") as recorded,
    ):
        assert daft.read_huggingface("org/repo") == "native result"
    assert len(recorded) == 1
    read.assert_not_called()


@pytest.mark.parametrize(
    "repo,metadata",
    [
        ({"tags": ["robotics", "video"], "siblings": []}, None),
        ({"tags": ["LeRobot"], "siblings": []}, None),
        (_repo(), {"features": {}, "fps": 30}),
        (_repo(), {"codebase_version": "v3.0", "features": []}),
    ],
)
def test_ordinary_datasets_do_not_warn(repo, metadata):
    with (
        patch("daft.io.huggingface._lerobot.repo_info", return_value=repo),
        patch("daft.io.huggingface._lerobot.read_json", return_value=metadata),
        warnings.catch_warnings(record=True) as recorded,
    ):
        warnings.simplefilter("always")
        warn_if_lerobot("lerobot-name-but-not-a-dataset/repo", None, IOConfig())
    assert not recorded


def test_advisory_failure_does_not_block_generic_reader():
    with (
        patch("daft.io.huggingface._lerobot.repo_info", side_effect=ThrottleError("unavailable")),
        patch("daft.io.huggingface.parquet_files", return_value=["data.parquet"]),
        patch("daft.io.huggingface.read_parquet", return_value="native result"),
    ):
        assert daft.read_huggingface("org/repo") == "native result"
