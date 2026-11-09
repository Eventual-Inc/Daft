from __future__ import annotations

import io
from unittest.mock import ANY, patch

import pytest

import daft
from daft import IOConfig
from daft.exceptions import DaftCoreException, ThrottleError
from daft.io import HTTPConfig, HuggingFaceConfig
from daft.io.huggingface._metadata import parquet_files, read_json, source_files


@pytest.fixture(autouse=True)
def no_advisory_network(monkeypatch):
    monkeypatch.setattr("daft.io.huggingface.warn_if_lerobot", lambda *args: None)


def _file(config="default", split="train", *, partial=False):
    folder = f"partial-{split}" if partial else split
    return {
        "config": config,
        "split": split,
        "url": f"https://huggingface.co/datasets/org/repo/resolve/refs%2Fconvert%2Fparquet/{config}/{folder}/0000.parquet",
    }


def _metadata(files=None, **kwargs):
    return {"parquet_files": files if files is not None else [_file()], "partial": False, **kwargs}


@pytest.mark.parametrize("config,split", [(None, None), ("a", None), (None, "train"), ("b", "test")])
def test_parquet_selection(config, split):
    files = [_file(c, s) for c in ("a", "b") for s in ("train", "test")]
    with patch("daft.io.huggingface._metadata.read_json", return_value=_metadata(files)):
        result = parquet_files("org/repo", config, split, False, IOConfig())
    assert result == [
        f["url"] for f in files if (config is None or f["config"] == config) and (split is None or f["split"] == split)
    ]


@pytest.mark.parametrize(
    "config,split,message", [("missing", None, "Unknown config_name"), (None, "missing", "Unknown split")]
)
def test_unknown_selection(config, split, message):
    with (
        patch("daft.io.huggingface._metadata.read_json", return_value=_metadata()),
        pytest.raises(ValueError, match=message),
    ):
        parquet_files("org/repo", config, split, False, IOConfig())


@pytest.mark.parametrize("partial_flag", [True, False])
def test_partial_conversion_is_rejected(partial_flag):
    with patch(
        "daft.io.huggingface._metadata.read_json", return_value=_metadata([_file(partial=True)], partial=partial_flag)
    ):
        with pytest.raises(ValueError, match="is partial"):
            parquet_files("org/repo", None, None, False, IOConfig())
        with pytest.warns(UserWarning, match="is partial"):
            assert parquet_files("org/repo", None, None, True, IOConfig()) == [_file(partial=True)["url"]]


def test_partial_flag_without_identified_paths_is_rejected():
    with (
        patch("daft.io.huggingface._metadata.read_json", return_value=_metadata(partial=True)),
        pytest.raises(ValueError, match="is partial"),
    ):
        parquet_files("org/repo", None, None, False, IOConfig())


def test_complete_split_is_not_blocked_by_an_unselected_partial_split():
    metadata = _metadata([_file(split="test"), _file(partial=True)], partial=True)
    with patch("daft.io.huggingface._metadata.read_json", return_value=metadata):
        assert parquet_files("org/repo", None, "test", False, IOConfig()) == [_file(split="test")["url"]]


@pytest.mark.parametrize("key", ["pending", "failed"])
def test_incomplete_selection_is_rejected(key):
    metadata = _metadata(**{key: [{"config": "default", "split": "train"}]})
    with (
        patch("daft.io.huggingface._metadata.read_json", return_value=metadata),
        pytest.raises(ValueError, match="pending or failed"),
    ):
        parquet_files("org/repo", None, None, False, IOConfig())


def test_empty_conversion_can_trigger_fallback():
    with (
        patch("daft.io.huggingface._metadata.read_json", return_value=_metadata([])),
        pytest.raises(FileNotFoundError),
    ):
        parquet_files("org/repo", None, None, False, IOConfig())


def test_reader_uses_native_scan_and_effective_io_config():
    io_config = IOConfig(hf=HuggingFaceConfig(anonymous=True))
    sentinel = object()
    with (
        patch("daft.io.huggingface.parquet_files", return_value=["file.parquet"]) as discover,
        patch("daft.io.huggingface.read_parquet", return_value=sentinel) as scan,
        daft.planning_config_ctx(default_io_config=io_config),
    ):
        assert daft.read_huggingface("org/repo", config_name="a", split="test") is sentinel
    discover.assert_called_once_with("org/repo", "a", "test", False, ANY)
    effective = discover.call_args.args[-1]
    assert effective.hf.anonymous
    scan.assert_called_once_with(["file.parquet"], io_config=effective)


@pytest.mark.parametrize("error", [FileNotFoundError("missing data"), DaftCoreException("corrupt parquet Status(400")])
def test_data_read_errors_do_not_trigger_fallback(error):
    with (
        patch("daft.io.huggingface.parquet_files", return_value=["file.parquet"]),
        patch("daft.io.huggingface.read_parquet", side_effect=error),
        patch("daft.io.huggingface._fallback_to_datasets_library") as fallback,
        pytest.raises(type(error)),
    ):
        daft.read_huggingface("org/repo")
    fallback.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        DaftCoreException("Status(401 Unauthorized)"),
        DaftCoreException("Status(403 Forbidden)"),
        ThrottleError("Status(429)"),
    ],
)
def test_discovery_auth_and_transient_errors_propagate(error):
    with (
        patch("daft.io.huggingface.parquet_files", side_effect=error),
        patch("daft.io.huggingface._fallback_to_datasets_library") as fallback,
        pytest.raises(type(error)),
    ):
        daft.read_huggingface("org/repo")
    fallback.assert_not_called()


@pytest.mark.parametrize("anonymous,token", [(False, "test-token"), (True, False)])
@pytest.mark.parametrize("split", [None, "test"])
def test_fallback_preserves_selection_and_credentials(anonymous, token, split):
    from datasets import Dataset, DatasetDict

    dataset = Dataset.from_dict({"value": [1]})
    result = dataset if split else DatasetDict(train=dataset, test=dataset)
    io_config = IOConfig(hf=HuggingFaceConfig(token="test-token", anonymous=anonymous))
    with patch("datasets.load_dataset", return_value=result) as load, pytest.warns(UserWarning, match="materialize"):
        actual = daft.read_huggingface(
            "org/repo", io_config, "datasets", config_name="a", split=split, revision="v1", data_dir="data"
        ).to_pydict()
    assert actual == {"value": [1] if split else [1, 1]}
    load.assert_called_once_with("org/repo", name="a", split=split, revision="v1", data_dir="data", token=token)


def test_viewer_metadata_uses_hf_auth_and_preserves_http_settings():
    config = IOConfig(
        hf=HuggingFaceConfig(token="test-token"),
        http=HTTPConfig(bearer_token="unrelated-token", num_tries=2, read_timeout_ms=1234),
    )
    with patch("daft.io.huggingface._metadata.open_file", return_value=io.StringIO("{}")) as open_metadata:
        read_json("https://datasets-server.huggingface.co/parquet", config, viewer=True)
    effective = open_metadata.call_args.kwargs["io_config"]
    assert effective.http.bearer_token == "test-token"
    assert effective.http.num_tries == 2
    assert effective.http.read_timeout_ms == 1234
    assert config.http.bearer_token == "unrelated-token"


def test_anonymous_viewer_does_not_send_either_token():
    config = IOConfig(
        hf=HuggingFaceConfig(token="hf-token", anonymous=True), http=HTTPConfig(bearer_token="http-token")
    )
    with patch("daft.io.huggingface._metadata.open_file", return_value=io.StringIO("{}")) as open_metadata:
        read_json("https://datasets-server.huggingface.co/parquet", config, viewer=True)
    assert open_metadata.call_args.kwargs["io_config"].http.bearer_token is None


@pytest.mark.parametrize(
    "declarations",
    [
        [{"split": "train", "path": "a/train/*.tar"}, {"split": "test", "path": ["a/test/*.tar"]}],
        {"train": "a/train/*.tar", "test": "a/test/*.tar"},
    ],
)
def test_webdataset_card_selection(declarations):
    info = {
        "cardData": {"configs": [{"config_name": "a", "data_files": declarations}]},
        "siblings": [
            {"rfilename": "a/train/0.tar"},
            {"rfilename": "a/test/0.tar"},
            {"rfilename": "b/test/0.tar"},
        ],
    }
    with patch("daft.io.huggingface._metadata.repo_info", return_value=info):
        files = source_files("org/repo", ".tar", "a", "test", None, "refs/pr/1", IOConfig())
    assert files == ["hf://datasets/org/repo@refs%2Fpr%2F1/a/test/0.tar"]


def test_webdataset_standard_split_boundaries():
    info = {
        "siblings": [
            {"rfilename": path} for path in ["train/0.tar", "data/train-0.tar", "pretrain/0.tar", "test/0.tar"]
        ]
    }
    with patch("daft.io.huggingface._metadata.repo_info", return_value=info):
        assert source_files("org/repo", ".tar", None, "train", None, None, IOConfig()) == [
            "hf://datasets/org/repo/data/train-0.tar",
            "hf://datasets/org/repo/train/0.tar",
        ]


def test_explicit_revision_does_not_use_current_conversion():
    with (
        patch("daft.io.huggingface.source_files", return_value=["old.parquet"]) as source,
        patch("daft.io.huggingface.parquet_files") as converted,
        patch("daft.io.huggingface.read_parquet"),
    ):
        daft.read_huggingface("org/repo", revision="old", split="train")
    converted.assert_not_called()
    assert source.call_args.args[:6] == ("org/repo", ".parquet", None, "train", None, "old")


@pytest.mark.parametrize("error", [FileNotFoundError("no conversion"), DaftCoreException("Status(404 Not Found)")])
def test_original_parquet_remains_native_without_conversion(error):
    with (
        patch("daft.io.huggingface.parquet_files", side_effect=error),
        patch("daft.io.huggingface.source_files", return_value=["original.parquet"]),
        patch("daft.io.huggingface.read_parquet", return_value="native") as read,
        patch("daft.io.huggingface._fallback_to_datasets_library") as fallback,
    ):
        assert daft.read_huggingface("org/repo") == "native"
    assert read.call_args.args == (["original.parquet"],)
    fallback.assert_not_called()


def test_source_declarations_exclude_auxiliary_parquet():
    info = {
        "cardData": {"configs": [{"config_name": "default", "data_files": "data/*/*.parquet"}]},
        "siblings": [{"rfilename": p} for p in ["data/chunk-000/episode.parquet", "meta/tasks.parquet"]],
    }
    with patch("daft.io.huggingface._metadata.repo_info", return_value=info):
        assert source_files("org/repo", ".parquet", None, None, None, None, IOConfig()) == [
            "hf://datasets/org/repo/data/chunk-000/episode.parquet"
        ]


@pytest.mark.parametrize(
    "kwargs",
    [{"split": "train[:10]"}, {"config_name": ""}, {"data_dir": "../data"}, {"data_dir": "/data"}, {"format": "csv"}],
)
def test_invalid_reader_arguments(kwargs):
    with pytest.raises(ValueError):
        daft.read_huggingface("org/repo", **kwargs)
