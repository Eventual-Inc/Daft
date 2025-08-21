from __future__ import annotations

import os
import uuid

import pytest

import daft
from tests.conftest import assert_df_equals


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="Test requires Hugging Face credentials and `--credentials` flag")


def token():
    return os.environ.get("HF_TOKEN")


def load(repo, split):
    from datasets import load_dataset

    return load_dataset(repo, token=token(), split=split).to_pandas()


@pytest.fixture(scope="session")
def api():
    import huggingface_hub

    return huggingface_hub.HfApi(token=token())


@pytest.fixture
def repo(api):
    repo_id = f"Eventual-Inc/test-{uuid.uuid4()}"
    api.create_repo(repo_id, private=True, repo_type="dataset")
    yield repo_id
    api.delete_repo(repo_id, repo_type="dataset")


@pytest.mark.integration()
def test_basic(repo):
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})

    df.write_huggingface(repo)

    actual = load(repo, "train")
    assert_df_equals(actual, df.to_pandas(), "a")


@pytest.mark.integration()
@pytest.mark.parametrize("split", ["train", "custom"])
def test_append(repo, split):
    df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
    df2 = daft.from_pydict({"a": [4, 5, 6], "b": ["x", "y", "z"]})

    df1.write_huggingface(repo, split=split)
    df2.write_huggingface(repo, split=split)

    actual = load(repo, split)
    expected = df1.concat(df2)
    assert_df_equals(actual, expected.to_pandas(), "a")


@pytest.mark.integration()
@pytest.mark.parametrize("split", ["train", "custom"])
def test_overwrite(repo, split):
    df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
    df2 = daft.from_pydict({"a": [4, 5, 6], "b": ["x", "y", "z"]})

    df1.write_huggingface(repo, split=split, overwrite=True)
    df2.write_huggingface(repo, split=split, overwrite=True)

    actual = load(repo, split)
    assert_df_equals(actual, df2.to_pandas(), "a")


@pytest.mark.integration()
def test_split(repo):
    df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
    df2 = daft.from_pydict({"a": [4, 5, 6], "b": ["x", "y", "z"]})

    df1.write_huggingface(repo)
    df2.write_huggingface(repo, split="custom")

    actual1 = load(repo, "train")
    actual2 = load(repo, "custom")

    assert_df_equals(actual1, df1.to_pandas(), "a")
    assert_df_equals(actual2, df2.to_pandas(), "a")


@pytest.mark.integration()
def test_revision(repo, api):
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
    api.create_branch(repo, branch="test", repo_type="dataset")
    df.write_huggingface(repo, revision="test")

    assert any(
        file.path.endswith(".parquet")
        for file in api.list_repo_tree(repo, repo_type="dataset", revision="test", recursive=True)
    )
