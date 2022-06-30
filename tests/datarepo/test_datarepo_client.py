import tempfile

import fsspec
import numpy as np

from daft.dataclasses import dataclass
from daft.datarepo.client import DatarepoClient


def test_datarepo_client_list_ids(path="memory://") -> None:

    client = DatarepoClient(f"{path}/test")
    assert client.list_ids() == []

    # Valid repos
    client._fs.makedir("test/foo/_log")
    client._fs.makedir("test/foo2/_log")
    client._fs.makedir("test/foo/bar/_log")

    # invalid repos
    client._fs.makedir("test/foo/baz/_log2")
    client._fs.makedir("test/foo/baz2/log")
    client._fs.makedir("test/foo3/")

    assert set(client.list_ids()) == {"foo", "foo2", "foo/bar"}

    assert client.get_path("foo") == f"{path}/test/foo"


# def test_datarepo_create_and_load() -> None:
#     @dataclass
#     class TestDc:
#         x: int
#         arr: np.ndarray

#     with tempfile.TemporaryDirectory() as td:
#         client = DatarepoClient(f"file://{td}")
#         assert client.list_ids() == []
#         dr = client.create("test_repo", TestDc)
#         assert client.list_ids() == ["test_repo"]
#         read_back_dr = client.from_id("test_repo")
#         assert dr.history() == read_back_dr.history()
