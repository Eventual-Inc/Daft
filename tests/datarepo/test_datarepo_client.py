import tempfile

import numpy as np

from daft.dataclasses import dataclass
from daft.datarepo.client import DatarepoClient


@dataclass
class _TestDc:
    x: int
    arr: np.ndarray


def test_datarepo_client_full_workflow() -> None:
    with tempfile.TemporaryDirectory() as td:
        client = DatarepoClient(f"file://{td}")
        assert client.list_ids() == []

        repo = client.create("test_repo", _TestDc)

        assert client.list_ids() == ["test_repo"]

        repo = client.from_id("test_repo")

        assert repo.name().endswith("test_repo")

        assert client.delete("test_repo") == True

        assert client.list_ids() == []


# def test_datarepo_create_and_load() -> None:

#     with tempfile.TemporaryDirectory() as td:
#         client = DatarepoClient(f"file://{td}")
#         assert client.list_ids() == []
#         dr = client.create("test_repo", TestDc)
#         assert client.list_ids() == ["test_repo"]
#         read_back_dr = client.from_id("test_repo")
#         assert dr.history() == read_back_dr.history()
