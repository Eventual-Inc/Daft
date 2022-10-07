import subprocess
import tempfile

import numpy as np
import pytest

from daft.experimental.dataclasses import dataclass
from daft.experimental.datarepo.client import DatarepoClient


@dataclass
class _TestDc:
    x: int
    arr: np.ndarray


def verify_java() -> bool:
    try:
        subprocess.run(["java", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except:
        return False


@pytest.mark.skipif(not verify_java(), reason="We do not have java on this machine")
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
