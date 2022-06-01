import fsspec

from daft.datarepos.client import DatarepoClient


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
