import pytest
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from daft.datarepo.metadata_service import _LocalDatarepoMetadataService

DATAREPO_ID = "datarepo_foo"


@pytest.fixture()
def empty_metadata_service():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield _LocalDatarepoMetadataService(tmpdir)


@pytest.fixture()
def populated_metadata_service(empty_metadata_service: _LocalDatarepoMetadataService):
    path = empty_metadata_service.get_path(DATAREPO_ID)
    mock_tbl = pa.Table.from_pylist([{"foo": i} for i in range(10)])
    pq.write_table(mock_tbl, path)
    yield empty_metadata_service


def test_list_repos_norepo(empty_metadata_service: _LocalDatarepoMetadataService):
    assert empty_metadata_service.list_ids() == []


def test_list_repos_singlerepo(populated_metadata_service: _LocalDatarepoMetadataService):
    assert populated_metadata_service.list_ids() == [DATAREPO_ID]
