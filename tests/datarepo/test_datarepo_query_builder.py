import pytest

from daft.dataclasses import dataclass
from daft.datarepo.log import DaftLakeLog
from daft.datarepo.query.definitions import QueryColumn
from daft.datarepo.query import stages
from daft.datarepo.datarepo import DataRepo
from daft.datarepo.query import functions as F

FAKE_DATAREPO_ID = "mydatarepo"
FAKE_DATAREPO_PATH = f"file:///tmp/fake_{FAKE_DATAREPO_ID}_path"


@dataclass
class MyFakeDataclass:
    foo: str


@pytest.fixture(scope="function")
def fake_datarepo() -> DataRepo:
    # TODO(jaychia): Use Datarepo client here instead once API stabilizes
    daft_lake_log = DaftLakeLog(FAKE_DATAREPO_PATH)
    return DataRepo(daft_lake_log)


def test_query_select_star(fake_datarepo: DataRepo) -> None:
    q = fake_datarepo.query(MyFakeDataclass)
    expected_stages = [
        stages.GetDatarepoStage(daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=None)
    ]
    assert len(q._query_tree.nodes()) == 1
    assert [k for k in q._query_tree.nodes()][0] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages


def test_query_limit(fake_datarepo: DataRepo) -> None:
    limit = 10
    q = fake_datarepo.query(MyFakeDataclass).limit(limit)
    expected_stages = [
        stages.GetDatarepoStage(daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=None),
        stages.LimitStage(limit=limit),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages


def test_query_filter(fake_datarepo: DataRepo) -> None:
    q = fake_datarepo.query(MyFakeDataclass).where("id", ">", 5)
    expected_stages = [
        stages.GetDatarepoStage(daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=None),
        stages.WhereStage("id", ">", 5),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages


def test_query_with_column(fake_datarepo: DataRepo) -> None:
    wrapped_func = lambda x: 1
    f = F.func(wrapped_func, return_type=int)
    q = fake_datarepo.query(MyFakeDataclass).with_column("foo", f("x"))
    expected_stages = [
        stages.GetDatarepoStage(daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=None),
        stages.WithColumnStage(
            new_column="foo",
            expr=F.QueryExpression(
                func=wrapped_func,
                args=("x",),
                kwargs={},
                batch_size=None,
                return_type=int,
            ),
        ),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages
