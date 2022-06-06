import pytest

from daft.dataclasses import dataclass
from daft.datarepo.log import DaftLakeLog
from daft.datarepo.query.definitions import QueryColumn
from daft.datarepo.query import stages
from daft.datarepo.datarepo import DataRepo

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


def test_query_limit_optimization_simple(fake_datarepo: DataRepo) -> None:
    limit = 10
    q = fake_datarepo.query(MyFakeDataclass).limit(limit)
    optimized_tree, root = q._optimize_query_tree()
    expected_optimized_read_stage = stages.GetDatarepoStage(
        daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=limit
    )
    assert len(optimized_tree.nodes()) == 1
    assert [v for _, v in optimized_tree.nodes().items()][0]["stage"] == expected_optimized_read_stage


def test_query_optimization_interleaved(fake_datarepo: DataRepo) -> None:
    limit = 10
    f = lambda x: 1
    q = (
        fake_datarepo.query(MyFakeDataclass)
        .limit(limit)
        .where("id", ">", 5)
        .limit(limit + 2)
        .where("id", ">", 6)
        .limit(limit + 1)
        .apply(f, "foo")
        .limit(limit)
    )
    optimized_tree, root = q._optimize_query_tree()
    expected_optimized_stages = [
        stages.GetDatarepoStage(
            daft_lake_log=fake_datarepo._log,
            dtype=MyFakeDataclass,
            read_limit=limit,
            filters=[[("id", ">", 6), ("id", ">", 5)]],
        ),
        stages.ApplyStage(f=f, args=("foo",), kwargs={}),
    ]
    assert [v["stage"] for _, v in optimized_tree.nodes().items()] == expected_optimized_stages


def test_query_limit_optimization_min_limits(fake_datarepo: DataRepo) -> None:
    limit = 10
    for q in [
        fake_datarepo.query(MyFakeDataclass).limit(limit).limit(limit + 10),
        fake_datarepo.query(MyFakeDataclass).limit(limit + 10).limit(limit),
        fake_datarepo.query(MyFakeDataclass).limit(limit).limit(limit),
    ]:
        optimized_tree, root = q._optimize_query_tree()
        expected_optimized_read_stage = stages.GetDatarepoStage(
            daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=limit
        )
        assert len(optimized_tree.nodes()) == 1
        assert [v for _, v in optimized_tree.nodes().items()][0]["stage"] == expected_optimized_read_stage


def test_query_filter(fake_datarepo: DataRepo) -> None:
    q = fake_datarepo.query(MyFakeDataclass).where("id", ">", 5)
    expected_stages = [
        stages.GetDatarepoStage(daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=None),
        stages.WhereStage("id", ">", 5),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages


def test_query_apply(fake_datarepo: DataRepo) -> None:
    f = lambda x: 1
    q = fake_datarepo.query(MyFakeDataclass).apply(f, "foo", somekwarg="bar")
    expected_stages = [
        stages.GetDatarepoStage(daft_lake_log=fake_datarepo._log, dtype=MyFakeDataclass, read_limit=None),
        stages.ApplyStage(f=f, args=("foo",), kwargs={"somekwarg": "bar"}),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages
