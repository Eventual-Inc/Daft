import pytest
import tempfile

from daft.dataclasses import DataclassBuilder, dataclass
from daft.datarepo.query.expressions import QueryColumn
from daft.datarepo.query import stages
from daft.datarepo.datarepo import DataRepo
from daft.datarepo.query import functions as F

from .utils import create_test_catalog

FAKE_CATALOG = "mydatarepo"


@dataclass
class MyFakeDataclass:
    foo: str


@pytest.fixture(scope="function")
def fake_datarepo() -> DataRepo:
    with tempfile.TemporaryDirectory() as td:
        catalog = create_test_catalog(td)
        yield DataRepo.create(catalog, FAKE_CATALOG, MyFakeDataclass)


def test_query_select_star(fake_datarepo: DataRepo) -> None:
    q = fake_datarepo.query(MyFakeDataclass)
    expected_stages = [stages.ReadIcebergTableStage(datarepo=fake_datarepo, dtype=MyFakeDataclass, read_limit=None)]
    assert len(q._query_tree.nodes()) == 1
    assert [k for k in q._query_tree.nodes()][0] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages


def test_query_limit(fake_datarepo: DataRepo) -> None:
    limit = 10
    q = fake_datarepo.query(MyFakeDataclass).limit(limit)
    expected_stages = [
        stages.ReadIcebergTableStage(datarepo=fake_datarepo, dtype=MyFakeDataclass, read_limit=None),
        stages.LimitStage(limit=limit),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages


def test_query_limit_optimization_simple(fake_datarepo: DataRepo) -> None:
    limit = 10
    q = fake_datarepo.query(MyFakeDataclass).limit(limit)
    optimized_tree, root = q._optimize_query_tree()
    expected_optimized_read_stage = stages.ReadIcebergTableStage(
        datarepo=fake_datarepo, dtype=MyFakeDataclass, read_limit=limit
    )
    assert len(optimized_tree.nodes()) == 1
    assert [v for _, v in optimized_tree.nodes().items()][0]["stage"] == expected_optimized_read_stage


def test_query_optimization_interleaved(fake_datarepo: DataRepo) -> None:
    limit = 10
    wrapped_func = lambda x: 1
    f = F.func(wrapped_func, return_type=int)
    q = (
        fake_datarepo.query(MyFakeDataclass)
        .limit(limit)
        .where("id", ">", 5)
        .limit(limit + 2)
        .where("id", ">", 6)
        .limit(limit + 1)
        .with_column("bar", f("x"))
        .limit(limit)
    )
    optimized_tree, root = q._optimize_query_tree()
    dataclass_builder = DataclassBuilder.from_class(MyFakeDataclass)
    dataclass_builder.add_field("bar", int)
    new_dataclass = dataclass_builder.generate()
    expected_optimized_stages = [
        stages.ReadIcebergTableStage(
            datarepo=fake_datarepo,
            dtype=MyFakeDataclass,
            read_limit=limit,
            filters=[[("id", ">", 6), ("id", ">", 5)]],
        ),
        stages.WithColumnStage(
            new_column="bar",
            expr=F.QueryExpression(
                func=wrapped_func,
                return_type=int,
                args=("x",),
                kwargs={},
                batch_size=None,
            ),
            dataclass=new_dataclass,
        ),
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
        expected_optimized_read_stage = stages.ReadIcebergTableStage(
            datarepo=fake_datarepo, dtype=MyFakeDataclass, read_limit=limit
        )
        assert len(optimized_tree.nodes()) == 1
        assert [v for _, v in optimized_tree.nodes().items()][0]["stage"] == expected_optimized_read_stage


def test_query_filter(fake_datarepo: DataRepo) -> None:
    q = fake_datarepo.query(MyFakeDataclass).where("id", ">", 5)
    expected_stages = [
        stages.ReadIcebergTableStage(datarepo=fake_datarepo, dtype=MyFakeDataclass, read_limit=None),
        stages.WhereStage("id", ">", 5),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages


def test_query_with_column(fake_datarepo: DataRepo) -> None:
    wrapped_func = lambda x: 1
    f = F.func(wrapped_func, return_type=int)
    q = fake_datarepo.query(MyFakeDataclass).with_column("bar", f("x"))
    dataclass_builder = DataclassBuilder.from_class(MyFakeDataclass)
    dataclass_builder.add_field("bar", int)
    new_dataclass = dataclass_builder.generate()
    expected_stages = [
        stages.ReadIcebergTableStage(datarepo=fake_datarepo, dtype=MyFakeDataclass, read_limit=None),
        stages.WithColumnStage(
            new_column="bar",
            expr=F.QueryExpression(
                func=wrapped_func,
                args=("x",),
                kwargs={},
                batch_size=None,
                return_type=int,
            ),
            dataclass=new_dataclass,
        ),
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["stage"] for _, v in q._query_tree.nodes().items()] == expected_stages
