from daft.datarepo.query import (
    QueryColumn,
    query,
    _QueryTreeNode,
    _DatarepoNodeType,
    _LimitOpNodeType,
    _FilterOpNodeType,
    _ApplyOpNodeType,
    _FilterPredicate,
)

FAKE_DATAREPO_ID = "mydatarepo"


def test_query_select_star() -> None:
    q = query(FAKE_DATAREPO_ID)
    data = {"datarepo_id": FAKE_DATAREPO_ID}
    assert len(q._query_tree.nodes()) == 1
    assert [k for k in q._query_tree.nodes()][0] == q._root
    assert [v["type"] for _, v in q._query_tree.nodes().items()] == [_DatarepoNodeType]
    assert [v["data"] for _, v in q._query_tree.nodes().items()] == [data]


def test_query_limit() -> None:
    limit = 10
    q = query(FAKE_DATAREPO_ID).limit(limit)
    data = [{"datarepo_id": FAKE_DATAREPO_ID}, {"limit": limit}]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["type"] for _, v in q._query_tree.nodes().items()] == [_DatarepoNodeType, _LimitOpNodeType]
    assert [v["data"] for _, v in q._query_tree.nodes().items()] == data


def test_query_filter() -> None:
    pred = _FilterPredicate(left="id", comparator=">", right="5")
    q = query(FAKE_DATAREPO_ID).filter(pred)
    data = [{"datarepo_id": FAKE_DATAREPO_ID}, {"predicate": pred}]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["type"] for _, v in q._query_tree.nodes().items()] == [_DatarepoNodeType, _FilterOpNodeType]
    assert [v["data"] for _, v in q._query_tree.nodes().items()] == data


def test_query_apply() -> None:
    f = lambda x: 1
    q = query(FAKE_DATAREPO_ID).apply(f, QueryColumn(name="foo"), somekwarg=QueryColumn(name="bar"))
    data = [
        {"datarepo_id": FAKE_DATAREPO_ID},
        {"f": f, "args": (QueryColumn(name="foo"),), "kwargs": {"somekwarg": QueryColumn(name="bar")}},
    ]
    assert len(q._query_tree.nodes()) == 2
    assert [k for k in q._query_tree.nodes()][-1] == q._root
    assert [v["type"] for _, v in q._query_tree.nodes().items()] == [_DatarepoNodeType, _ApplyOpNodeType]
    assert [v["data"] for _, v in q._query_tree.nodes().items()] == data
