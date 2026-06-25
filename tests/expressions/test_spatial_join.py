from __future__ import annotations

import daft
from daft.functions import st_intersects, st_point, st_geomfromtext


def _points_polys():
    # points: id 1 at (1,1) inside; id 2 at (9,9) outside the polygon
    pts = daft.from_pydict({"pid": [1, 2], "px": [1.0, 9.0], "py": [1.0, 9.0]}).select(
        daft.col("pid"), st_point(daft.col("px"), daft.col("py")).alias("pgeom")
    )
    # one polygon (0,0)-(2,2)
    polys = daft.from_pydict(
        {"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}
    ).select(daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qgeom"))
    return pts, polys


def test_sql_spatial_join_on():
    pts, polys = _points_polys()
    result = daft.sql(
        "SELECT pid, qid FROM pts JOIN polys ON ST_Intersects(pgeom, qgeom) ORDER BY pid",
        pts=pts,
        polys=polys,
    ).to_pydict()
    # only point 1 intersects the polygon
    assert result["pid"] == [1]
    assert result["qid"] == [10]
    assert len(result["pid"]) == 1


def test_python_join_on_predicate():
    pts, polys = _points_polys()
    result = (
        pts.join(polys, on=st_intersects(pts["pgeom"], polys["qgeom"]))
        .select("pid", "qid")
        .sort("pid")
        .to_pydict()
    )
    assert result["pid"] == [1]
    assert result["qid"] == [10]


def test_python_join_on_predicate_rejects_outer():
    import pytest

    pts, polys = _points_polys()
    with pytest.raises(ValueError, match="inner"):
        pts.join(polys, on=st_intersects(pts["pgeom"], polys["qgeom"]), how="left")


def test_python_join_st_dwithin():
    from daft.functions import st_dwithin, st_point

    left = daft.from_pydict({"lid": [1, 2], "lx": [0.0, 0.0], "ly": [0.0, 0.0]}).select(
        daft.col("lid"), st_point(daft.col("lx"), daft.col("ly")).alias("lg")
    )
    right = daft.from_pydict({"rid": [10, 11], "rx": [3.0, 100.0], "ry": [4.0, 100.0]}).select(
        daft.col("rid"), st_point(daft.col("rx"), daft.col("ry")).alias("rg")
    )
    # distance lid->rid: (1,10)=5, (1,11)=~141, (2,10)=5, (2,11)=~141
    result = (
        left.join(right, on=st_dwithin(left["lg"], right["rg"], 5.0))
        .select("lid", "rid")
        .sort(["lid", "rid"])
        .to_pydict()
    )
    assert list(zip(result["lid"], result["rid"])) == [(1, 10), (2, 10)]
