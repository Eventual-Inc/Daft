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
