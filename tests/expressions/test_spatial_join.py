from __future__ import annotations

import pytest

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


# ── Oracle-equivalence tests ───────────────────────────────────────────────────


@pytest.mark.parametrize("predicate_name", ["st_intersects", "st_contains"])
def test_spatial_join_matches_oracle(predicate_name):
    """The R-tree nested-loop join must match the brute-force cross-join+filter oracle."""
    from daft import functions as F

    pred = getattr(F, predicate_name)
    pts = daft.from_pydict({"pid": [1, 2, 3], "x": [1.0, 9.0, 0.5], "y": [1.0, 9.0, 0.5]}).select(
        daft.col("pid"), st_point(daft.col("x"), daft.col("y")).alias("pg")
    )
    polys = daft.from_pydict(
        {"qid": [10, 11], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((8 8,10 8,10 10,8 10,8 8))"]}
    ).select(daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qg"))

    # container (polygon) is arg0 for st_contains; for st_intersects order is symmetric
    got = pts.join(polys, on=pred(polys["qg"], pts["pg"])).select("pid", "qid").sort(["pid", "qid"]).to_pydict()
    oracle = (
        pts.join(polys, how="cross")
        .where(pred(daft.col("qg"), daft.col("pg")))
        .select("pid", "qid")
        .sort(["pid", "qid"])
        .to_pydict()
    )
    assert len(oracle["pid"]) > 0, "oracle returned no rows — test would be vacuous"
    assert list(zip(got["pid"], got["qid"])) == list(zip(oracle["pid"], oracle["qid"]))


# ── Partitioned-path test (equi-key + spatial predicate) ──────────────────────


def test_spatial_join_partitioned_by_key():
    """A join with both an equality key AND a spatial predicate exercises the partitioned R-tree path."""
    pts = daft.from_pydict(
        {"region": ["a", "a", "b"], "pid": [1, 2, 3], "x": [1.0, 9.0, 1.0], "y": [1.0, 9.0, 1.0]}
    ).select("region", "pid", st_point(daft.col("x"), daft.col("y")).alias("pg"))
    # Alias the polygon-side region to "pregion" to avoid same-name ambiguity in the predicate resolver
    polys = daft.from_pydict(
        {"region": ["a", "b"], "qid": [10, 20], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((0 0,2 0,2 2,0 2,0 0))"]}
    ).select(daft.col("region").alias("pregion"), "qid", st_geomfromtext(daft.col("wkt")).alias("qg"))

    # equality on region + spatial predicate → partitioned R-tree nested-loop path
    # (NOT a hash join — the spatial residual routes the whole join to the nested-loop operator)
    got = (
        pts.join(polys, on=(pts["region"] == polys["pregion"]) & st_intersects(polys["qg"], pts["pg"]))
        .select("pid", "qid")
        .sort(["pid", "qid"])
        .to_pydict()
    )
    # region a: pid1 at (1,1) is inside poly10; pid2 at (9,9) is outside. region b: pid3 at (1,1) -> qid20
    assert list(zip(got["pid"], got["qid"])) == [(1, 10), (3, 20)]


# ── Bbox-index equivalence test ───────────────────────────────────────────────


def test_spatial_join_bbox_index_equivalence():
    """with_spatial_bbox() on the build side (polys) must yield the same join rows as the WKB baseline.

    IMPORTANT: the four bbox columns (min_x, min_y, max_x, max_y) MUST be kept in the output
    projection so that Daft's column-pruning optimizer does not drop them before the join
    operator executes.  Without them in the projection, column pruning silently removes all four
    columns and the join falls back to the WKB path — making the test vacuous.  Listing them
    explicitly in .select() is what causes the precomputed-bbox fast-path to genuinely engage.
    """
    pts = daft.from_pydict({"pid": [1, 2, 3], "x": [1.0, 9.0, 0.5], "y": [1.0, 9.0, 0.5]}).select(
        daft.col("pid"), st_point(daft.col("x"), daft.col("y")).alias("pg")
    )
    polys = daft.from_pydict(
        {"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}
    ).select(daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qg"))

    # Baseline: spatial join without precomputed bbox — operator derives MBRs from WKB.
    base = (
        pts.join(polys, on=st_intersects(polys["qg"], pts["pg"]))
        .select("pid", "qid")
        .sort(["pid", "qid"])
        .to_pydict()
    )

    # Indexed: precompute bbox on the build side (polys) AND retain all four bbox columns in
    # the projection.  Keeping them referenced is REQUIRED for the fast-path to engage — column
    # pruning otherwise drops them before the operator runs (see module docstring above).
    polys_idx = polys.with_spatial_bbox("qg")
    indexed = (
        pts.join(polys_idx, on=st_intersects(polys_idx["qg"], pts["pg"]))
        # min_x/min_y/max_x/max_y are listed here intentionally: this prevents column pruning
        # from dropping them and is what causes the precomputed-bbox fast-path to actually run.
        .select("pid", "qid", "min_x", "min_y", "max_x", "max_y")
        .sort(["pid", "qid"])
        .to_pydict()
    )

    assert len(base["pid"]) > 0, "baseline returned no rows — test would be vacuous"
    assert list(zip(base["pid"], base["qid"])) == list(zip(indexed["pid"], indexed["qid"]))
