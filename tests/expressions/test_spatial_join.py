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


def _pts_polys_for_bbox():
    pts = daft.from_pydict({"pid": [1, 2, 3], "x": [1.0, 9.0, 0.5], "y": [1.0, 9.0, 0.5]}).select(
        daft.col("pid"), st_point(daft.col("x"), daft.col("y")).alias("pg")
    )
    polys = daft.from_pydict(
        {"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"], "shift": [1000.0]}
    ).select(daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qg"), daft.col("shift"))
    return pts, polys


def test_spatial_join_bbox_index_equivalence():
    """with_spatial_bbox() on the build side yields the same join rows as the WKB baseline.

    The `rtree_*` columns are preserved through the spatial join transparently (the optimizer keeps
    them on the build side), so they need NOT be listed in the final projection — selecting only the
    non-bbox columns still engages the precomputed-bbox fast-path, and the bbox columns are dropped
    from the output.
    """
    pts, polys = _pts_polys_for_bbox()

    # Baseline: spatial join without precomputed bbox — operator derives MBRs from WKB.
    base = (
        pts.join(polys, on=st_intersects(polys["qg"], pts["pg"]))
        .select("pid", "qid")
        .sort(["pid", "qid"])
        .to_pydict()
    )

    # Indexed: precompute bbox on the build side; select ONLY non-bbox columns. Transparent
    # preservation keeps rtree_* on the build side anyway, so results must match the baseline,
    # and the rtree_* columns must NOT appear in the output.
    polys_idx = polys.with_spatial_bbox("qg")
    joined = pts.join(polys_idx, on=st_intersects(polys_idx["qg"], pts["pg"])).select("pid", "qid")
    assert not any(c.startswith("rtree_") for c in joined.column_names), "rtree_* leaked into output"
    indexed = joined.sort(["pid", "qid"]).to_pydict()

    assert len(base["pid"]) > 0, "baseline returned no rows — test would be vacuous"
    assert list(zip(base["pid"], base["qid"])) == list(zip(indexed["pid"], indexed["qid"]))


def test_spatial_join_bbox_index_is_actually_used():
    """Adversarial proof the preserved rtree_* index is genuinely used by the operator.

    Corrupt the rtree_* bbox columns (shift them far away) on the build side and select only the
    non-bbox columns. Because transparent preservation keeps the columns on the build side and the
    operator builds its R-tree from them, the corrupted bbox makes the probe find no candidates →
    NO matches. If the columns were pruned (or ignored), the join would fall back to WKB and return
    the correct match — so an empty result is what proves the precomputed index is in effect.
    """
    pts, polys = _pts_polys_for_bbox()
    pts = pts.where(daft.col("pid") == 1)  # single point (1,1), inside the polygon

    wrong = polys.with_spatial_bbox("qg").with_columns(
        {
            "rtree_min_x": daft.col("rtree_min_x") + daft.col("shift"),
            "rtree_min_y": daft.col("rtree_min_y") + daft.col("shift"),
            "rtree_max_x": daft.col("rtree_max_x") + daft.col("shift"),
            "rtree_max_y": daft.col("rtree_max_y") + daft.col("shift"),
        }
    )
    out = pts.join(wrong, on=st_intersects(wrong["qg"], pts["pg"])).select("pid", "qid").to_pydict()
    assert out["pid"] == [], "corrupted rtree_* bbox was ignored — the precomputed index is not being used"

    # Sanity: with the CORRECT precomputed bbox, the same join finds the match.
    correct = polys.with_spatial_bbox("qg")
    ok = pts.join(correct, on=st_intersects(correct["qg"], pts["pg"])).select("pid", "qid").to_pydict()
    assert ok["pid"] == [1]
