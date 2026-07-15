from __future__ import annotations

import datetime
import os

import pytest

import daft
from daft.functions import st_geomfromtext, st_intersects, st_point


def _points_polys():
    # points: id 1 at (1,1) inside; id 2 at (9,9) outside the polygon
    pts = daft.from_pydict({"pid": [1, 2], "px": [1.0, 9.0], "py": [1.0, 9.0]}).select(
        daft.col("pid"), st_point(daft.col("px"), daft.col("py")).alias("pgeom")
    )
    # one polygon (0,0)-(2,2)
    polys = daft.from_pydict({"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}).select(
        daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qgeom")
    )
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
        pts.join(polys, on=st_intersects(pts["pgeom"], polys["qgeom"])).select("pid", "qid").sort("pid").to_pydict()
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
        {
            "region": ["a", "b"],
            "qid": [10, 20],
            "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((0 0,2 0,2 2,0 2,0 0))"],
        }
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
    polys = daft.from_pydict({"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"], "shift": [1000.0]}).select(
        daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qg"), daft.col("shift")
    )
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
        pts.join(polys, on=st_intersects(polys["qg"], pts["pg"])).select("pid", "qid").sort(["pid", "qid"]).to_pydict()
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


# ── Full ON-predicate enforcement in the NLJ filter ────────────────────────────


def test_spatial_join_composite_equi_key():
    """Two equality columns + a spatial predicate must both be enforced, not just the spatial one."""
    pts = daft.from_pydict(
        {
            "region": ["a", "a", "b"],
            "zone": [1, 2, 1],
            "pid": [1, 2, 3],
            "x": [1.0, 1.0, 1.0],
            "y": [1.0, 1.0, 1.0],
        }
    ).select("region", "zone", "pid", st_point(daft.col("x"), daft.col("y")).alias("pg"))
    polys = daft.from_pydict(
        {
            "region": ["a", "b"],
            "zone": [1, 1],
            "qid": [10, 20],
            "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((0 0,2 0,2 2,0 2,0 0))"],
        }
    ).select(
        daft.col("region").alias("pregion"),
        daft.col("zone").alias("pzone"),
        "qid",
        st_geomfromtext(daft.col("wkt")).alias("qg"),
    )
    got = (
        pts.join(
            polys,
            on=(pts["region"] == polys["pregion"])
            & (pts["zone"] == polys["pzone"])
            & st_intersects(polys["qg"], pts["pg"]),
        )
        .select("pid", "qid")
        .sort(["pid", "qid"])
        .to_pydict()
    )
    # pid=2 (region=a, zone=2) has no polygon with zone=2 and must NOT match qid=10,
    # even though it is spatially inside qid=10's polygon.
    assert list(zip(got["pid"], got["qid"])) == [(1, 10), (3, 20)]


def test_spatial_join_null_key_never_matches():
    """A NULL equi-join key must never match another NULL key under standard `=` semantics."""
    left = daft.from_pydict({"lid": [1, 2], "key": [None, "k"], "x": [1.0, 1.0], "y": [1.0, 1.0]}).select(
        "lid", "key", st_point(daft.col("x"), daft.col("y")).alias("lg")
    )
    # Alias the right-side key to "rkey" to avoid same-name ambiguity in the predicate
    # resolver (see test_spatial_join_partitioned_by_key above) — this only renames the
    # schema field and does not change the NULL-collision semantics under test.
    right = daft.from_pydict(
        {"rid": [10, 11], "key": [None, "k"], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"] * 2}
    ).select("rid", daft.col("key").alias("rkey"), st_geomfromtext(daft.col("wkt")).alias("rg"))
    got = (
        left.join(right, on=(left["key"] == right["rkey"]) & st_intersects(right["rg"], left["lg"]))
        .select("lid", "rid")
        .sort(["lid", "rid"])
        .to_pydict()
    )
    assert list(zip(got["lid"], got["rid"])) == [(2, 11)]


def test_spatial_join_literal_none_string_key_does_not_match_null():
    """A literal 'None' string key must not join against NULL keys.

    Today both str_value-render to "None" and collide into one group.
    """
    left = daft.from_pydict({"lid": [1], "key": ["None"], "x": [1.0], "y": [1.0]}).select(
        "lid", "key", st_point(daft.col("x"), daft.col("y")).alias("lg")
    )
    # Alias the right-side key to "rkey" to avoid same-name ambiguity in the predicate
    # resolver (see test_spatial_join_partitioned_by_key above) — this only renames the
    # schema field and does not change the NULL-vs-"None" semantics under test.
    right = daft.from_pydict({"rid": [10], "key": [None], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}).select(
        "rid", daft.col("key").alias("rkey"), st_geomfromtext(daft.col("wkt")).alias("rg")
    )
    right = right.with_column("rkey", right["rkey"].cast(daft.DataType.string()))
    got = (
        left.join(right, on=(left["key"] == right["rkey"]) & st_intersects(right["rg"], left["lg"]))
        .select("lid", "rid")
        .to_pydict()
    )
    assert got["lid"] == []


def test_spatial_join_null_safe_key_collision():
    """A literal string "None" must NOT null-safe-match a real NULL key.

    Even though both render to the same string "None" via `str_value` during partition-key
    R-tree grouping and would land in the same candidate group.

    `FilterNullJoinKey` only strips NULLs ahead of STANDARD (`=`) equi-keys — it explicitly
    skips null-safe (`<=>`) keys (see `filter_null_join_key.rs`, `!*null_eq_null` filter) —
    so the real NULL on the right survives to reach the spatial NLJ's candidate-generation
    path. Pre-fix, the NLJ filter carried only the spatial residual, so once candidate
    generation merged the "None"/NULL groups by `str_value`, the pair was emitted with no
    equality re-check. Post-fix, the complete filter re-evaluates `key <=> rkey`, which is
    false for a literal string vs a real NULL under null-safe semantics, so no row joins.
    """
    left = daft.from_pydict({"lid": [1], "key": ["None"], "x": [1.0], "y": [1.0]}).select(
        "lid", "key", st_point(daft.col("x"), daft.col("y")).alias("lg")
    )
    # Alias the right-side key to "rkey" to avoid same-name ambiguity in the predicate
    # resolver (see test_spatial_join_partitioned_by_key above) — this only renames the
    # schema field and does not change the NULL-vs-"None" semantics under test.
    right = daft.from_pydict({"rid": [10], "key": [None], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}).select(
        "rid", daft.col("key").alias("rkey"), st_geomfromtext(daft.col("wkt")).alias("rg")
    )
    right = right.with_column("rkey", right["rkey"].cast(daft.DataType.string()))
    got = (
        left.join(
            right,
            on=left["key"].eq_null_safe(right["rkey"]) & st_intersects(right["rg"], left["lg"]),
        )
        .select("lid", "rid")
        .to_pydict()
    )
    # Under null-safe (`<=>`) semantics a literal "None" string is NOT equal to NULL, so
    # no row should join — even though both spatially intersect the same polygon.
    assert got["lid"] == []


def test_spatial_join_cross_dtype_equi_key():
    """Equal equi-key values at different dtypes must still join.

    Equi-key columns holding EQUAL values at DIFFERENT dtypes (Date `2024-01-01` vs
    Timestamp `2024-01-01T00:00:00`) must still join: the translate.rs dtype guard in the
    `partition_key` closure detects the mismatch and falls back to unpartitioned candidate
    generation with the complete filter, rather than letting per-key `str_value` grouping
    place the matching rows in different groups and silently drop the row.

    Date vs Timestamp is used (per the guard's own comment in translate.rs) rather than
    Int64 vs Float64: Rust's `{}` Display renders `1.0f64` as `"1"`, identical to `1i64`'s
    `"1"`, so an Int64/Float64 pair does NOT actually collide differently under
    `str_value` and would not RED pre-fix (verified empirically). Date's `str_value`
    renders as `"2024-01-01"` and Timestamp's as `"2024-01-01T00:00:00"` (or similar,
    always including a time component), which DO differ, so this pair genuinely exercises
    the guard.
    """
    left = daft.from_pydict({"lid": [1], "key": [datetime.date(2024, 1, 1)], "x": [1.0], "y": [1.0]}).select(
        "lid", "key", st_point(daft.col("x"), daft.col("y")).alias("lg")
    )
    # Alias the right-side key to "rkey" to avoid same-name ambiguity in the predicate
    # resolver (see test_spatial_join_partitioned_by_key above).
    right = daft.from_pydict(
        {
            "rid": [10],
            "rkey": [datetime.datetime(2024, 1, 1)],
            "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"],
        }
    ).select("rid", "rkey", st_geomfromtext(daft.col("wkt")).alias("rg"))
    got = (
        left.join(
            right,
            on=(left["key"] == right["rkey"]) & st_intersects(right["rg"], left["lg"]),
        )
        .select("lid", "rid")
        .to_pydict()
    )
    # key=2024-01-01 (Date) == rkey=2024-01-01T00:00:00 (Timestamp), and both spatially
    # intersect the polygon.
    assert list(zip(got["lid"], got["rid"])) == [(1, 10)]


@pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") != "ray",
    reason="SpatialHashJoinNode only exists in the distributed (Ray) pipeline",
)
def test_distributed_spatial_hash_join_does_not_hash_collide_different_keys():
    """Hash collisions must not let non-matching keys join.

    Many distinct keys, few partitions: hash collisions are guaranteed. The equi-key
    must still be enforced, not just the spatial predicate.

    Note: written as an equi-join followed by `.where(st_intersects(...))` rather than a
    combined `on=` predicate. This exercises the `Filter(spatial) -> [Project?] ->
    Join(equi)` shape of the distributed rewrite (see translate.rs f_down) — the WHERE
    clause is a separate Filter node above an equi-only Join. A combined `on=` predicate
    instead keeps the equality and spatial conjuncts together on the Join node itself (no
    separate Filter node above the Join); that bare-Join shape is covered by
    `test_distributed_spatial_join_combined_on_predicate` below, which routes through the
    same `SpatialHashJoinNode` via a second f_down branch. Both shapes are verified via
    `df.explain(True)` showing `SpatialHashJoin`; the semantics under test are identical
    either way.
    """
    n = 200
    pts = daft.from_pydict(
        {
            "key": [str(i) for i in range(n)],
            "pid": list(range(n)),
            "x": [1.0] * n,
            "y": [1.0] * n,
        }
    ).select("key", "pid", st_point(daft.col("x"), daft.col("y")).alias("pg"))
    # Every polygon covers the SAME area as every point, so the spatial predicate
    # alone matches everything — only the equi-key restricts matches.
    polys = daft.from_pydict(
        {
            "pkey": [str(i) for i in range(n)],
            "qid": list(range(n)),
            "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"] * n,
        }
    ).select("pkey", "qid", st_geomfromtext(daft.col("wkt")).alias("qg"))

    got = (
        pts.join(polys, left_on="key", right_on="pkey")
        .where(st_intersects(daft.col("qg"), daft.col("pg")))
        .select("pid", "qid")
        .to_pydict()
    )
    assert sorted(got["pid"]) == list(range(n))
    assert all(pid == qid for pid, qid in zip(got["pid"], got["qid"]))


@pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") != "ray",
    reason="SpatialHashJoinNode only exists in the distributed (Ray) pipeline",
)
def test_distributed_spatial_join_combined_on_predicate():
    """A combined `on=` equality + spatial predicate must route to the spatial hash join.

    The natural way to write a distributed spatial join: a single combined `on=`
    predicate mixing an equality key and a spatial predicate, e.g.
    `df.join(other, on=(a == b) & st_intersects(...))`.

    Unlike `test_distributed_spatial_hash_join_does_not_hash_collide_different_keys`
    (which uses `.where(...)` to produce a separate Filter node above an equi-only
    Join), this shape produces a BARE `Join` node whose `on` already contains both the
    equality conjunct and the spatial residual, with no wrapping Filter. Before the
    fix, this shape fell through to the generic distributed join translator, which
    panics with `todo!("FLOTILLA_MS?: Implement non-equality joins")` as soon as
    `split_eq_preds` leaves a non-empty (spatial) residual — see translate_join.rs.

    Many distinct keys, few partitions: hash collisions are guaranteed, so this also
    proves the equi-key is genuinely enforced (not just the spatial predicate) even
    though the equi-key and spatial predicate are combined in one ON clause.
    """
    n = 200
    pts = daft.from_pydict(
        {
            "key": [str(i) for i in range(n)],
            "pid": list(range(n)),
            "x": [1.0] * n,
            "y": [1.0] * n,
        }
    ).select("key", "pid", st_point(daft.col("x"), daft.col("y")).alias("pg"))
    # Every polygon covers the SAME area as every point, so the spatial predicate
    # alone matches everything — only the equi-key restricts matches.
    polys = daft.from_pydict(
        {
            "pkey": [str(i) for i in range(n)],
            "qid": list(range(n)),
            "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"] * n,
        }
    ).select("pkey", "qid", st_geomfromtext(daft.col("wkt")).alias("qg"))

    got = (
        pts.join(polys, on=(pts["key"] == polys["pkey"]) & st_intersects(polys["qg"], pts["pg"]))
        .select("pid", "qid")
        .to_pydict()
    )
    assert sorted(got["pid"]) == list(range(n))
    assert all(pid == qid for pid, qid in zip(got["pid"], got["qid"]))


# ── R-tree acceleration must not be applied to unsound predicates ─────────────


def test_python_join_st_disjoint():
    """st_disjoint must not be R-tree-accelerated.

    Its true matches typically have NON-intersecting bboxes, which bbox candidate
    generation silently drops.
    """
    from daft.functions import st_disjoint

    left = daft.from_pydict({"lid": [1, 2], "lx": [0.0, 100.0], "ly": [0.0, 100.0]}).select(
        daft.col("lid"), st_point(daft.col("lx"), daft.col("ly")).alias("lg")
    )
    right = daft.from_pydict({"rid": [10], "rx": [0.0], "ry": [0.0]}).select(
        daft.col("rid"), st_point(daft.col("rx"), daft.col("ry")).alias("rg")
    )
    result = left.join(right, on=st_disjoint(left["lg"], right["rg"])).select("lid", "rid").sort("lid").to_pydict()
    # lid=1 coincides with rid=10 (not disjoint); lid=2 is far away (disjoint).
    assert result["lid"] == [2]
    assert result["rid"] == [10]


def test_not_st_intersects_select():
    """Direct (non-join) regression test for the Not-over-ScalarFn field-name bug.

    Negating a spatial predicate over materialized, non-aliased geometry columns must
    compute the correct boolean result instead of raising `DaftCoreException`
    'Mismatch of expected expression name and name from computed series'. This pins the
    root fix (Not/IsNull/NotNull::to_field naming) independently of the join machinery
    exercised by `test_python_join_not_st_intersects` below.
    """
    df = (
        daft.from_pydict(
            {
                "x": [1.0, 9.0],
                "y": [1.0, 9.0],
                "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((0 0,2 0,2 2,0 2,0 0))"],
            }
        )
        .select(
            st_point(daft.col("x"), daft.col("y")).alias("a"),
            st_geomfromtext(daft.col("wkt")).alias("b"),
        )
        .collect()
    )

    result = df.select((~st_intersects(daft.col("a"), daft.col("b"))).alias("r")).to_pydict()
    # (1,1) is inside the polygon -> st_intersects True -> negated False.
    # (9,9) is outside the polygon -> st_intersects False -> negated True.
    assert result["r"] == [False, True]


@pytest.mark.timeout(30)
def test_python_join_not_st_intersects():
    """A negated spatial predicate must skip bbox-based R-tree acceleration."""
    left = daft.from_pydict({"lid": [1, 2], "lx": [1.0, 100.0], "ly": [1.0, 100.0]}).select(
        daft.col("lid"), st_point(daft.col("lx"), daft.col("ly")).alias("lg")
    )
    right = daft.from_pydict({"rid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}).select(
        daft.col("rid"), st_geomfromtext(daft.col("wkt")).alias("rg")
    )
    result = left.join(right, on=~st_intersects(left["lg"], right["rg"])).select("lid", "rid").sort("lid").to_pydict()
    # lid=1 is inside the polygon -> excluded by NOT; lid=2 is far outside -> included.
    assert result["lid"] == [2]
    assert result["rid"] == [10]


def test_python_join_or_composed_spatial_predicate():
    """An OR-composed predicate must skip acceleration.

    A pair failing the spatial branch (and its bbox test) can still satisfy the other
    OR branch.
    """
    left = daft.from_pydict({"lid": [1, 2], "lx": [1.0, 100.0], "ly": [1.0, 100.0]}).select(
        daft.col("lid"), st_point(daft.col("lx"), daft.col("ly")).alias("lg")
    )
    right = daft.from_pydict({"rid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}).select(
        daft.col("rid"), st_geomfromtext(daft.col("wkt")).alias("rg")
    )
    result = (
        left.join(
            right,
            on=st_intersects(left["lg"], right["rg"]) | ((left["lid"] + 8) == right["rid"]),
        )
        .select("lid", "rid")
        .sort("lid")
        .to_pydict()
    )
    # lid=1 intersects; lid=2 does not intersect but satisfies lid+8 == rid (2+8=10).
    assert result["lid"] == [1, 2]
    assert result["rid"] == [10, 10]
