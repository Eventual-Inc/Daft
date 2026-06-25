# Spatial Joins

Daft supports spatial joins via the [df.join()][daft.DataFrame.join] `on=` predicate and SQL `JOIN ... ON ST_*`. Both routes the join to the R-tree nested-loop operator — no hash join shuffle is involved.

## Python API

Pass a spatial predicate expression as the `on=` argument. The two geometry columns must have **distinct names**; if both DataFrames share the same column name, alias one side first.

```python
import daft
from daft.functions import st_intersects, st_point, st_geomfromtext

pts = daft.from_pydict({"pid": [1, 2], "x": [1.0, 9.0], "y": [1.0, 9.0]}).select(
    daft.col("pid"), st_point(daft.col("x"), daft.col("y")).alias("pg")
)
polys = daft.from_pydict({"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}).select(
    daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qg")
)

result = pts.join(polys, on=st_intersects(polys["qg"], pts["pg"]))
```

Only **inner** joins are supported for spatial predicates. Passing `how="left"` (or any outer variant) raises a `ValueError`.

## SQL

Use `JOIN ... ON ST_Intersects(...)` (or any other spatial predicate):

```python
result = daft.sql(
    "SELECT pid, qid FROM pts JOIN polys ON ST_Intersects(pg, qg)",
    pts=pts,
    polys=polys,
)
```

## Supported predicates

| Predicate | Description |
|-----------|-------------|
| `st_intersects(a, b)` | True if A and B share any point |
| `st_contains(a, b)` | True if A completely contains B |
| `st_within(a, b)` | True if A is completely within B |
| `st_covers(a, b)` | True if no point of B is outside A (boundary included) |
| `st_covered_by(a, b)` | True if no point of A is outside B (boundary included) |
| `st_dwithin(a, b, d)` | True if the planar distance between A and B is ≤ d (coordinate units) |

All predicates are planar (Cartesian). For geodesic joins, project coordinates to a local metric CRS before joining.

## Combining equality keys with a spatial predicate

You can partition the join by an equality key alongside the spatial predicate. The equi-key becomes the R-tree partition key (the join still routes to the nested-loop operator, not the hash-join operator):

```python
# Alias the polygon-side key to avoid same-name ambiguity in the predicate resolver
polys = polys.with_column_renamed("region", "pregion")

result = pts.join(
    polys,
    on=(pts["region"] == polys["pregion"]) & st_intersects(polys["qg"], pts["pg"]),
)
```

## Optional: precompute bounding boxes

[df.with_spatial_bbox()][daft.DataFrame.with_spatial_bbox] adds `min_x`, `min_y`, `max_x`, `max_y` Float64 columns to a DataFrame. The spatial-join operator detects these columns on the build side and uses them as a precomputed bounding-box index, skipping per-row WKB extraction during the join:

```python
# Precompute bbox once on the build side (polygons)
polys_idx = polys.with_spatial_bbox("qg")

# The join operator auto-detects min_x/min_y/max_x/max_y and uses them as an index
result = pts.join(polys_idx, on=st_intersects(polys_idx["qg"], pts["pg"]))
```

This is most beneficial when the build side is reused across multiple joins (e.g. cached or persisted), since the bounding-box values are computed once. Results are identical with or without the index.

## Geometry constructors

Use `st_point(x, y)` to build points from coordinate columns, and `st_geomfromtext(wkt)` to parse WKT strings:

```python
from daft.functions import st_point, st_geomfromtext

pts = df.select(st_point(df["lon"], df["lat"]).alias("geom"))
polys = df.select(st_geomfromtext(df["wkt"]).alias("geom"))
```

See the [spatial functions reference][daft.functions] for the full list of geometry constructors and predicates.
