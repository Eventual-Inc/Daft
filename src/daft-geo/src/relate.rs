use geo::coordinate_position::{CoordPos, CoordinatePosition};
use geo::relate::Relate;
use geo::{Contains, Geometry, Intersects};

/// The DE-9IM spatial predicate to evaluate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelatePred {
    Intersects,
    Contains,
    Within,
    Touches,
    Crosses,
    Overlaps,
    Disjoint,
    Equals,
    Covers,
    CoveredBy,
}

/// Point fast path: when one operand is a single `Point`, most DE-9IM predicates
/// reduce to a point-in-geometry test (`coordinate_position` / ray-cast) that
/// avoids building the full topology graph `Relate` requires. Returns `None`
/// when no fast path applies (non-point pairs, or predicates whose point
/// semantics aren't reducible), in which case the caller falls back to relate.
///
/// Semantics per DE-9IM, verified against the relate matrix by
/// `point_fast_path_agrees_with_relate_matrix`:
/// - Contains(a, pt): pt in interior(a) — boundary excluded (`geo::Contains`).
/// - Within(pt, b): mirror of Contains.
/// - Intersects / Disjoint: pt not in exterior / pt in exterior.
/// - Covers(a, pt) / CoveredBy(pt, b): boundary included.
/// - Touches: pt on boundary only (a point has no boundary, so point-point
///   touches is always false — `coordinate_position` yields `Inside` there).
fn point_fast_path(a: &Geometry, b: &Geometry, pred: RelatePred) -> Option<bool> {
    match pred {
        RelatePred::Contains => match b {
            Geometry::Point(p) => Some(a.contains(p)),
            _ => None,
        },
        RelatePred::Within => match a {
            Geometry::Point(p) => Some(b.contains(p)),
            _ => None,
        },
        RelatePred::Intersects | RelatePred::Disjoint => {
            let hit = match (a, b) {
                (_, Geometry::Point(p)) => a.intersects(p),
                (Geometry::Point(p), _) => b.intersects(p),
                _ => return None,
            };
            Some(matches!(pred, RelatePred::Intersects) == hit)
        }
        RelatePred::Covers => match b {
            Geometry::Point(p) => Some(a.coordinate_position(&p.0) != CoordPos::Outside),
            _ => None,
        },
        RelatePred::CoveredBy => match a {
            Geometry::Point(p) => Some(b.coordinate_position(&p.0) != CoordPos::Outside),
            _ => None,
        },
        RelatePred::Touches => match (a, b) {
            (other, Geometry::Point(p)) | (Geometry::Point(p), other) => {
                Some(other.coordinate_position(&p.0) == CoordPos::OnBoundary)
            }
            _ => None,
        },
        // Crosses/Overlaps/Equals with a point operand are rare in joins and
        // have subtler dimension-dependent semantics — leave them to relate.
        RelatePred::Crosses | RelatePred::Overlaps | RelatePred::Equals => None,
    }
}

/// Evaluate a DE-9IM predicate between two geometries. Correct for all geometry-type pairs.
///
/// Wrapped in `catch_unwind` so that a panic on degenerate input (e.g. a self-intersecting
/// ring triggering a bug in the `geo` relate implementation) yields `false` rather than
/// unwinding the compute thread — consistent with the catch_unwind convention used by
/// buffer, BooleanOps, convexhull, and simplify in this crate.
pub fn relate_pred(a: &Geometry, b: &Geometry, pred: RelatePred) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(fast) = point_fast_path(a, b, pred) {
            return fast;
        }
        let m = a.relate(b); // geo 0.33: Geometry: Relate -> IntersectionMatrix
        match pred {
            RelatePred::Intersects => m.is_intersects(),
            RelatePred::Contains => m.is_contains(),
            RelatePred::Within => m.is_within(),
            RelatePred::Touches => m.is_touches(),
            RelatePred::Crosses => m.is_crosses(),
            RelatePred::Overlaps => m.is_overlaps(),
            RelatePred::Disjoint => m.is_disjoint(),
            RelatePred::Equals => m.is_equal_topo(),
            RelatePred::Covers => m.is_covers(),
            RelatePred::CoveredBy => m.is_coveredby(),
        }
    }))
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::relate::Relate;
    use geo::{Geometry, MultiPolygon, Point, Polygon, LineString, Coord};

    fn square() -> Geometry {
        // unit square (0,0)-(2,2)
        let ring = LineString(vec![
            Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 0.0 },
            Coord { x: 2.0, y: 2.0 }, Coord { x: 0.0, y: 2.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        Geometry::Polygon(Polygon::new(ring, vec![]))
    }

    // ── Point fast-path tests ─────────────────────────────────────────────
    //
    // `point_fast_path` must agree with the full DE-9IM matrix on every input
    // it accepts (returns `Some`). The baseline below IS the spec: the same
    // matrix dispatch `relate_pred` uses, evaluated without any fast path.

    fn relate_baseline(a: &Geometry, b: &Geometry, pred: RelatePred) -> bool {
        let m = a.relate(b);
        match pred {
            RelatePred::Intersects => m.is_intersects(),
            RelatePred::Contains => m.is_contains(),
            RelatePred::Within => m.is_within(),
            RelatePred::Touches => m.is_touches(),
            RelatePred::Crosses => m.is_crosses(),
            RelatePred::Overlaps => m.is_overlaps(),
            RelatePred::Disjoint => m.is_disjoint(),
            RelatePred::Equals => m.is_equal_topo(),
            RelatePred::Covers => m.is_covers(),
            RelatePred::CoveredBy => m.is_coveredby(),
        }
    }

    const ALL_PREDS: [RelatePred; 10] = [
        RelatePred::Intersects, RelatePred::Contains, RelatePred::Within,
        RelatePred::Touches, RelatePred::Crosses, RelatePred::Overlaps,
        RelatePred::Disjoint, RelatePred::Equals, RelatePred::Covers,
        RelatePred::CoveredBy,
    ];

    /// Square (0,0)-(4,4) with a hole (1,1)-(3,3): exercises interior,
    /// exterior ring boundary, hole boundary, and inside-the-hole cases.
    fn holed_square() -> Geometry {
        let outer = LineString(vec![
            Coord { x: 0.0, y: 0.0 }, Coord { x: 4.0, y: 0.0 },
            Coord { x: 4.0, y: 4.0 }, Coord { x: 0.0, y: 4.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        let hole = LineString(vec![
            Coord { x: 1.0, y: 1.0 }, Coord { x: 3.0, y: 1.0 },
            Coord { x: 3.0, y: 3.0 }, Coord { x: 1.0, y: 3.0 },
            Coord { x: 1.0, y: 1.0 },
        ]);
        Geometry::Polygon(Polygon::new(outer, vec![hole]))
    }

    fn containers() -> Vec<Geometry> {
        vec![
            square(),
            holed_square(),
            Geometry::MultiPolygon(MultiPolygon(vec![
                Polygon::new(
                    LineString(vec![
                        Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 0.0 },
                        Coord { x: 2.0, y: 2.0 }, Coord { x: 0.0, y: 2.0 },
                        Coord { x: 0.0, y: 0.0 },
                    ]),
                    vec![],
                ),
                Polygon::new(
                    LineString(vec![
                        Coord { x: 10.0, y: 10.0 }, Coord { x: 12.0, y: 10.0 },
                        Coord { x: 12.0, y: 12.0 }, Coord { x: 10.0, y: 12.0 },
                        Coord { x: 10.0, y: 10.0 },
                    ]),
                    vec![],
                ),
            ])),
            Geometry::LineString(LineString(vec![
                Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 2.0 },
                Coord { x: 4.0, y: 0.0 },
            ])),
            Geometry::Point(Point::new(1.0, 1.0)),
        ]
    }

    fn probe_points() -> Vec<Geometry> {
        [
            (0.5, 0.5),   // interior (in-ring for holed square)
            (2.0, 2.0),   // holed-square interior-band / square corner-ish
            (0.0, 1.0),   // exterior-ring boundary edge
            (0.0, 0.0),   // exterior-ring corner
            (1.0, 2.0),   // hole boundary (holed square) / boundary (square)
            (2.0, 2.0),   // dedupe-safe repeat
            (1.5, 1.5),   // inside the hole (exterior for holed square)
            (1.0, 1.0),   // hole corner / point-equal case
            (100.0, 100.0), // far outside
            (11.0, 11.0), // inside second multipolygon part
        ]
        .iter()
        .map(|&(x, y)| Geometry::Point(Point::new(x, y)))
        .collect()
    }

    /// Every (container, point, pred) and (point, container, pred) combination
    /// the fast path accepts must equal the DE-9IM baseline.
    #[test]
    fn point_fast_path_agrees_with_relate_matrix() {
        for container in containers() {
            for pt in probe_points() {
                for pred in ALL_PREDS {
                    for (a, b) in [(&container, &pt), (&pt, &container)] {
                        if let Some(fast) = point_fast_path(a, b, pred) {
                            let expected = relate_baseline(a, b, pred);
                            assert_eq!(
                                fast, expected,
                                "fast path disagrees with relate: {pred:?} a={a:?} b={b:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// The workload case must actually take the fast path — not silently
    /// decline and fall back to relate.
    #[test]
    fn point_fast_path_accepts_point_in_polygon_predicates() {
        let poly = square();
        let pt = Geometry::Point(Point::new(1.0, 1.0));
        assert!(point_fast_path(&poly, &pt, RelatePred::Contains).is_some());
        assert!(point_fast_path(&pt, &poly, RelatePred::Within).is_some());
        assert!(point_fast_path(&poly, &pt, RelatePred::Intersects).is_some());
        assert!(point_fast_path(&pt, &poly, RelatePred::Intersects).is_some());
        assert!(point_fast_path(&poly, &pt, RelatePred::Disjoint).is_some());
        assert!(point_fast_path(&poly, &pt, RelatePred::Covers).is_some());
        assert!(point_fast_path(&pt, &poly, RelatePred::CoveredBy).is_some());
    }

    /// Non-point pairs must decline so relate keeps handling them.
    #[test]
    fn point_fast_path_declines_non_point_pairs() {
        let a = square();
        let b = holed_square();
        for pred in ALL_PREDS {
            assert!(
                point_fast_path(&a, &b, pred).is_none(),
                "polygon-polygon must not take the point fast path ({pred:?})"
            );
        }
    }

    /// Boundary semantics through the public entry point: contains is
    /// interior-only, covers includes the boundary.
    #[test]
    fn relate_pred_boundary_point_semantics_unchanged() {
        let poly = square();
        let boundary = Geometry::Point(Point::new(0.0, 1.0));
        let inside = Geometry::Point(Point::new(1.0, 1.0));
        assert!(!relate_pred(&poly, &boundary, RelatePred::Contains));
        assert!(relate_pred(&poly, &boundary, RelatePred::Covers));
        assert!(relate_pred(&poly, &boundary, RelatePred::Intersects));
        assert!(!relate_pred(&poly, &boundary, RelatePred::Disjoint));
        assert!(relate_pred(&poly, &inside, RelatePred::Contains));
        assert!(relate_pred(&inside, &poly, RelatePred::Within));
        assert!(!relate_pred(&boundary, &poly, RelatePred::Within));
        assert!(relate_pred(&boundary, &poly, RelatePred::CoveredBy));
    }

    #[test]
    fn test_contains_and_within_are_symmetric() {
        let poly = square();
        let inside = Geometry::Point(Point::new(1.0, 1.0));
        // previously st_contains returned false for Polygon/Point only via hand-coding;
        // relate must handle it and within is the mirror.
        assert!(relate_pred(&poly, &inside, RelatePred::Contains));
        assert!(relate_pred(&inside, &poly, RelatePred::Within));
        assert!(!relate_pred(&inside, &poly, RelatePred::Contains));
    }

    #[test]
    fn test_disjoint_is_not_intersects() {
        let poly = square();
        let far = Geometry::Point(Point::new(100.0, 100.0));
        assert!(relate_pred(&poly, &far, RelatePred::Disjoint));
        assert!(!relate_pred(&poly, &far, RelatePred::Intersects));
    }

    #[test]
    fn test_linestring_polygon_intersects_now_handled() {
        // a type pair the old hand-coded st_intersects fell through to false on
        let poly = square();
        let line = Geometry::LineString(LineString(vec![
            Coord { x: -1.0, y: 1.0 }, Coord { x: 3.0, y: 1.0 },
        ]));
        assert!(relate_pred(&poly, &line, RelatePred::Intersects));
    }

    #[test]
    fn test_covers_and_covered_by() {
        let poly = square(); // (0,0)-(2,2)
        // Boundary point: contained-by-covers but NOT contains (interior only).
        let boundary = Geometry::Point(Point::new(0.0, 1.0));
        assert!(relate_pred(&poly, &boundary, RelatePred::Covers));
        assert!(relate_pred(&boundary, &poly, RelatePred::CoveredBy));
        assert!(!relate_pred(&poly, &boundary, RelatePred::Contains)); // interior-only
        // A far point is neither covered nor covering.
        let far = Geometry::Point(Point::new(100.0, 100.0));
        assert!(!relate_pred(&poly, &far, RelatePred::Covers));
    }
}
