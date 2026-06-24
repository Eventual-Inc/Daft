use geo::Geometry;
use geo::relate::Relate;

/// The DE-9IM spatial predicate to evaluate.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum RelatePred {
    Intersects,
    Contains,
    Within,
    Touches,
    Crosses,
    Overlaps,
    Disjoint,
    Equals,
}

/// Evaluate a DE-9IM predicate between two geometries. Correct for all geometry-type pairs.
pub(crate) fn relate_pred(a: &Geometry, b: &Geometry, pred: RelatePred) -> bool {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Geometry, Point, Polygon, LineString, Coord};

    fn square() -> Geometry {
        // unit square (0,0)-(2,2)
        let ring = LineString(vec![
            Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 0.0 },
            Coord { x: 2.0, y: 2.0 }, Coord { x: 0.0, y: 2.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        Geometry::Polygon(Polygon::new(ring, vec![]))
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
}
