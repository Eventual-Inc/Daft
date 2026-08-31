use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Schema};
use daft_core::series::Series;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{
    Coord, Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
    orient::{Direction, Orient},
};
use serde::{Deserialize, Serialize};

use crate::utils::{geom_to_wkb, unary_geom_to_geom, validate_geometry_field};

/// Compare two coordinates lexicographically by (x, y).
///
/// NaN components compare as equal rather than panicking; this only affects the
/// deterministic ordering used for canonicalization, not the geometry's validity.
fn coord_cmp(a: &Coord<f64>, b: &Coord<f64>) -> std::cmp::Ordering {
    a.x.partial_cmp(&b.x)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| a.y.partial_cmp(&b.y).unwrap_or(std::cmp::Ordering::Equal))
}

/// Compare two coordinate sequences lexicographically; shorter sequence sorts first on a
/// common-prefix tie. Used to order rings, multi-part members, and holes deterministically.
fn coords_cmp(a: &[Coord<f64>], b: &[Coord<f64>]) -> std::cmp::Ordering {
    a.iter()
        .zip(b.iter())
        .map(|(ca, cb)| coord_cmp(ca, cb))
        .find(|o| *o != std::cmp::Ordering::Equal)
        .unwrap_or_else(|| a.len().cmp(&b.len()))
}

/// Rotate a closed ring so it starts at its lexicographically smallest vertex, preserving
/// winding direction. No-op for open or degenerate (fewer than 3 point) rings.
fn rotate_ring_to_min_vertex(ring: &LineString<f64>) -> LineString<f64> {
    let coords = &ring.0;
    let open_len = coords.len().saturating_sub(1);
    if open_len < 2 || coords.first() != coords.last() {
        return ring.clone();
    }
    let min_idx = (0..open_len)
        .min_by(|&i, &j| coord_cmp(&coords[i], &coords[j]))
        .unwrap_or(0);
    if min_idx == 0 {
        return ring.clone();
    }
    let mut rotated = Vec::with_capacity(coords.len());
    rotated.extend_from_slice(&coords[min_idx..open_len]);
    rotated.extend_from_slice(&coords[..min_idx]);
    rotated.push(rotated[0]);
    LineString::new(rotated)
}

/// Canonicalize a polygon: orient the exterior ring clockwise and interior rings
/// counter-clockwise, rotate every ring to start at its lexicographically smallest
/// vertex, and sort the interior rings into a deterministic order (hole order is not
/// semantically meaningful).
fn normalize_polygon(poly: &Polygon<f64>) -> Polygon<f64> {
    let oriented = poly.orient(Direction::Reversed);
    let exterior = rotate_ring_to_min_vertex(oriented.exterior());
    let mut interiors: Vec<LineString<f64>> = oriented
        .interiors()
        .iter()
        .map(rotate_ring_to_min_vertex)
        .collect();
    interiors.sort_by(|a, b| coords_cmp(&a.0, &b.0));
    Polygon::new(exterior, interiors)
}

/// Recursively canonicalize a geometry so that spatially-equivalent inputs (differing
/// only in ring orientation, ring starting vertex, or non-semantic part ordering)
/// produce an identical output, per the `ST_Normalize` convention. Used upstream of
/// hash generation to avoid false-positive change detections.
fn normalize_geometry(geom: &Geometry<f64>) -> Geometry<f64> {
    match geom {
        Geometry::Polygon(p) => Geometry::Polygon(normalize_polygon(p)),
        Geometry::MultiPolygon(mp) => {
            let mut polys: Vec<Polygon<f64>> = mp.iter().map(normalize_polygon).collect();
            polys.sort_by(|a, b| coords_cmp(&a.exterior().0, &b.exterior().0));
            Geometry::MultiPolygon(MultiPolygon(polys))
        }
        Geometry::MultiLineString(mls) => {
            let mut lines: Vec<LineString<f64>> = mls.iter().cloned().collect();
            lines.sort_by(|a, b| coords_cmp(&a.0, &b.0));
            Geometry::MultiLineString(MultiLineString::new(lines))
        }
        Geometry::MultiPoint(mp) => {
            let mut pts: Vec<Point<f64>> = mp.iter().cloned().collect();
            pts.sort_by(|a, b| coord_cmp(&a.0, &b.0));
            Geometry::MultiPoint(MultiPoint(pts))
        }
        Geometry::GeometryCollection(gc) => {
            let mut geoms: Vec<Geometry<f64>> = gc.iter().map(normalize_geometry).collect();
            // Sort by the normalized WKB bytes for a total, deterministic order across
            // mixed geometry types.
            geoms.sort_by(|a, b| {
                geom_to_wkb(a)
                    .unwrap_or_default()
                    .cmp(&geom_to_wkb(b).unwrap_or_default())
            });
            Geometry::GeometryCollection(GeometryCollection::new_from(geoms))
        }
        // Point and LineString have no non-semantic ordering to canonicalize; other
        // variants (Line, Rect, Triangle) are not producible from WKB/WKT input.
        other => other.clone(),
    }
}

fn apply_normalize(g: &Geometry) -> Option<Geometry> {
    // Wrapped in catch_unwind for defensive robustness, matching the other geometry
    // transforms in this crate, since sorting/winding can panic on degenerate input.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| normalize_geometry(g))).ok()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StNormalize;

#[typetag::serde]
impl ScalarUDF for StNormalize {
    fn name(&self) -> &'static str {
        "st_normalize"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_geom_to_geom(inputs.required(0)?, self.name(), apply_normalize)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Returns the geometry in a canonical, normalized form. Polygon rings are wound \
         consistently (clockwise exterior, counter-clockwise interiors) and rotated to \
         start at their lexicographically smallest vertex; the non-semantic ordering of \
         multi-part geometry and geometry-collection members is sorted deterministically. \
         Geometrically equivalent inputs that differ only in ring orientation, ring \
         starting vertex, or part order produce identical output, making this useful as a \
         pre-step to hashing for change detection."
    }
}

#[must_use]
pub fn st_normalize(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StNormalize, vec![geom]).into()
}

#[cfg(test)]
mod tests {
    use geo::Geometry;
    use wkt::{ToWkt, TryFromWkt};

    use super::normalize_geometry;

    fn norm_wkt(wkt: &str) -> String {
        let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt).unwrap();
        normalize_geometry(&geom).to_wkt().to_string()
    }

    #[test]
    fn polygon_ring_orientation_is_normalized() {
        let cw = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))";
        let ccw = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
        assert_eq!(norm_wkt(cw), norm_wkt(ccw));
    }

    #[test]
    fn polygon_ring_start_vertex_is_normalized() {
        let a = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
        let b = "POLYGON((10 10, 0 10, 0 0, 10 0, 10 10))";
        assert_eq!(norm_wkt(a), norm_wkt(b));
    }

    #[test]
    fn polygon_hole_orientation_and_order_are_normalized() {
        let a = "POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 2,2 2,2 1,1 1),(4 4,4 5,5 5,5 4,4 4))";
        let b = "POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4),(1 1,2 1,2 2,1 2,1 1))";
        assert_eq!(norm_wkt(a), norm_wkt(b));
    }

    #[test]
    fn multipolygon_part_order_is_normalized() {
        let a = "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((10 10,10 11,11 11,11 10,10 10)))";
        let b = "MULTIPOLYGON(((10 10,10 11,11 11,11 10,10 10)),((0 0,0 1,1 1,1 0,0 0)))";
        assert_eq!(norm_wkt(a), norm_wkt(b));
    }

    #[test]
    fn multipoint_order_is_normalized() {
        let a = "MULTIPOINT(3 3, 1 1, 2 2)";
        let b = "MULTIPOINT(1 1, 2 2, 3 3)";
        assert_eq!(norm_wkt(a), norm_wkt(b));
    }

    #[test]
    fn different_geometries_stay_different() {
        let a = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))";
        let b = "POLYGON((0 0, 0 5, 10 10, 10 0, 0 0))";
        assert_ne!(norm_wkt(a), norm_wkt(b));
    }

    #[test]
    fn normalization_is_idempotent_and_deterministic() {
        let wkt = "MULTIPOLYGON(((10 10,10 11,11 11,11 10,10 10)),((0 0,0 1,1 1,1 0,0 0)))";
        let once = norm_wkt(wkt);
        let twice = norm_wkt(&once);
        assert_eq!(once, twice);
    }
}
