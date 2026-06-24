use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::Geometry;
use serde::{Deserialize, Serialize};

use crate::utils::{read_f64_arg, read_f64_arg_expr, unary_geom_to_geom, validate_geometry_field};

/// Apply a planar buffer of `distance` units to geometry `g`.
///
/// - **Point**: returns a 64-vertex circular polygon (exact circle approximation).
/// - **Polygon**: uses `geo_buffer::buffer_polygon` (straight-skeleton offset) for a real
///   planar offset; falls back to bbox-envelope expansion if the crate returns an empty result.
/// - **MultiPolygon**: uses `geo_buffer::buffer_multi_polygon`.
/// - **All others**: falls back to expanding the bounding-box envelope by `distance`.
///
/// All buffer operations are **planar** (Cartesian). For geodesic buffers, project your
/// coordinates to a local metric CRS before calling this function.
fn apply_buffer(g: &Geometry, distance: f64) -> Option<Geometry> {
    use geo::BoundingRect;

    match g {
        // Point → 64-vertex circle (a correct planar buffer for points)
        Geometry::Point(p) => {
            let n_points = 64usize;
            let coords: Vec<geo::Coord<f64>> = (0..=n_points)
                .map(|i| {
                    let angle = 2.0 * std::f64::consts::PI * i as f64 / n_points as f64;
                    geo::Coord {
                        x: p.x() + distance * angle.cos(),
                        y: p.y() + distance * angle.sin(),
                    }
                })
                .collect();
            let ring = geo::LineString(coords);
            Some(Geometry::Polygon(geo::Polygon::new(ring, vec![])))
        }

        // Polygon → real planar offset via geo_buffer straight-skeleton
        Geometry::Polygon(poly) => {
            let mp = geo_buffer::buffer_polygon(poly, distance);
            if mp.0.is_empty() {
                // Fallback: bbox envelope (e.g. degenerate polygon or full shrink)
                let bbox = g.bounding_rect()?;
                let expanded = geo::Rect::new(
                    geo::Coord {
                        x: bbox.min().x - distance,
                        y: bbox.min().y - distance,
                    },
                    geo::Coord {
                        x: bbox.max().x + distance,
                        y: bbox.max().y + distance,
                    },
                );
                Some(Geometry::Rect(expanded))
            } else {
                Some(Geometry::MultiPolygon(mp))
            }
        }

        // MultiPolygon → real planar offset via geo_buffer
        Geometry::MultiPolygon(mp) => {
            let result = geo_buffer::buffer_multi_polygon(mp, distance);
            if result.0.is_empty() {
                let bbox = g.bounding_rect()?;
                let expanded = geo::Rect::new(
                    geo::Coord {
                        x: bbox.min().x - distance,
                        y: bbox.min().y - distance,
                    },
                    geo::Coord {
                        x: bbox.max().x + distance,
                        y: bbox.max().y + distance,
                    },
                );
                Some(Geometry::Rect(expanded))
            } else {
                Some(Geometry::MultiPolygon(result))
            }
        }

        // All other types (LineString, etc.) → expand bounding-box envelope
        _ => {
            let bbox = g.bounding_rect()?;
            let expanded = geo::Rect::new(
                geo::Coord {
                    x: bbox.min().x - distance,
                    y: bbox.min().y - distance,
                },
                geo::Coord {
                    x: bbox.max().x + distance,
                    y: bbox.max().y + distance,
                },
            );
            Some(Geometry::Rect(expanded))
        }
    }
}

/// Zero-field unit struct; the `distance` parameter is read from the trailing
/// positional argument at call time (not stored in the struct).
///
/// This is necessary because the Python `_call_builtin_scalar_fn` path resolves
/// the registered instance by name and cannot pass struct-field parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StBuffer;

#[typetag::serde]
impl ScalarUDF for StBuffer {
    fn name(&self) -> &'static str {
        "st_buffer"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let distance = read_f64_arg(&inputs, 1, "distance", self.name())?;
        unary_geom_to_geom(inputs.required(0)?, self.name(), |g| {
            apply_buffer(g, distance)
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        // Validate distance arg exists and is numeric
        let _distance = read_f64_arg_expr(&inputs, 1, "distance", self.name())?;
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Returns a geometry that is the given distance from the input geometry (planar Cartesian). \
         For Point geometries this produces a 64-vertex circular polygon approximation; \
         for Polygon/MultiPolygon a real planar offset is computed via straight-skeleton; \
         for other types the bounding-box envelope is expanded by distance."
    }
}

#[must_use]
pub fn st_buffer(geom: ExprRef, distance: f64) -> ExprRef {
    ScalarFn::builtin(StBuffer, vec![geom, daft_dsl::lit(distance)]).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Area, Geometry, Point};

    #[test]
    fn test_point_buffer_area_approx_pi_r2() {
        let p = Geometry::Point(Point::new(0.0_f64, 0.0));
        let buf = apply_buffer(&p, 1.0).unwrap();
        // area of a radius-1 buffer ≈ π (64-gon approximation is very close)
        let a = buf.unsigned_area();
        assert!(
            (a - std::f64::consts::PI).abs() < 0.01,
            "expected area ≈ π, got {a}"
        );
    }

    #[test]
    fn test_polygon_buffer_expands() {
        use geo::{LineString, Polygon};
        // Unit square
        let poly = Polygon::new(
            LineString::from(vec![(0.0_f64, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            vec![],
        );
        let g = Geometry::Polygon(poly);
        let buf = apply_buffer(&g, 1.0).unwrap();
        // Result should have larger area than the unit square (area = 1)
        let a = buf.unsigned_area();
        assert!(a > 1.0, "buffered polygon area {a} should exceed unit square area");
    }
}
