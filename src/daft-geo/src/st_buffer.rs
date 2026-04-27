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

use crate::utils::{unary_geom_to_geom, validate_geometry_field};

/// Simple radial buffer by translating all coordinates outward.
/// This is an approximate implementation using coordinate scaling.
/// For production use, a proper geodesic buffer library should be used.
fn apply_buffer(g: &Geometry, distance: f64) -> Option<Geometry> {
    use geo::BoundingRect;

    // For points: create an approximate circle polygon
    if let Geometry::Point(p) = g {
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
        return Some(Geometry::Polygon(geo::Polygon::new(ring, vec![])));
    }

    // For other geometries: expand bounding box by distance as a simple envelope
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StBuffer {
    pub distance: ordered_float::OrderedFloat<f64>,
}

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
        let distance = self.distance.0;
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
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Returns a geometry that is the given distance from the input geometry. \
         For points this produces a circular polygon; for other types it returns the \
         bounding-box envelope expanded by the distance."
    }
}

#[must_use]
pub fn st_buffer(geom: ExprRef, distance: f64) -> ExprRef {
    ScalarFn::builtin(
        StBuffer {
            distance: ordered_float::OrderedFloat(distance),
        },
        vec![geom],
    )
    .into()
}
