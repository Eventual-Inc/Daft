use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::{Distance, Euclidean, Geodesic, Geometry};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_geom_to_f64, validate_geometry_field, read_bool_arg, read_bool_arg_expr};

fn geom_distance(a: &Geometry, b: &Geometry) -> f64 {
    let euclidean = Euclidean;
    match (a, b) {
        (Geometry::Point(pa), Geometry::Point(pb)) => euclidean.distance(pa, pb),
        (Geometry::Point(p), Geometry::Polygon(poly)) => euclidean.distance(p, poly),
        (Geometry::Polygon(poly), Geometry::Point(p)) => euclidean.distance(poly, p),
        (Geometry::Polygon(a), Geometry::Polygon(b)) => euclidean.distance(a, b),
        (Geometry::LineString(a), Geometry::Point(p)) => euclidean.distance(a, p),
        (Geometry::Point(p), Geometry::LineString(ls)) => euclidean.distance(p, ls),
        _ => f64::NAN,
    }
}

fn geom_distance_geodesic(a: &Geometry, b: &Geometry) -> f64 {
    match (a, b) {
        (Geometry::Point(pa), Geometry::Point(pb)) => Geodesic.distance(*pa, *pb),
        _ => f64::NAN, // geodesic distance for non-point pairs is out of scope; planar covers them
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StDistance;

#[typetag::serde]
impl ScalarUDF for StDistance {
    fn name(&self) -> &'static str { "st_distance" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        let use_spheroid = read_bool_arg(&inputs, 2, "use_spheroid", self.name())?;
        let f = if use_spheroid { geom_distance_geodesic } else { geom_distance };
        binary_geom_to_f64(inputs.required(0)?, inputs.required(1)?, self.name(), f)
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        read_bool_arg_expr(&inputs, 2, "use_spheroid", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Minimum distance between A and B. Planar (coordinate units) by default; WGS84 geodesic meters when use_spheroid=true (lon/lat point inputs)."
    }
}

#[must_use]
pub fn st_distance(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StDistance, vec![geom_a, geom_b]).into()
}
