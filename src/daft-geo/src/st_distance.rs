use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::{Distance, Euclidean, Geometry};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_geom_to_f64, validate_geometry_field};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StDistance;

#[typetag::serde]
impl ScalarUDF for StDistance {
    fn name(&self) -> &'static str { "st_distance" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        binary_geom_to_f64(inputs.required(0)?, inputs.required(1)?, self.name(), geom_distance)
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the minimum Euclidean distance between geometry A and geometry B."
    }
}

#[must_use]
pub fn st_distance(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StDistance, vec![geom_a, geom_b]).into()
}
