use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::{Euclidean, Geometry};
use geo::line_measures::LengthMeasurable;
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_f64, validate_geometry_field};

fn geom_length(g: &Geometry) -> f64 {
    let euclidean = Euclidean;
    match g {
        Geometry::Line(l) => l.length(&euclidean),
        Geometry::LineString(ls) => ls.length(&euclidean),
        Geometry::MultiLineString(mls) => mls.length(&euclidean),
        _ => 0.0,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StLength;

#[typetag::serde]
impl ScalarUDF for StLength {
    fn name(&self) -> &'static str { "st_length" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        unary_geom_to_f64(inputs.required(0)?, self.name(), geom_length)
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the Euclidean length of a geometry (0 for points, perimeter for polygons)."
    }
}

#[must_use]
pub fn st_length(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StLength, vec![geom]).into()
}
