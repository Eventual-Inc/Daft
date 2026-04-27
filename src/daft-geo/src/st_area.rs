use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::Area;
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_f64, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StArea;

#[typetag::serde]
impl ScalarUDF for StArea {
    fn name(&self) -> &'static str { "st_area" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        unary_geom_to_f64(inputs.required(0)?, self.name(), |g| g.unsigned_area())
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the 2D area of a geometry (in coordinate-system units squared)."
    }
}

#[must_use]
pub fn st_area(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StArea, vec![geom]).into()
}
