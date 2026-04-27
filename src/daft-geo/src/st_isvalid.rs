use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_bool, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StIsValid;

#[typetag::serde]
impl ScalarUDF for StIsValid {
    fn name(&self) -> &'static str { "st_isvalid" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        // geo 0.29 doesn't have IsValid; successfully parsed geometry is considered valid
        unary_geom_to_bool(inputs.required(0)?, self.name(), |_g| true)
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Boolean))
    }

    fn docstring(&self) -> &'static str {
        "Returns true if the geometry is topologically valid."
    }
}

#[must_use]
pub fn st_isvalid(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StIsValid, vec![geom]).into()
}
